// functions/api/[[path]].js
/**
 * Cloudflare Pages Functions (catch-all) for D1
 * - CORS ครบ (GET/POST/PUT/PATCH/DELETE/OPTIONS)
 * - Health/meta:
 *     GET  /api
 * - Tables meta:
 *     GET  /api/tables
 *     GET  /api/tables/:table
 *     GET  /api/tables/:table/columns
 * - Generic CRUD (ทุกตาราง):
 *     GET  /api/tables/:table?q=&limit=&offset=&order=
 *     POST /api/tables/:table                 (create 1 แถว)
 *     POST /api/tables/:table/bulk            (create หลายแถว [{...},{...}])
 *     PUT  /api/tables/:table?id=<id>         (update แทนที่)
 *     PATCH /api/tables/:table?id=<id>        (update บางฟิลด์)
 *     DELETE /api/tables/:table?id=<id>       (ลบ)
 * - Shortcuts (ใส่ alias ให้ตารางที่ใช้บ่อย เพิ่มได้ง่าย):
 *     /api/hr/employees → hr_employees
 *     /api/hr/attendance → hr_attendance
 *     /api/hr/timeclock → hr_timeclock
 *     /api/sales/customers → sales_customers
 *     /api/sales/quotations → sales_quotations
 *     /api/sales/quotationitems → sales_quotationitems
 *
 * หมายเหตุ:
 * - ต้อง bind D1 เป็นชื่อ "DB" ที่ Pages Settings › Functions › D1 bindings
 * - ระบบจะเดา primary key ตามลำดับ: PRAGMA(pk=1) → "id" → "rowid"
 * - ถ้าตารางมีคอลัมน์ CreateDate/UpdateDate/CreateBy/UpdateBy จะถูกเติมอัตโนมัติ
 */

export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const db = env.DB; // D1 binding ต้องตั้งชื่อ DB
  const user = request.headers.get("x-user") || "system";

  // ========= Helpers (อยู่บนสุด) =========
  const origin = request.headers.get("Origin") || "*";
  const baseHeaders = {
    "content-type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-User",
  };
  const json = (data, status = 200, headers = {}) =>
    new Response(JSON.stringify(data), { status, headers: { ...baseHeaders, ...headers } });
  const text = (msg, status = 200, headers = {}) =>
    new Response(msg, { status, headers: { ...baseHeaders, ...headers, "content-type": "text/plain; charset=utf-8" } });

  const q = (name, def = "") => (url.searchParams.get(name) ?? def).trim();

  const readBody = async () => {
    const ct = (request.headers.get("content-type") || "").toLowerCase();
    if (ct.includes("application/json")) {
      try { return await request.json(); } catch { return {}; }
    }
    if (ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data")) {
      const form = await request.formData();
      return Object.fromEntries(form.entries());
    }
    const raw = await request.text();
    try { return JSON.parse(raw); } catch { return {}; }
  };

  const safeIdent = (name) => {
    if (!/^[A-Za-z0-9_]+$/.test(name)) throw new Error(`Invalid identifier: ${name}`);
    return name;
  };

  const normalizeObject = (obj = {}) => {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      if (typeof v === "string") {
        const t = v.trim();
        // แปลงตัวเลขที่เป็น string → number (ไม่ไปยุ่งค่าที่ดูเหมือนวันที่/มีตัวอักษร)
        if (/^-?\d+(\.\d+)?$/.test(t)) out[k] = Number(t);
        else out[k] = t;
      } else out[k] = v;
    }
    return out;
  };

  const getColumns = async (table) => {
    const rs = await db.prepare(`PRAGMA table_info(${table})`).all();
    return rs.results || [];
  };

  const hasColumn = async (table, col) => {
    const cols = await getColumns(table);
    return cols.some((c) => c.name === col);
  };

  const getPrimaryKey = async (table) => {
    const cols = await getColumns(table);
    const pk = cols.find((c) => c.pk === 1)?.name;
    if (pk) return pk;
    if (cols.some((c) => c.name.toLowerCase() === "id")) return "id";
    return "rowid";
  };

  const addAuditOnCreate = async (table, obj) => {
    const o = { ...obj };
    if (await hasColumn(table, "CreateDate")) o.CreateDate = o.CreateDate ?? { __raw: "datetime('now')" };
    if (await hasColumn(table, "UpdateDate")) o.UpdateDate = o.UpdateDate ?? { __raw: "datetime('now')" };
    if (await hasColumn(table, "CreateBy"))   o.CreateBy   = o.CreateBy   ?? user;
    if (await hasColumn(table, "UpdateBy"))   o.UpdateBy   = o.UpdateBy   ?? user;
    return o;
  };

  const addAuditOnUpdate = async (table, obj) => {
    const o = { ...obj };
    if (await hasColumn(table, "UpdateDate")) o.UpdateDate = { __raw: "datetime('now')" };
    if (await hasColumn(table, "UpdateBy"))   o.UpdateBy   = user;
    return o;
  };

  const buildInsert = (table, dataObj) => {
    const keys = Object.keys(dataObj);
    if (!keys.length) throw new Error("Empty object");
    const cols = keys.map((k) => `"${safeIdent(k)}"`).join(", ");
    const vals = keys.map((k) => (dataObj[k]?.__raw ? dataObj[k].__raw : "?")).join(", ");
    const bind = keys.filter((k) => !dataObj[k]?.__raw).map((k) => dataObj[k]);
    const sql = `INSERT INTO ${table} (${cols}) VALUES (${vals}) RETURNING *`;
    return { sql, bind };
  };

  const buildUpdate = (table, dataObj, idField) => {
    const keys = Object.keys(dataObj);
    if (!keys.length) throw new Error("Empty object");
    const sets = keys.map((k) =>
      dataObj[k]?.__raw ? `"${safeIdent(k)}" = ${dataObj[k].__raw}` : `"${safeIdent(k)}" = ?`
    ).join(", ");
    const bind = keys.filter((k) => !dataObj[k]?.__raw).map((k) => dataObj[k]);
    const sql = `UPDATE ${table} SET ${sets} WHERE ${idField} = ? RETURNING *`;
    return { sql, bind };
  };

  const listTables = async () => {
    const rs = await db.prepare(`
      SELECT name
      FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%'
      ORDER BY name
    `).all();
    return rs.results?.map((r) => r.name) ?? [];
  };

  // ========= CORS Preflight =========
  if (method === "OPTIONS") {
    return new Response(null, { status: 204, headers: baseHeaders });
  }

  // ========= Guard base path =========
  if (!url.pathname.startsWith("/api")) {
    return json({ ok: false, error: "Not found" }, 404);
  }

  // ========= Health =========
  const subpath = url.pathname.replace(/^\/api\/?/, "").replace(/\/+$/, "");
  if (subpath === "" || subpath === "/") {
    return json({
      ok: true,
      service: "bizapp-api",
      time: new Date().toISOString(),
      tips: "Use /api/tables/<table> for generic CRUD; /api/<shortcut> for common tables.",
    });
  }

  // ========= Generic CRUD maker =========
  const makeCrud = (tableName) => {
    const table = safeIdent(tableName);
    return {
      list: async () => {
        const idField = await getPrimaryKey(table);
        const term   = q("q");
        const limit  = Math.min(Number(q("limit", "200")) || 200, 1000);
        const offset = Math.max(Number(q("offset", "0")) || 0, 0);
        const order  = q("order", `${idField} DESC`);
        let sql = `SELECT * FROM ${table}`;
        const bind = [];

        if (term) {
          const cols = await getColumns(table);
          const texts = cols
            .filter((c) => (c.type || "").toUpperCase().includes("CHAR") || (c.type || "").toUpperCase().includes("TEXT"))
            .map((c) => c.name);
          if (texts.length) {
            const likeExp = texts.map((c) => `"${c}" LIKE ?`).join(" OR ");
            sql += ` WHERE (${likeExp})`;
            texts.forEach(() => bind.push(`%${term}%`));
          }
        }
        sql += ` ORDER BY ${order} LIMIT ${limit} OFFSET ${offset}`;
        const rs = await db.prepare(sql).bind(...bind).all();
        return json({ ok: true, data: rs.results || [] });
      },

      create: async () => {
        const body = normalizeObject(await readBody());
        const withAudit = await addAuditOnCreate(table, body);
        const { sql, bind } = buildInsert(table, withAudit);
        const row = await db.prepare(sql).bind(...bind).first();
        return json({ ok: true, data: row });
      },

      bulkCreate: async () => {
        const payload = await readBody();
        const arr = Array.isArray(payload) ? payload : payload?.rows;
        if (!Array.isArray(arr) || arr.length === 0) {
          return json({ ok: false, error: "Body must be array of objects or {rows:[...]}" }, 400);
        }
        const results = [];
        for (const item of arr) {
          const body = normalizeObject(item);
          const withAudit = await addAuditOnCreate(table, body);
          const { sql, bind } = buildInsert(table, withAudit);
          const row = await db.prepare(sql).bind(...bind).first();
          results.push(row);
        }
        return json({ ok: true, data: results });
      },

      update: async () => {
        const id = q("id");
        if (!id) return json({ ok: false, error: "id is required" }, 400);
        const idField = await getPrimaryKey(table);
        const body = normalizeObject(await readBody());
        const withAudit = await addAuditOnUpdate(table, body);
        const { sql, bind } = buildUpdate(table, withAudit, idField);
        const row = await db.prepare(sql).bind(...bind, id).first();
        if (!row) return json({ ok: false, error: "not found" }, 404);
        return json({ ok: true, data: row });
      },

      patch: async () => {
        // same as update (partial) — ฝั่ง client ส่งเฉพาะฟิลด์ที่อยากแก้
        return this.update();
      },

      remove: async () => {
        const id = q("id");
        if (!id) return json({ ok: false, error: "id is required" }, 400);
        const idField = await getPrimaryKey(table);
        const row = await db.prepare(
          `DELETE FROM ${table} WHERE ${idField} = ? RETURNING *`
        ).bind(id).first();
        if (!row) return json({ ok: false, error: "not found" }, 404);
        return json({ ok: true, data: row });
      },
    };
  };

  // ========= Router =========
  try {
    const seg = subpath.split("/").filter(Boolean); // e.g. ["tables","hr_employees"] or ["hr","employees"]
    const top = seg[0]?.toLowerCase();

    // --- /api/tables ... ---
    if (top === "tables") {
      if (seg.length === 1) {
        if (method !== "GET") return json({ ok: false, error: "Method not allowed" }, 405);
        const tables = await listTables();
        return json({ ok: true, data: tables });
      }

      const tableName = seg[1];
      const table = safeIdent(tableName);

      if (seg.length === 2) {
        const crud = makeCrud(table);
        if (method === "GET")    return await crud.list();
        if (method === "POST")   return await crud.create();
        if (method === "PUT")    return await crud.update();
        if (method === "PATCH")  return await crud.update(); // partial update
        if (method === "DELETE") return await crud.remove();
        return json({ ok: false, error: "Method not allowed" }, 405);
      }

      if (seg.length === 3 && seg[2].toLowerCase() === "columns") {
        if (method !== "GET") return json({ ok: false, error: "Method not allowed" }, 405);
        const cols = await getColumns(table);
        return json({ ok: true, data: cols });
      }

      if (seg.length === 3 && seg[2].toLowerCase() === "bulk") {
        if (method !== "POST") return json({ ok: false, error: "Method not allowed" }, 405);
        const crud = makeCrud(table);
        return await crud.bulkCreate();
      }

      return json({ ok: false, error: "Not found" }, 404);
    }

    // --- Shortcuts → map เป็นตาราง ---
    const shortcutMap = {
      "hr/employees":         "hr_employees",
      "hr/attendance":        "hr_attendance",
      "hr/timeclock":         "hr_timeclock",
      "sales/customers":      "sales_customers",
      "sales/quotations":     "sales_quotations",
      "sales/quotationitems": "sales_quotationitems",
      // เพิ่ม alias อื่น ๆ ได้ตาม schema ของคุณ
    };
    const key = seg.join("/").toLowerCase();

    if (shortcutMap[key]) {
      const table = shortcutMap[key];
      const crud = makeCrud(table);
      if (method === "GET")    return await crud.list();
      if (method === "POST")   return await crud.create();
      if (method === "PUT")    return await crud.update();
      if (method === "PATCH")  return await crud.update();
      if (method === "DELETE") return await crud.remove();
      return json({ ok: false, error: "Method not allowed" }, 405);
    }

    // ไม่ match route ใด ๆ
    return json({ ok: false, error: `No route for: ${subpath}` }, 404);

  } catch (err) {
    console.error("[[path]] error:", err);
    return json({ ok: false, error: err.message || String(err) }, 500);
  }
};
