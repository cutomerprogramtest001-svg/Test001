// functions/api/[[path]].js
/**
 * Cloudflare Pages Functions (catch-all) for D1
 * - CORS ครบ (GET/POST/PUT/DELETE/OPTIONS)
 * - Health/meta:    GET /api
 * - Tables meta:    GET /api/tables , GET /api/tables/<table> , GET /api/tables/<table>/columns
 * - Generic CRUD:   /api/tables/<table>  (GET list/search, POST create, PUT update?id=..., DELETE ?id=...)
 * - Shortcuts CRUD: /api/hr/employees , /api/hr/attendance , /api/hr/timeclock
 *                   /api/sales/customers , /api/sales/quotations , /api/sales/quotationitems
 *
 * หมายเหตุ:
 * - ผูก D1 binding ชื่อ "DB" ใน Pages Settings › Functions › D1 bindings
 * - ชื่อ PK: พยายามตรวจจาก PRAGMA (pk=1) ถ้าไม่พบ ใช้ "id"; ถ้าไม่มีจริง ๆ จะ fallback เป็น "rowid"
 * - Timestamp auto: ถ้าตารางมี CreateDate/UpdateDate จะใส่ค่า datetime('now') ให้อัตโนมัติ
 */

export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);

  // ========= Helpers (อยู่บนสุด ป้องกัน ReferenceError) =========
  const origin = request.headers.get("Origin") || "*";

  const baseHeaders = {
    "content-type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-User",
  };

  const json = (data, status = 200, headers = {}) =>
    new Response(JSON.stringify(data), {
      status,
      headers: { ...baseHeaders, ...headers },
    });

  const text = (msg, status = 200, headers = {}) =>
    new Response(msg, { status, headers: { ...baseHeaders, ...headers, "content-type": "text/plain; charset=utf-8" } });

  const method = request.method.toUpperCase();
  const db = env.DB;                      // <== ต้อง bind เป็น DB
  const user = request.headers.get("x-user") || "system";

  const q = (name, def = "") => (url.searchParams.get(name) ?? def).trim();

  const readBody = async () => {
    const ct = request.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      try { return await request.json(); } catch { return {}; }
    }
    if (ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data")) {
      const form = await request.formData();
      return Object.fromEntries(form.entries());
    }
    // raw text (ลอง parse JSON ถ้าเป็นไปได้)
    try { return JSON.parse(await request.text()); } catch { return {}; }
  };

  const normalizeObject = (obj = {}) => {
    // ตัดช่องว่างปลาย/หัว, แปลงเลขที่เป็น string → number ถ้าแปลงได้
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      if (typeof v === "string") {
        const trimmed = v.trim();
        // อย่าแปลงวันที่/รหัสที่มีตัวอักษรปน
        if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
          out[k] = Number(trimmed);
        } else {
          out[k] = trimmed;
        }
      } else {
        out[k] = v;
      }
    }
    return out;
  };

  const safeIdent = (name) => {
    // ป้องกัน SQL injection ในชื่อ table/column (อนุญาตเฉพาะ a-zA-Z0-9_ )
    if (!/^[A-Za-z0-9_]+$/.test(name)) throw new Error(`Invalid identifier: ${name}`);
    return name;
  };

  const getColumns = async (table) => {
    const rs = await db.prepare(`PRAGMA table_info(${table})`).all();
    // results = [{cid, name, type, notnull, dflt_value, pk}, ...]
    return rs.results || [];
  };

  const getPrimaryKey = async (table) => {
    // เลือกคอลัมน์ pk=1 ถ้ามี
    const cols = await getColumns(table);
    const pkCol = cols.find((c) => c.pk === 1)?.name;
    if (pkCol) return pkCol;
    // ถ้าไม่มี pk ให้ลองหา "id"
    if (cols.some((c) => c.name.toLowerCase() === "id")) return "id";
    // fallback rowid
    return "rowid";
  };

  const tableHasColumn = async (table, colName) => {
    const cols = await getColumns(table);
    return cols.some((c) => c.name === colName);
  };

  const addAuditColsOnCreate = async (table, obj) => {
    // ถ้าตารางมี CreateDate/UpdateDate/CreateBy/UpdateBy → ใส่อัตโนมัติ
    const out = { ...obj };
    if (await tableHasColumn(table, "CreateDate")) out.CreateDate = out.CreateDate ?? { __raw: "datetime('now')" };
    if (await tableHasColumn(table, "UpdateDate")) out.UpdateDate = out.UpdateDate ?? { __raw: "datetime('now')" };
    if (await tableHasColumn(table, "CreateBy"))   out.CreateBy   = out.CreateBy   ?? user;
    if (await tableHasColumn(table, "UpdateBy"))   out.UpdateBy   = out.UpdateBy   ?? user;
    return out;
  };

  const addAuditColsOnUpdate = async (table, obj) => {
    const out = { ...obj };
    if (await tableHasColumn(table, "UpdateDate")) out.UpdateDate = { __raw: "datetime('now')" };
    if (await tableHasColumn(table, "UpdateBy"))   out.UpdateBy   = user;
    return out;
  };

  const buildInsert = (table, dataObj) => {
    // รองรับค่าแบบ { key: {__raw:"..."}} เพื่อแทรกฟังก์ชัน SQL เช่น datetime('now')
    const keys = Object.keys(dataObj);
    if (!keys.length) throw new Error("Empty object");

    const cols = keys.map((k) => `"${safeIdent(k)}"`).join(", ");
    const place = keys.map((k) => (dataObj[k]?.__raw ? dataObj[k].__raw : "?")).join(", ");
    const bind = keys.filter((k) => !dataObj[k]?.__raw).map((k) => dataObj[k]);

    const sql = `INSERT INTO ${table} (${cols}) VALUES (${place}) RETURNING *`;
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
    return rs.results?.map(r => r.name) ?? [];
  };

  // ========= CORS Preflight =========
  if (method === "OPTIONS") {
    return new Response(null, { status: 204, headers: baseHeaders });
  }

  // ========= Guard: ต้องขึ้นต้นด้วย /api =========
  if (!url.pathname.startsWith("/api")) {
    return json({ ok: false, error: "Not found" }, 404);
  }

  // ========= Health / Meta =========
  const subpath = url.pathname.replace(/^\/api\/?/, "").replace(/\/+$/, ""); // eg: "", "tables", "hr/employees"
  if (subpath === "" || subpath === "/") {
    return json({
      ok: true,
      service: "bizapp-api",
      time: new Date().toISOString(),
      routes: [
        "GET  /api",
        "GET  /api/tables",
        "GET  /api/tables/:table",
        "GET  /api/tables/:table/columns",
        "GET  /api/tables/:table?q=&limit=&offset=&order=",
        "POST /api/tables/:table",
        "PUT  /api/tables/:table?id=",
        "DELETE /api/tables/:table?id=",
        "GET/POST/PUT/DELETE  /api/hr/employees (shortcut)",
        "GET/POST/PUT/DELETE  /api/hr/attendance (shortcut)",
        "GET/POST/PUT/DELETE  /api/hr/timeclock (shortcut)",
        "GET/POST/PUT/DELETE  /api/sales/customers (shortcut)",
        "GET/POST/PUT/DELETE  /api/sales/quotations (shortcut)",
        "GET/POST/PUT/DELETE  /api/sales/quotationitems (shortcut)",
      ],
    });
  }

  // ========= Generic table CRUD handlers =========
  const makeCrud = (tableName) => {
    const table = safeIdent(tableName);

    return {
      list: async () => {
        const idField = await getPrimaryKey(table);
        const term = q("q");                 // ค้นหาเบื้องต้น
        const limit = Math.min(Number(q("limit", "200")) || 200, 1000);
        const offset = Math.max(Number(q("offset", "0")) || 0, 0);
        const order = q("order", idField ? `${idField} DESC` : "rowid DESC");

        // เบื้องต้น allow simple like เฉพาะ text columns
        let sql = `SELECT * FROM ${table}`;
        const binds = [];
        if (term) {
          // หา text columns
          const cols = await getColumns(table);
          const textCols = cols
            .filter((c) => (c.type || "").toUpperCase().includes("CHAR") || (c.type || "").toUpperCase().includes("TEXT"))
            .map((c) => c.name);
          if (textCols.length) {
            const likes = textCols.map((c) => `"${c}" LIKE ?`).join(" OR ");
            sql += ` WHERE (${likes})`;
            textCols.forEach(() => binds.push(`%${term}%`));
          }
        }
        sql += ` ORDER BY ${order} LIMIT ${limit} OFFSET ${offset}`;
        const rs = await db.prepare(sql).bind(...binds).all();
        return json({ ok: true, data: rs.results || [] });
      },

      create: async () => {
        const body = normalizeObject(await readBody());
        const withAudit = await addAuditColsOnCreate(table, body);
        const { sql, bind } = buildInsert(table, withAudit);
        const row = await db.prepare(sql).bind(...bind).first();
        return json({ ok: true, data: row });
      },

      update: async () => {
        const id = q("id");
        if (!id) return json({ ok: false, error: "id is required" }, 400);

        const idField = await getPrimaryKey(table);
        const body = normalizeObject(await readBody());
        const withAudit = await addAuditColsOnUpdate(table, body);
        const { sql, bind } = buildUpdate(table, withAudit, idField);
        const row = await db.prepare(sql).bind(...bind, id).first();
        if (!row) return json({ ok: false, error: "not found" }, 404);
        return json({ ok: true, data: row });
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

  // ========= Route Resolver =========
  try {
    const seg = subpath.split("/").filter(Boolean); // ["tables", "<table>", "columns"] | ["hr","employees"] | ...
    const top = seg[0]?.toLowerCase();

    // --- /api/tables ... (meta & generic CRUD) ---
    if (top === "tables") {
      // GET /api/tables
      if (seg.length === 1) {
        if (method !== "GET") return json({ ok: false, error: "Method not allowed" }, 405);
        const tables = await listTables();
        return json({ ok: true, data: tables });
      }

      const tableName = seg[1];
      const table = safeIdent(tableName);

      // GET /api/tables/<table>
      if (seg.length === 2) {
        const crud = makeCrud(table);
        if (method === "GET")    return await crud.list();
        if (method === "POST")   return await crud.create();
        if (method === "PUT")    return await crud.update();
        if (method === "DELETE") return await crud.remove();
        return json({ ok: false, error: "Method not allowed" }, 405);
      }

      // GET /api/tables/<table>/columns
      if (seg.length === 3 && seg[2].toLowerCase() === "columns") {
        if (method !== "GET") return json({ ok: false, error: "Method not allowed" }, 405);
        const cols = await getColumns(table);
        return json({ ok: true, data: cols });
      }

      return json({ ok: false, error: "Not found" }, 404);
    }

    // --- Shortcuts mapping → generic CRUD by table ---
    const shortcutMap = {
      "hr/employees":       "hr_employees",
      "hr/attendance":      "hr_attendance",
      "hr/timeclock":       "hr_timeclock",
      "sales/customers":    "sales_customers",
      "sales/quotations":   "sales_quotations",
      "sales/quotationitems":"sales_quotationitems",
    };

    const key = seg.join("/").toLowerCase(); // e.g. "hr/employees"
    if (shortcutMap[key]) {
      const crud = makeCrud(shortcutMap[key]);
      if (method === "GET")    return await crud.list();
      if (method === "POST")   return await crud.create();
      if (method === "PUT")    return await crud.update();
      if (method === "DELETE") return await crud.remove();
      return json({ ok: false, error: "Method not allowed" }, 405);
    }

    // ไม่ match route ใด ๆ
    return json({ ok: false, error: `No route for: ${subpath}` }, 404);

  } catch (err) {
    // Error รวม (กัน worker ตาย เงียบ ๆ)
    console.error("[[path]].js error:", err);
    return json({ ok: false, error: err.message || String(err) }, 500);
  }
};
