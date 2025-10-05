// functions/api/[[path]].js
export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const db = env.DB; // D1 binding ชื่อ DB
  const user = request.headers.get("x-user") || "system";

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
        if (/^-?\d+(\.\d+)?$/.test(t)) out[k] = Number(t);
        else out[k] = t;
      } else out[k] = v;
    }
    return out;
  };

  const getColumns = async (table) => (await db.prepare(`PRAGMA table_info(${table})`).all()).results || [];
  const hasColumn = async (table, col) => (await getColumns(table)).some(c => c.name === col);
  const getPrimaryKey = async (table) => {
    const cols = await getColumns(table);
    const pk = cols.find(c => c.pk === 1)?.name;
    if (pk) return pk;
    if (cols.some(c => c.name.toLowerCase() === "id")) return "id";
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
    const cols = keys.map(k => `"${safeIdent(k)}"`).join(", ");
    const vals = keys.map(k => (dataObj[k]?.__raw ? dataObj[k].__raw : "?")).join(", ");
    const bind = keys.filter(k => !dataObj[k]?.__raw).map(k => dataObj[k]);
    const sql = `INSERT INTO ${table} (${cols}) VALUES (${vals}) RETURNING *`;
    return { sql, bind };
  };

  const buildUpdate = (table, dataObj, idField) => {
    const keys = Object.keys(dataObj);
    if (!keys.length) throw new Error("Empty object");
    const sets = keys.map(k => dataObj[k]?.__raw
      ? `"${safeIdent(k)}" = ${dataObj[k].__raw}`
      : `"${safeIdent(k)}" = ?`
    ).join(", ");
    const bind = keys.filter(k => !dataObj[k]?.__raw).map(k => dataObj[k]);
    const sql = `UPDATE ${table} SET ${sets} WHERE ${idField} = ? RETURNING *`;
    return { sql, bind };
  };

  const listTables = async () => {
    const rs = await db.prepare(`
      SELECT name FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%'
      ORDER BY name
    `).all();
    return rs.results?.map(r => r.name) ?? [];
  };

  // ---------- CORS ----------
  if (method === "OPTIONS") return new Response(null, { status: 204, headers: baseHeaders });

  if (!url.pathname.startsWith("/api")) return json({ ok: false, error: "Not found" }, 404);

  // ---------- Health ----------
  const subpath = url.pathname.replace(/^\/api\/?/, "").replace(/\/+$/, "");
  if (subpath === "" || subpath === "/") {
    return json({ ok: true, service: "bizapp-api", time: new Date().toISOString() });
  }

  // ---------- Generic CRUD ----------
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
            .filter(c => (c.type || "").toUpperCase().includes("CHAR") || (c.type || "").toUpperCase().includes("TEXT"))
            .map(c => c.name);
          if (texts.length) {
            const likeExp = texts.map(c => `"${c}" LIKE ?`).join(" OR ");
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
        if (!Array.isArray(arr) || arr.length === 0) return json({ ok:false, error:"Body must be array of objects or {rows:[...]}" }, 400);
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
      remove: async () => {
        const id = q("id");
        if (!id) return json({ ok: false, error: "id is required" }, 400);
        const idField = await getPrimaryKey(table);
        const row = await db.prepare(`DELETE FROM ${table} WHERE ${idField} = ? RETURNING *`).bind(id).first();
        if (!row) return json({ ok: false, error: "not found" }, 404);
        return json({ ok: true, data: row });
      },
    };
  };

  // ---------- Geo ----------
  const ensureGeoTables = async () => {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS geo_provinces (id INTEGER PRIMARY KEY, name_th TEXT, name_en TEXT);
      CREATE TABLE IF NOT EXISTS geo_amphures (id INTEGER PRIMARY KEY, province_id INTEGER, name_th TEXT, name_en TEXT);
      CREATE TABLE IF NOT EXISTS geo_tambons (id INTEGER PRIMARY KEY, amphure_id INTEGER, zip_code TEXT, name_th TEXT, name_en TEXT);
    `);
  };
  const geoSeedIfNeeded = async () => {
    await ensureGeoTables();
    const count = await db.prepare(`SELECT COUNT(*) AS c FROM geo_provinces`).first();
    if (count && count.c > 0) return { seeded: false, reason: "already populated" };
    const base = "https://raw.githubusercontent.com/kongvut/thai-province-data/master";
    const [provRes, amphRes, tambRes] = await Promise.all([
      fetch(`${base}/api_province.json`),
      fetch(`${base}/api_amphure.json`),
      fetch(`${base}/api_tambon.json`),
    ]);
    if (!provRes.ok || !amphRes.ok || !tambRes.ok) throw new Error("Fetch geo dataset failed");
    const provinces = await provRes.json();
    const amphures  = await amphRes.json();
    const tambons   = await tambRes.json();

    const insP = db.prepare(`INSERT INTO geo_provinces (id, name_th, name_en) VALUES (?, ?, ?)`);
    for (const p of provinces) await insP.bind(p.id, p.name_th, p.name_en).run();

    const insA = db.prepare(`INSERT INTO geo_amphures (id, province_id, name_th, name_en) VALUES (?, ?, ?, ?)`);
    for (const a of amphures) await insA.bind(a.id, a.province_id, a.name_th, a.name_en).run();

    const insT = db.prepare(`INSERT INTO geo_tambons (id, amphure_id, zip_code, name_th, name_en) VALUES (?, ?, ?, ?, ?)`);
    for (const t of tambons) await insT.bind(t.id, t.amphure_id, String(t.zip_code || ""), t.name_th, t.name_en).run();

    return { seeded: true, provinces: provinces.length, amphures: amphures.length, tambons: tambons.length };
  };
  const geoRouter = async (seg) => {
    const ep = seg[1]?.toLowerCase();
    if (seg.length === 2 && ep === "seed") {
      if (!["GET","POST"].includes(method)) return json({ ok:false, error:"Method not allowed" },405);
      return json({ ok:true, data: await geoSeedIfNeeded() });
    }
    if (seg.length === 2 && ep === "provinces") {
      if (method!=="GET") return json({ ok:false, error:"Method not allowed" },405);
      await ensureGeoTables();
      const term = q("q");
      let sql = `SELECT id, name_th, name_en FROM geo_provinces`;
      const bind = [];
      if (term) { sql += ` WHERE name_th LIKE ? OR name_en LIKE ?`; bind.push(`%${term}%`,`%${term}%`); }
      sql += ` ORDER BY name_th`;
      const rs = await db.prepare(sql).bind(...bind).all();
      return json({ ok:true, data: rs.results||[] });
    }
    if (seg.length === 2 && ep === "amphures") {
      if (method!=="GET") return json({ ok:false, error:"Method not allowed" },405);
      await ensureGeoTables();
      const provinceId = q("provinceId");
      let sql = `SELECT id, province_id, name_th, name_en FROM geo_amphures`;
      const bind = [];
      if (provinceId) { sql += ` WHERE province_id = ?`; bind.push(Number(provinceId)); }
      sql += ` ORDER BY name_th`;
      const rs = await db.prepare(sql).bind(...bind).all();
      return json({ ok:true, data: rs.results||[] });
    }
    if (seg.length === 2 && ep === "tambons") {
      if (method!=="GET") return json({ ok:false, error:"Method not allowed" },405);
      await ensureGeoTables();
      const amphureId = q("amphureId");
      let sql = `SELECT id, amphure_id, zip_code, name_th, name_en FROM geo_tambons`;
      const bind = [];
      if (amphureId) { sql += ` WHERE amphure_id = ?`; bind.push(Number(amphureId)); }
      sql += ` ORDER BY name_th`;
      const rs = await db.prepare(sql).bind(...bind).all();
      return json({ ok:true, data: rs.results||[] });
    }
    return json({ ok:false, error:"Geo endpoint not found" },404);
  };

  // ---------- Alias helper ----------
  const aliasCrud = async (seg, table) => {
    const crud = makeCrud(table);
    if (seg.length === 1) {
      if (method === "GET")    return await crud.list();
      if (method === "POST")   return await crud.create();
      if (method === "PUT")    return await crud.update();
      if (method === "PATCH")  return await crud.update();
      if (method === "DELETE") return await crud.remove();
      return json({ ok:false, error:"Method not allowed" },405);
    }
    if (seg.length === 2 && seg[1].toLowerCase() === "columns") {
      if (method !== "GET") return json({ ok:false, error:"Method not allowed" },405);
      return json({ ok:true, data: await getColumns(table) });
    }
    if (seg.length === 2 && seg[1].toLowerCase() === "bulk") {
      if (method !== "POST") return json({ ok:false, error:"Method not allowed" },405);
      return await makeCrud(table).bulkCreate();
    }
    return json({ ok:false, error:"Not found" },404);
  };

  // ---------- Sales helpers ----------
  const genNumber = async (prefix, table, col = "qNo", width = 5) => {
    // หา running no จากเลขมากสุดของคอลัมน์นั้น
    const rs = await db.prepare(`SELECT ${col} AS no FROM ${table} WHERE ${col} LIKE ? ORDER BY ${col} DESC LIMIT 1`).bind(`${prefix}%`).all();
    const last = rs.results?.[0]?.no || null;
    let n = 1;
    if (last) {
      const m = last.match(/(\d+)$/);
      if (m) n = Number(m[1]) + 1;
    }
    return `${prefix}${String(n).padStart(width,"0")}`;
  };

  const confirmQuotation = async () => {
    const body = await readBody();
    const qNo = body?.qNo;
    if (!qNo) return json({ ok:false, error:"qNo is required" },400);
    const row = await db.prepare(`UPDATE sales_quotations SET status='Confirmed', UpdateDate=datetime('now') WHERE qNo=? RETURNING *`).bind(qNo).first();
    if (!row) return json({ ok:false, error:"Quotation not found" },404);
    return json({ ok:true, data: row });
  };

  const createSOFromQuotation = async () => {
    const body = await readBody();
    const qNo = body?.qNo;
    if (!qNo) return json({ ok:false, error:"qNo is required" },400);

    const qh = await db.prepare(`SELECT * FROM sales_quotations WHERE qNo=?`).bind(qNo).first();
    if (!qh) return json({ ok:false, error:"Quotation not found" },404);
    if (qh.status !== "Confirmed") return json({ ok:false, error:"Quotation not confirmed" },400);

    const soNo = await genNumber("SO", "sales_saleorders", "soNo", 5);
    await db.prepare(`
      INSERT INTO sales_saleorders
      (soNo, soDate, status, customerCode, billTo, shipTo, paymentTerm, totalBeforeDiscount, discount, grandTotal, note, CreateDate, UpdateDate, CreateBy, UpdateBy)
      VALUES (?, date('now'), 'Open', ?, ?, ?, 'CASH', ?, ?, ?, '', datetime('now'), datetime('now'), ?, ?)
    `).bind(
      soNo, qh.customerCode, qh.customerCode, qh.customerCode,
      qh.totalBeforeDiscount, qh.discount, qh.grandTotal,
      user, user
    ).run();

    const items = await db.prepare(`SELECT * FROM sales_quotationitems WHERE qNo=?`).bind(qNo).all();
    const ins = db.prepare(`
      INSERT INTO sales_saleorderitems (soNo, itemCode, itemName, qty, uom, unitPrice, lineTotal, remark, CreateDate, UpdateDate, CreateBy, UpdateBy)
      VALUES (?, ?, ?, ?, 'EA', ?, ?, '', datetime('now'), datetime('now'), ?, ?)
    `);
    for (const it of (items.results||[])) {
      await ins.bind(soNo, it.itemCode||"", it.itemName||"", it.qty||0, it.unitPrice||0, it.lineTotal||0, user, user).run();
    }

    const so = await db.prepare(`SELECT * FROM sales_saleorders WHERE soNo=?`).bind(soNo).first();
    return json({ ok:true, data: { so, items: items.results||[] } });
  };

  // ---------- Router ----------
  try {
    const seg = subpath.split("/").filter(Boolean); // ["tables","hr_employees"] or ["hr","employee-master"]
    const top = seg[0]?.toLowerCase();

    // /api/geo/*
    if (top === "geo") return await geoRouter(seg);

    // /api/tables/*
    if (top === "tables") {
      if (seg.length === 1) {
        if (method !== "GET") return json({ ok:false, error:"Method not allowed" },405);
        return json({ ok:true, data: await listTables() });
      }
      const table = safeIdent(seg[1]);
      if (seg.length === 2) {
        const crud = makeCrud(table);
        if (method === "GET")    return await crud.list();
        if (method === "POST")   return await crud.create();
        if (method === "PUT")    return await crud.update();
        if (method === "PATCH")  return await crud.update();
        if (method === "DELETE") return await crud.remove();
        return json({ ok:false, error:"Method not allowed" },405);
      }
      if (seg.length === 3 && seg[2].toLowerCase() === "columns") {
        if (method !== "GET") return json({ ok:false, error:"Method not allowed" },405);
        return json({ ok:true, data: await getColumns(table) });
      }
      if (seg.length === 3 && seg[2].toLowerCase() === "bulk") {
        if (method !== "POST") return json({ ok:false, error:"Method not allowed" },405);
        return await makeCrud(table).bulkCreate();
      }
      return json({ ok:false, error:"Not found" },404);
    }

    // Shortcuts (ดั้งเดิม)
    const shortcutMap = {
      "hr/employees":         "hr_employees",
      "hr/attendance":        "hr_attendance",
      "hr/timeclock":         "hr_timeclock",
      "sales/customers":      "sales_customers",
      "sales/quotations":     "sales_quotations",
      "sales/quotationitems": "sales_quotationitems",
      "sales/saleorders":     "sales_saleorders",
      "sales/saleorderitems": "sales_saleorderitems",
      "inv/products":         "inv_products",
    };
    const joined = seg.slice(0,2).join("/").toLowerCase();
    if (shortcutMap[joined]) {
      return await aliasCrud(seg.slice(1), shortcutMap[joined]); // รองรับ /columns /bulk
    }

    // Alias ให้ชื่อ route ตรงกับ UI
    if (top === "hr") {
      if (seg[1]?.toLowerCase() === "employee-master")  return await aliasCrud(seg.slice(1), "hr_employees");
      if (seg[1]?.toLowerCase() === "time-attendance")  return await aliasCrud(seg.slice(1), "hr_attendance");
      if (["clock","clock-in-out"].includes((seg[1]||"").toLowerCase())) return await aliasCrud(seg.slice(1), "hr_timeclock");
      if (seg[1]?.toLowerCase() === "leave")            return await aliasCrud(seg.slice(1), "hr_attendance"); // หรือ hr_leaves ถ้าแยก
    }

    if (top === "sales") {
      if (seg[1]?.toLowerCase() === "customer-profile") return await aliasCrud(seg.slice(1), "sales_customers");

      // helpers
      if (seg[1]?.toLowerCase() === "quotations" && seg[2]?.toLowerCase() === "confirm" && method === "POST") {
        return await confirmQuotation();
      }
      if (seg[1]?.toLowerCase() === "saleorders" && seg[2]?.toLowerCase() === "create-from-quotation" && method === "POST") {
        return await createSOFromQuotation();
      }
    }

    return json({ ok:false, error:`No route for: ${subpath}` },404);

  } catch (err) {
    console.error("[[path]] error:", err);
    return json({ ok:false, error: err.message || String(err) }, 500);
  }
};
