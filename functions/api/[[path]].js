// functions/api/[[path]].js — CLEAN & STABLE

export const onRequest = async ({ request, env }) => {
  // ========= Bootstraps =========
  const url = new URL(request.url);
  if (!url.pathname.startsWith("/api")) return notFound();

  const method = request.method.toUpperCase();
  const db = env.DB; // D1 binding
  const origin = request.headers.get("Origin") || "*";
  const headers = {
    "content-type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-User",
  };
  if (method === "OPTIONS") return new Response(null, { status: 204, headers });

  const seg = url.pathname.replace(/^\/api\/?/, "").split("/").filter(Boolean);
  const idFromPath = seg.length >= 3 ? decodeURIComponent(seg[2]) : null;
  const q = (k, d = "") => (url.searchParams.get(k) ?? d).trim();

  // ========= Small utilities =========
  const json = (data, status = 200) =>
    new Response(JSON.stringify(data), { status, headers });

  function notFound() { return new Response("Not Found", { status: 404 }); }

  async function readBody() {
    const ct = (request.headers.get("content-type") || "").toLowerCase();
    if (ct.includes("application/json")) {
      try { return await request.json(); } catch { return {}; }
    }
    if (ct.includes("application/x-www-form-urlencoded")
        || ct.includes("multipart/form-data")) {
      const fd = await request.formData();
      return Object.fromEntries(fd.entries());
    }
    const raw = await request.text();
    try { return JSON.parse(raw); } catch { return {}; }
  }

  const safeIdent = (name) => {
    if (!/^[A-Za-z0-9_]+$/.test(name)) throw new Error(`Invalid identifier: ${name}`);
    return name;
  };

  const getColumns = async (table) => {
    const r = await db.prepare(`PRAGMA table_info(${table})`).all();
    return r?.results || r || [];
  };
  const hasColumn = async (table, col) =>
    (await getColumns(table)).some(c => c.name === col);
  const getPK = async (table) => {
    const cols = await getColumns(table);
    const pk = cols.find(c => c.pk === 1)?.name;
    if (pk) return pk;
    if (cols.some(c => c.name.toLowerCase() === "id")) return "id";
    return "rowid";
  };

  const buildInsert = (table, dataObj) => {
    const keys = Object.keys(dataObj);
    if (!keys.length) throw new Error("Empty object");
    const cols = keys.map(k => `"${safeIdent(k)}"`).join(", ");
    const vals = keys.map(k => dataObj[k]?.__raw ? dataObj[k].__raw : "?").join(", ");
    const bind = keys.filter(k => !dataObj[k]?.__raw).map(k => dataObj[k]);
    return { sql: `INSERT INTO ${table} (${cols}) VALUES (${vals}) RETURNING *`, bind };
  };
  const buildUpdate = (table, dataObj, idField) => {
    const keys = Object.keys(dataObj);
    if (!keys.length) throw new Error("Empty object");
    const sets = keys.map(k =>
      dataObj[k]?.__raw
        ? `"${safeIdent(k)}"=${dataObj[k].__raw}`
        : `"${safeIdent(k)}"=?`
    ).join(", ");
    const bind = keys.filter(k => !dataObj[k]?.__raw).map(k => dataObj[k]);
    return { sql: `UPDATE ${table} SET ${sets} WHERE ${idField}=? RETURNING *`, bind };
  };

  // ========= HEALTH =========
  if (seg.length === 0) return json({ ok: true, service: "bizapp-api", time: new Date().toISOString() });

  // ========= GEO (single table: geo_admin) =========
  if (seg[0] === "geo") {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS geo_admin(
        id INTEGER PRIMARY KEY,
        parent_id INTEGER,
        level TEXT NOT NULL CHECK(level IN ('province','amphure','tambon')),
        name TEXT NOT NULL,
        zipcode TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_geo_admin_level ON geo_admin(level);
      CREATE INDEX IF NOT EXISTS idx_geo_admin_parent ON geo_admin(parent_id);
      CREATE INDEX IF NOT EXISTS idx_geo_admin_zip ON geo_admin(zipcode);
    `);

    // minimal seed for first run (safe: OR IGNORE)
    const c = await db.prepare(`SELECT COUNT(*) n FROM geo_admin`).first();
    if (!c || !c.n) {
      await db.batch([
        db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(10,null,'province','กรุงเทพมหานคร',null),
        db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(11,null,'province','สมุทรปราการ',null),
        db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(12,null,'province','นนทบุรี',null),
        db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(1001,10,'amphure','พระนคร',null),
        db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(1012,10,'amphure','ห้วยขวาง',null),
        db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(101201,1012,'tambon','ห้วยขวาง','10310')
      ]);
    }

    if (seg[1] === "provinces" && method === "GET") {
      const rs = await db.prepare(
        `SELECT id, name FROM geo_admin WHERE level='province' ORDER BY name`
      ).all();
      return json(rs.results || []);
    }
    if (seg[1] === "amphures" && method === "GET") {
      const pid = q("province_id");
      if (!pid) return json({ error: "province_id required" }, 400);
      const rs = await db.prepare(
        `SELECT id, name, parent_id AS province_id
           FROM geo_admin
          WHERE level='amphure' AND parent_id=?
          ORDER BY name`
      ).bind(pid).all();
      return json(rs.results || []);
    }
    if (seg[1] === "tambons" && method === "GET") {
      const aid = q("amphure_id");
      if (!aid) return json({ error: "amphure_id required" }, 400);
      const rs = await db.prepare(
        `SELECT id, name, zipcode, parent_id AS amphure_id
           FROM geo_admin
          WHERE level='tambon' AND parent_id=?
          ORDER BY name`
      ).bind(aid).all();
      return json(rs.results || []);
    }
    if (seg[1] === "status" && method === "GET") {
      const p = await db.prepare(`SELECT COUNT(*) n FROM geo_admin WHERE level='province'`).first();
      const a = await db.prepare(`SELECT COUNT(*) n FROM geo_admin WHERE level='amphure'`).first();
      const t = await db.prepare(`SELECT COUNT(*) n FROM geo_admin WHERE level='tambon'`).first();
      return json({ provinces: p?.n||0, amphures: a?.n||0, tambons: t?.n||0 });
    }
    return json({ error: "Not Found" }, 404);
  }

  // ========= GENERIC CRUD (covers /api/hr/*, /api/sales/*, /api/inv/*, /api/purch/*) =========
  if (["hr","sales","inv","purch"].includes(seg[0]) && seg.length >= 2) {
    const table = `${safeIdent(seg[0])}_${safeIdent(seg[1])}`;
    const pk = await getPK(table);

    if (method === "GET" && !idFromPath) {
      // common filters
      const limit = Math.min(+q("limit","100")||100, 1000);
      const offset = Math.max(+q("offset","0")||0, 0);
      const search = q("search","");
      const from   = q("from","");
      const to     = q("to","");

      // build simple WHERE dynamically
      const cols = await getColumns(table);
      const names = cols.map(c=>c.name);
      const params = [];
      let where = "1=1";

      if (search) {
        const hay = names.filter(n => /code|name|firstName|lastName|email|phone|desc|id/i.test(n));
        if (hay.length) {
          where += " AND (" + hay.map(n => `${n} LIKE ?`).join(" OR ") + ")";
          hay.forEach(()=>params.push(`%${search}%`));
        }
      }
      if (from && names.includes("date")) { where += " AND date>=?"; params.push(from); }
      if (to   && names.includes("date")) { where += " AND date<=?"; params.push(to);   }

      const sql = `SELECT * FROM ${table} WHERE ${where} ORDER BY ${pk} DESC LIMIT ? OFFSET ?`;
      const { results } = await db.prepare(sql).bind(...params, limit, offset).all();
      return json({ ok:true, data: results });
    }

    if (method === "GET" && idFromPath) {
      const row = await db.prepare(`SELECT * FROM ${table} WHERE ${pk}=?`).bind(idFromPath).first();
      return row ? json(row) : json({ error:"not found" }, 404);
    }

    if (method === "POST") {
      const body = await readBody();
      const { sql, bind } = buildInsert(table, body);
      const row = await db.prepare(sql).bind(...bind).first();
      return json(row || {});
    }

    if ((method === "PUT" || method === "PATCH") && idFromPath) {
      const body = await readBody();
      const { sql, bind } = buildUpdate(table, body, pk);
      const row = await db.prepare(sql).bind(...bind, idFromPath).first();
      return row ? json(row) : json({ error:"not found" }, 404);
    }

    if (method === "DELETE" && idFromPath) {
      const row = await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`)
        .bind(idFromPath).first();
      return row ? json(row) : json({ error:"not found" }, 404);
    }

    return json({ error: "method not allowed" }, 405);
  }

  // ========= FALLBACK =========
  return json({ error: "Not Found" }, 404);
};
