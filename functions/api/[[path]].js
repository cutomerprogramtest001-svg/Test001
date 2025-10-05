// functions/api/[[path]].js
export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  // Expect paths like: /api/hr/employees, /api/employees, /api/tables, /api/tables/<table>, /api/tables/<table>/columns
  if (!url.pathname.startsWith('/api')) {
    return json({ error: 'Not found' }, 404);
  }
 // --- Global CORS preflight for ALL /api routes ---
  if (request.method === 'OPTIONS') {
    const origin = request.headers.get('Origin') || '*';
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': origin,
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, X-User',
        'Access-Control-Allow-Credentials': 'true'
      }
    });
  } 
  const path = url.pathname.replace(/^\/api\/?/, '').trim(); // strip /api/
  // ========== [Sales Module] — drop-in router (append-safe) ==========
async function salesRouter(path, method, url, request, env) {
  if (!path.startsWith('sales/')) return null;

  const db   = env.DB;
  const user = request.headers.get('x-user') || 'system';
  const send = (data, status = 200) =>
    new Response(JSON.stringify(data), {
      status,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': request.headers.get('Origin') || '*',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, X-User',
        'Access-Control-Allow-Credentials': 'true',
      },
    });

  if (method === 'OPTIONS') return send(null, 204);

  // helpers (เฉพาะ scope นี้)
  const safeJsonLocal = async () => {
    const t = await request.text(); if (!t) return {};
    try { return JSON.parse(t); } catch { return {}; }
  };
  const ident = (s) => String(s||'').replace(/[^a-zA-Z0-9_]/g,'');
  const getCols = async (table) => {
    const rs = await db.prepare(`PRAGMA table_info(${ident(table)})`).all();
    return (rs.results || []).map(r => r.name);
  };
  const withAuditInsert = (b, user, cols) => {
    const out = {};
    for (const k of Object.keys(b||{})) {
      if (!cols.includes(k)) continue;
      if (k==='id'||k==='CreateDate'||k==='UpdateDate') continue;
      out[k] = b[k];
    }
    if (cols.includes('CreateBy')) out.CreateBy = user;
    if (cols.includes('UpdateBy')) out.UpdateBy = user;
    return out;
  };
  const withAuditUpdate = (b, user, cols) => {
    const out = {};
    for (const k of Object.keys(b||{})) {
      if (!cols.includes(k)) continue;
      if (k==='id'||k==='CreateDate'||k==='UpdateDate') continue;
      out[k] = b[k];
    }
    if (cols.includes('UpdateBy')) out.UpdateBy = user;
    return out;
  };
  const pagin = (def=50) => {
    const sp = url.searchParams;
    const limit  = Math.min(Math.max(parseInt(sp.get('limit')||def,10),1),500);
    const offset = Math.max(parseInt(sp.get('offset')||'0',10),0);
    return { limit, offset };
  };

  // parse path: /api/sales/<table>[/<id>] -> physical = sales_<table>
  const segs  = path.split('/').filter(Boolean); // ['sales','orders','123']
  const tBase = segs[1];
  const rowId = segs[2] || null;
  if (!tBase) return send({ ok:false, error:'Missing sales table' }, 400);

  const table = `sales_${ident(tBase)}`;
  const exists = await db.prepare(
    `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`
  ).bind(table).first();
  if (!exists) return send({ ok:false, error:`Sales table not found: ${table}` }, 404);

  const cols = await getCols(table);
  const sp   = url.searchParams;

  // LIST
  if (method === 'GET' && !rowId) {
    const { limit, offset } = pagin(50);
    const where = [], binds = [];

    const from   = sp.get('from');   // yyyy-mm-dd
    const to     = sp.get('to');
    const status = sp.get('status');
    const cust   = sp.get('customer') || sp.get('customerId');
    const search = sp.get('search');

    const dateCol = ['date','docDate','orderDate','invoiceDate','createdAt','CreateDate'].find(c => cols.includes(c));
    if (status && cols.includes('status')) { where.push('status = ?'); binds.push(status); }
    if (cust) {
      if (cols.includes('customerId')) { where.push('customerId = ?'); binds.push(cust); }
      else if (cols.includes('customer')) { where.push('customer = ?'); binds.push(cust); }
    }
    if (dateCol && (from || to)) {
      if (from && to) { where.push(`${ident(dateCol)} BETWEEN ? AND ?`); binds.push(from, to); }
      else if (from)  { where.push(`${ident(dateCol)} >= ?`);          binds.push(from); }
      else if (to)    { where.push(`${ident(dateCol)} <= ?`);          binds.push(to); }
    }
    if (search) {
      // แทนที่บรรทัดเดิมทั้งหมดของ sCols ด้วยบรรทัดนี้
      const sCols = (() => {
        // คอลัมน์ค้นหาปกติฝั่ง sales (จะกรองเฉพาะคอลัมน์ที่มีจริงในตารางนั้น ๆ)
        let base = ['docNo','customerName','note','remark','refNo'].filter(c => cols.includes(c));
        // ถ้าเป็นตารางลูกค้า → ใช้ชุดคอลัมน์นี้แทน
        if (table === 'sales_customers') {
          base = ['code','firstName','lastName','nationalId','phone','email'].filter(c => cols.includes(c));
        }
        return base;
      })();

    }

    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const orderBy  = dateCol ? `${ident(dateCol)} DESC, id DESC` : `id DESC`;

    const list = await db.prepare(
      `SELECT * FROM ${ident(table)} ${whereSql} ORDER BY ${orderBy} LIMIT ? OFFSET ?`
    ).bind(...binds, limit, offset).all();

    const total = await db.prepare(`SELECT COUNT(*) AS c FROM ${ident(table)} ${whereSql}`)
      .bind(...binds).first();

    return send({ ok:true, data:list.results||[], total: total?.c||0, limit, offset });
  }

  // GET one
  if (method === 'GET' && rowId) {
    const row = await db.prepare(`SELECT * FROM ${ident(table)} WHERE id=?`).bind(rowId).first();
    return row ? send({ ok:true, data:row }) : send({ ok:false, error:'Not found' }, 404);
  }
    
      // CREATE
      if (method === 'POST') {
        const body = await safeJsonLocal();
        const payload = withAuditInsert(body, user, cols);
        if (!Object.keys(payload).length) return send({ ok:false, error:'Empty payload' }, 400);
    // >>> add: duplicate-check (POST) for sales_customers
    if (table === 'sales_customers') {
      if (payload.code) payload.code = String(payload.code).trim();
      if (payload.code) {
        // เทียบแบบไม่แคร์ตัวพิมพ์ + ตัดช่องว่าง เพื่อกัน CU001 กับ cu001
        const dup = await db.prepare(
          `SELECT id FROM sales_customers WHERE LOWER(TRIM(code)) = LOWER(TRIM(?)) LIMIT 1`
        ).bind(payload.code).first();
        if (dup) {
          return send({ ok:false, error:`Duplicate customer code: ${payload.code}` }, 409);
        }
      }
    }

    const keys = Object.keys(payload);
    const q = `INSERT INTO ${ident(table)} (${keys.map(ident).join(', ')})
               VALUES (${keys.map(()=>'?').join(', ')})`;
    const info = await db.prepare(q).bind(...keys.map(k=>payload[k])).run();
    const id = info.lastRowId ?? info.meta?.last_row_id;
    const row = await db.prepare(`SELECT * FROM ${ident(table)} WHERE id=?`).bind(id).first();
    return send({ ok:true, data: row, id }, 201);
  }

  // UPDATE
  if (method === 'PUT' && rowId) {
    const body = await safeJsonLocal();
    const payload = withAuditUpdate(body, user, cols);
    // >>> add: duplicate-check (PUT) for sales_customers
    if (table === 'sales_customers') {
      if (payload.code) payload.code = String(payload.code).trim();
      if (payload.code) {
        const dup = await db.prepare(
          `SELECT id FROM sales_customers WHERE LOWER(TRIM(code)) = LOWER(TRIM(?)) AND id <> ? LIMIT 1`
        ).bind(payload.code, rowId).first();
        if (dup) {
          return send({ ok:false, error:`Duplicate customer code: ${payload.code}` }, 409);
        }
      }
    }

    const keys = Object.keys(payload);
    if (!keys.length) return send({ ok:false, error:'No updatable fields' }, 400);
    const setSql = keys.map(k=>`${ident(k)}=?`).join(', ');
    const extra  = cols.includes('UpdateDate') ? `, UpdateDate = datetime('now')` : '';
    const sql = `UPDATE ${ident(table)} SET ${setSql}${extra} WHERE id=?`;
    const binds = keys.map(k=>payload[k]); binds.push(rowId);
    const res = await db.prepare(sql).bind(...binds).run();
    return send({ ok:true, changes: res.changes });
  }

  // DELETE
  if (method === 'DELETE' && rowId) {
    const res = await db.prepare(`DELETE FROM ${ident(table)} WHERE id=?`).bind(rowId).run();
    return send({ ok:true, changes: res.changes });
  }

  return send({ ok:false, error:'Unsupported sales route or method' }, 405);
}
globalThis.__salesRouter = salesRouter;

    // ========== [HR Attendance & TimeClock] — drop-in router (append-safe) ==========
  // ใช้เมื่อคุณไม่อยากแก้โครงไฟล์เดิมมาก: แค่ "เรียก" hrRouter() ใน onRequest เดิมก็พอ
  // ต้องมี D1 binding ชื่อ env.DB
  async function hrRouter(path, method, url, request, env) {
    // รับเฉพาะเส้นทาง hr/attendance*, hr/timeclock*
    if (!path.startsWith("hr/attendance") && !path.startsWith("hr/timeclock")) return null;

    const db = env.DB;
    const user = request.headers.get("x-user") || "system";
    const send = (data, status = 200) =>
      new Response(JSON.stringify(data), {
        status,
        headers: {
          "Content-Type": "application/json; charset=utf-8",
          "Access-Control-Allow-Origin": request.headers.get("Origin") || "*",
          "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, X-User",
          "Access-Control-Allow-Credentials": "true",
        },
      });

    if (method === "OPTIONS") return send(null, 204);

    // ----- helpers (เฉพาะในสcopeนี้ เพื่อไม่ชนของเดิม) -----
    const safeJsonLocal = async () => {            // ⬅️ rename กันชนกับ safeJson ด้านล่างไฟล์
      const t = await request.text();
      if (!t) return {};
      try { return JSON.parse(t); } catch { return {}; }
    };
    const isoDateOnly = (s) => {
      if (!s) return "";
      const d = new Date(s);
      if (Number.isNaN(d.getTime())) return "";
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${dd}`;
    };
    const hoursBetween = (inAt, outAt) => {
      const a = new Date(inAt), b = new Date(outAt);
      if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return 0;
      return Math.max(0, (b.getTime() - a.getTime()) / 3_600_000);
    };
    const pagin = (def = 50) => {
      const limit = Math.min(Math.max(parseInt(url.searchParams.get("limit") || def, 10), 1), 500);
      const offset = Math.max(parseInt(url.searchParams.get("offset") || "0", 10), 0);
      return { limit, offset };
    };

    // -------------------- Attendance --------------------
    if (path === "hr/attendance") {
      if (method === "GET") {
        const { limit, offset } = pagin(50);
        const sp = url.searchParams;
        const where = [];
        const binds = [];

        // คีย์ฟิลเตอร์หลัก (ตาม UI)
        const search = sp.get("search");
        const empId = sp.get("empId");
        const position = sp.get("position");
        const leaveType = sp.get("leaveType");
        const status = sp.get("status");
        const from = isoDateOnly(sp.get("from"));
        const to   = isoDateOnly(sp.get("to"));

        if (empId)    { where.push("empId = ?");    binds.push(empId); }
        if (position) { where.push("position = ?"); binds.push(position); }
        if (leaveType){ where.push("leaveType = ?");binds.push(leaveType); }
        if (status)   { where.push("status = ?");   binds.push(status); }
        if (from && to) { where.push("date BETWEEN ? AND ?"); binds.push(from, to); }
        else if (from)  { where.push("date >= ?");  binds.push(from); }
        else if (to)    { where.push("date <= ?");  binds.push(to); }

        if (search && !empId) {
          where.push("(empId LIKE ? OR fullName LIKE ? OR note LIKE ?)");
          binds.push(`%${search}%`, `%${search}%`, `%${search}%`);
        }

        const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
        const list = await db.prepare(
          `SELECT id, empId, fullName, position, date, checkIn, checkOut, status, leaveType, note, geo,
                  CreateDate, CreateBy, UpdateDate, UpdateBy
             FROM hr_attendance
             ${whereSql}
             ORDER BY date DESC, id DESC
             LIMIT ? OFFSET ?`
        ).bind(...binds, limit, offset).all();

        const total = await db.prepare(`SELECT COUNT(*) AS c FROM hr_attendance ${whereSql}`)
          .bind(...binds).first();

        return send({ ok: true, data: list.results || [], total: total?.c || 0, limit, offset });
      }

      if (method === "POST") {
        const body = await safeJsonLocal();
        const empId     = (body.empId || "").trim();
        const fullName  = (body.fullName || "").trim();
        const position  = (body.position || "").trim();
        const date      = isoDateOnly(body.date);
        const checkIn   = (body.checkIn || "").trim();
        const checkOut  = (body.checkOut || "").trim();
        const status    = (body.status || "Pending").trim();
        const leaveType = (body.leaveType || "").trim();
        const note      = (body.note || "").trim();
        const geo       = (body.geo || "").trim();

        if (!empId || !fullName || !date) {
          return send({ ok: false, error: "empId, fullName, date are required" }, 400);
        }

        const stmt = await db.prepare(
          `INSERT INTO hr_attendance
           (empId, fullName, position, date, checkIn, checkOut, status, leaveType, note, geo,
            CreateBy, UpdateBy)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).bind(empId, fullName, position, date, checkIn, checkOut, status, leaveType, note, geo, user, user).run();

        return send({ ok: true, id: stmt.lastRowId });
      }
    }

    if (path.startsWith("hr/attendance/")) {
      const id = path.split("/")[2];
      if (!id) return send({ ok: false, error: "Missing attendance id" }, 400);

      if (method === "PUT") {
        const body = await safeJsonLocal();
        const cols = {
          empId: body.empId,
          fullName: body.fullName,
          position: body.position,
          date: body.date ? isoDateOnly(body.date) : undefined,
          checkIn: body.checkIn,
          checkOut: body.checkOut,
          status: body.status,
          leaveType: body.leaveType,
          note: body.note,
          geo: body.geo,
        };
        const keys = Object.keys(cols).filter(k => cols[k] !== undefined);
        if (keys.length === 0) return send({ ok: false, error: "No fields to update" }, 400);

        const sets = keys.map(k => `${k} = ?`).join(", ");
        const binds = keys.map(k => cols[k]);
        const sql = `UPDATE hr_attendance SET ${sets}, UpdateDate = datetime('now'), UpdateBy = ? WHERE id = ?`;
        const res = await db.prepare(sql).bind(...binds, user, id).run();
        return send({ ok: true, changes: res.changes });
      }

      if (method === "DELETE") {
        const res = await db.prepare(`DELETE FROM hr_attendance WHERE id = ?`).bind(id).run();
        return send({ ok: true, changes: res.changes });
      }
    }

    // -------------------- TimeClock --------------------
    if (path === "hr/timeclock") {
      if (method === "GET") {
        const { limit, offset } = pagin(100);
        const sp = url.searchParams;
        const where = [];
        const binds = [];
        const emp = sp.get("emp");
        const from = isoDateOnly(sp.get("from"));
        const to   = isoDateOnly(sp.get("to"));

        if (emp)  { where.push("empId = ?"); binds.push(emp); }
        if (from && to) { where.push("date BETWEEN ? AND ?"); binds.push(from, to); }
        else if (from)  { where.push("date >= ?"); binds.push(from); }
        else if (to)    { where.push("date <= ?"); binds.push(to); }

        const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
        const list = await db.prepare(
          `SELECT id, empId, date, inAt, outAt, hours, note, geo,
                  CreateDate, CreateBy, UpdateDate, UpdateBy
             FROM hr_timeclock
             ${whereSql}
             ORDER BY date DESC, id DESC
             LIMIT ? OFFSET ?`
        ).bind(...binds, limit, offset).all();

        const total = await db.prepare(`SELECT COUNT(*) AS c FROM hr_timeclock ${whereSql}`)
          .bind(...binds).first();

        return send({ ok: true, data: list.results || [], total: total?.c || 0, limit, offset });
      }

      if (method === "POST") {
        // clock-in (หรือสร้าง record พร้อม outAt ถ้าส่งมาด้วย)
        const body = await safeJsonLocal();
        const empId = (body.empId || "").trim();
        if (!empId) return send({ ok: false, error: "empId is required" }, 400);

        const date  = isoDateOnly(body.date || new Date().toISOString());
        const inAt  = body.inAt ? new Date(body.inAt).toISOString() : new Date().toISOString();
        const outAt = body.outAt ? new Date(body.outAt).toISOString() : null;
        const note  = (body.note || "").trim();
        const geo   = (body.geo || "").trim();
        const hours = outAt ? Number(hoursBetween(inAt, outAt).toFixed(4)) : 0;

        const stmt = await db.prepare(
          `INSERT INTO hr_timeclock (empId, date, inAt, outAt, hours, note, geo, CreateBy, UpdateBy)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).bind(empId, date, inAt, outAt, hours, note, geo, user, user).run();

        return send({ ok: true, id: stmt.lastRowId, hours });
      }
    }

    if (path.startsWith("hr/timeclock/")) {
      const id = path.split("/")[2];
      if (!id) return send({ ok: false, error: "Missing timeclock id" }, 400);

      if (method === "PUT") {
        const body = await safeJsonLocal();
        const inAt  = body.inAt  ? new Date(body.inAt).toISOString() : undefined;
        const outAt = body.outAt ? new Date(body.outAt).toISOString() : undefined;

        // ถ้าส่งมาไม่ครบ ต้องอ่านค่าปัจจุบันมาเติม เพื่อคำนวณ hours ใหม่
        let cur = null;
        if (inAt === undefined || outAt === undefined || body.hours === undefined) {
          cur = await db.prepare(`SELECT inAt, outAt FROM hr_timeclock WHERE id = ?`).bind(id).first();
        }
        const finInAt  = inAt  !== undefined ? inAt  : cur?.inAt  || null;
        const finOutAt = outAt !== undefined ? outAt : cur?.outAt || null;
        const hours = (body.hours !== undefined)
          ? Number(body.hours)
          : (finInAt && finOutAt ? Number(hoursBetween(finInAt, finOutAt).toFixed(4)) : 0);

        const cols = {
          empId:  body.empId,
          date:   body.date ? isoDateOnly(body.date) : undefined,
          inAt:   inAt,
          outAt:  outAt,
          hours:  hours,
          note:   body.note,
          geo:    body.geo,
        };
        const keys = Object.keys(cols).filter(k => cols[k] !== undefined);
        if (keys.length === 0) return send({ ok: false, error: "No fields to update" }, 400);

        const sets = keys.map(k => `${k} = ?`).join(", ");
        const binds = keys.map(k => cols[k]);
        const sql = `UPDATE hr_timeclock SET ${sets}, UpdateDate = datetime('now'), UpdateBy = ? WHERE id = ?`;
        const res = await db.prepare(sql).bind(...binds, user, id).run();
        return send({ ok: true, changes: res.changes, hours });
      }

      if (method === "DELETE") {
        const res = await db.prepare(`DELETE FROM hr_timeclock WHERE id = ?`).bind(id).run();
        return send({ ok: true, changes: res.changes });
      }
    }

    // ถ้าเงื่อนไขไม่เข้า ให้คืน null เพื่อให้ onRequest เดิมไปทำงานต่อ
    return null;
  }

  // helper เผื่อไฟล์อื่นอยากใช้
  globalThis.__hrRouter = hrRouter;
  
  const method = request.method.toUpperCase();
  const hrResp = await hrRouter(path, method, url, request, env);
  if (hrResp) return hrResp;
  const salesResp = await salesRouter(path, method, url, request, env);
  if (salesResp) return salesResp;
  r = await geoRouter(path, method, url, request, env);   // <<< ADD
  if (r) return r;
  const user = request.headers.get('x-user') || 'system';
  const db = env.DB; // <-- bind D1 as "DB" in Pages > Settings > Functions > D1
// ===== GEO Auto-Create Tables + Auto-Seed + Router (append-safe) =====
async function ensureGeoTables(db) {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS th_provinces (
      id        INTEGER PRIMARY KEY,
      code      TEXT,
      name_th   TEXT NOT NULL,
      name_en   TEXT
    );
    CREATE TABLE IF NOT EXISTS th_amphures (
      id          INTEGER PRIMARY KEY,
      province_id INTEGER NOT NULL,
      code        TEXT,
      name_th     TEXT NOT NULL,
      name_en     TEXT
    );
    CREATE TABLE IF NOT EXISTS th_tambons (
      id          INTEGER PRIMARY KEY,
      amphure_id  INTEGER NOT NULL,
      code        TEXT,
      name_th     TEXT NOT NULL,
      name_en     TEXT,
      zipcode     TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_amphures_province ON th_amphures(province_id, name_th);
    CREATE INDEX IF NOT EXISTS idx_tambons_amphure  ON th_tambons(amphure_id, name_th);
    CREATE INDEX IF NOT EXISTS idx_tambons_zipcode  ON th_tambons(zipcode);
  `);
}

const GEO_DATASETS = {
  provinces: 'https://cdn.jsdelivr.net/gh/kongvut/thai-province-data@latest/json/provinces.json',
  amphures:  'https://cdn.jsdelivr.net/gh/kongvut/thai-province-data@latest/json/amphures.json',
  tambons:   'https://cdn.jsdelivr.net/gh/kongvut/thai-province-data@latest/json/tambons.json',
};

async function ensureGeoSeed(db, fetch, { force=false } = {}) {
  await ensureGeoTables(db);

  const getCount = async (tbl) => {
    const row = await db.prepare(`SELECT COUNT(1) AS c FROM ${tbl}`).first();
    return (row && (row.c ?? row.count ?? row.C)) || 0;
  };
  const [pc, ac, tc] = await Promise.all([
    getCount('th_provinces').catch(()=>0),
    getCount('th_amphures').catch(()=>0),
    getCount('th_tambons').catch(()=>0),
  ]);

  const autoEnv = (typeof AUTOSEED !== 'undefined' ? String(AUTOSEED) : (globalThis?.AUTOSEED || 'true'));
  const shouldSeed = force || (autoEnv !== 'false' && (pc === 0 || ac === 0 || tc === 0));
  if (!shouldSeed) return { seeded:false, pc, ac, tc };

  const [pRes, aRes, tRes] = await Promise.all([
    fetch(GEO_DATASETS.provinces),
    fetch(GEO_DATASETS.amphures),
    fetch(GEO_DATASETS.tambons),
  ]);
  if (!pRes.ok || !aRes.ok || !tRes.ok) throw new Error('Fetch GEO dataset failed');
  const [pJs, aJs, tJs] = await Promise.all([pRes.json(), aRes.json(), tRes.json()]);

  const exec = (sql, binds=[]) => db.prepare(sql).bind(...binds).run();
  await db.exec('BEGIN');
  try {
    if (pc === 0) {
      const sql = `INSERT OR IGNORE INTO th_provinces(id, code, name_th, name_en) VALUES (?, ?, ?, ?)`;
      for (const p of pJs) {
        await exec(sql, [Number(p.id), String(p.code ?? p.id), p.name_th || p.name, p.name_en || null]);
      }
    }
    if (ac === 0) {
      const sql = `INSERT OR IGNORE INTO th_amphures(id, province_id, code, name_th, name_en) VALUES (?, ?, ?, ?, ?)`;
      for (const a of aJs) {
        await exec(sql, [Number(a.id), Number(a.province_id), String(a.code ?? a.id), a.name_th || a.name, a.name_en || null]);
      }
    }
    if (tc === 0) {
      const sql = `INSERT OR IGNORE INTO th_tambons(id, amphure_id, code, name_th, name_en, zipcode) VALUES (?, ?, ?, ?, ?, ?)`;
      for (const t of tJs) {
        const z = Array.isArray(t.zip_code) ? t.zip_code[0] : (t.zip_code ?? t.zipcode ?? null);
        await exec(sql, [Number(t.id), Number(t.amphure_id), String(t.code ?? t.id), t.name_th || t.name, t.name_en || null, z ? String(z) : null]);
      }
    }
    await db.exec('COMMIT');
  } catch (e) {
    await db.exec('ROLLBACK'); throw e;
  }
  return { seeded:true };
}

async function geoRouter({ request, url, path, db, send }) {
  if (!path.startsWith('geo/')) return;
  const method = request.method.toUpperCase();
  const qs = Object.fromEntries(url.searchParams.entries());

  // CORS
  if (method === 'OPTIONS') {
    return send({ ok:true }, 200, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, x-user'
    });
  }
  if (method !== 'GET') {
    return send({ ok:false, error:'Method Not Allowed' }, 405, { 'Access-Control-Allow-Origin': '*' });
  }

  // สร้างตาราง + seed ถ้าจำเป็น (หรือบังคับด้วย ?autoseed=1)
  try { await ensureGeoSeed(db, fetch, { force: qs.autoseed === '1' }); } catch (e) { /* เงียบไว้ให้ list ทำงานต่อได้ */ }

  const send200 = (data) => send({ ok:true, data }, 200, { 'Access-Control-Allow-Origin': '*' });

  if (path === 'geo/provinces') {
    const search = (qs.search || '').trim();
    let sql = `SELECT id, name_th AS nameTh, name_en AS nameEn FROM th_provinces`;
    const binds = [];
    if (search) {
      sql += ` WHERE LOWER(name_th) LIKE LOWER(?) OR LOWER(name_en) LIKE LOWER(?)`;
      const pat = `%${search}%`; binds.push(pat, pat);
    }
    sql += ` ORDER BY name_th`;
    const rows = binds.length ? await db.prepare(sql).bind(...binds).all() : await db.prepare(sql).all();
    return send200(rows.results || rows);
  }

  if (path === 'geo/amphures') {
    const pid = Number(qs.province_id || qs.provinceId || 0) || 0;
    if (!pid) return send({ ok:false, error:'province_id required' }, 400, { 'Access-Control-Allow-Origin': '*' });
    const search = (qs.search || '').trim();
    let sql = `SELECT id, province_id AS provinceId, name_th AS nameTh, name_en AS nameEn
               FROM th_amphures WHERE province_id = ?`;
    const binds = [pid];
    if (search) {
      sql += ` AND (LOWER(name_th) LIKE LOWER(?) OR LOWER(name_en) LIKE LOWER(?))`;
      const pat = `%${search}%`; binds.push(pat, pat);
    }
    sql += ` ORDER BY name_th`;
    const rows = await db.prepare(sql).bind(...binds).all();
    return send200(rows.results || rows);
  }

  if (path === 'geo/tambons') {
    const aid = Number(qs.amphure_id || qs.amphureId || 0) || 0;
    if (!aid) return send({ ok:false, error:'amphure_id required' }, 400, { 'Access-Control-Allow-Origin': '*' });
    const search = (qs.search || '').trim();
    let sql = `SELECT id, amphure_id AS amphureId, name_th AS nameTh, name_en AS nameEn, zipcode
               FROM th_tambons WHERE amphure_id = ?`;
    const binds = [aid];
    if (search) {
      sql += ` AND (LOWER(name_th) LIKE LOWER(?) OR LOWER(name_en) LIKE LOWER(?))`;
      const pat = `%${search}%`; binds.push(pat, pat);
    }
    sql += ` ORDER BY name_th`;
    const rows = await db.prepare(sql).bind(...binds).all();
    return send200(rows.results || rows);
  }

  if (path === 'geo/seed') {
    const info = await ensureGeoSeed(db, fetch, { force:true });
    return send({ ok:true, ...info }, 200, { 'Access-Control-Allow-Origin': '*' });
  }

  return send({ ok:false, error:'Not Found' }, 404, { 'Access-Control-Allow-Origin': '*' });
}
// ===== /GEO Auto-Create + Seed + Router =====

  // -------- meta endpoints --------
  if (path === '' || path === '/') {
    return json({ ok: true, hello: 'bizapp-api' });
  }
  if (path === 'tables' && method === 'GET') {
    const rows = await db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).all();
    return json(rows.results?.map(r => r.name) || []);
  }
  const tablesMatch = path.match(/^tables\/([^/]+)(?:\/(columns))?$/);
  if (tablesMatch && method === 'GET') {
    const t = tablesMatch[1];
    if (tablesMatch[2] === 'columns') {
      const cols = await db.prepare(`PRAGMA table_info(${ident(t)})`).all();
      return json(cols.results || []);
    } else {
      const data = await db.prepare(`SELECT * FROM ${ident(t)} ORDER BY id DESC`).all();
      return json(data.results || []);
    }
  }

  // -------- resolve table / id from path --------
  // patterns:
  //   <table>
  //   <table>/<id>
  //   <module>/<table>
  //   <module>/<table>/<id>
  const segs = path.split('/').filter(Boolean);
  let moduleName = null, tableBase = null, id = null;

  if (segs.length === 1) {
    tableBase = segs[0];
  } else if (segs.length === 2) {
    if (isNumeric(segs[1])) { tableBase = segs[0]; id = segs[1]; }
    else { moduleName = segs[0]; tableBase = segs[1]; }
  } else {
    moduleName = segs[0];
    tableBase = segs[1];
    if (isNumeric(segs[2])) id = segs[2];
  }

  // Compose physical table name: with module prefix if present
  // e.g. hr + employees -> hr_employees
  const table = moduleName ? `${moduleName}_${tableBase}` : tableBase;

  // small helper: check table exists
  const exists = await db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).bind(table).first();
  if (!exists) return json({ error: `Table not found: ${table}` }, 404);

  // ====== SPECIAL CASE (explicit) — hr_employees CRUD ======
  // (คุณใช้ endpoint นี้อยู่ ให้ทำงานแน่นอน)
  if (table === 'hr_employees') {
    if (method === 'GET' && !id) {
      const rs = await db.prepare(`SELECT * FROM hr_employees ORDER BY id DESC`).all();
      return json(rs.results || []);
    }
    if (method === 'GET' && id) {
      const row = await db.prepare(`SELECT * FROM hr_employees WHERE id=?`).bind(id).first();
      return row ? json(row) : json({ error: 'Not found' }, 404);
    }
    if (method === 'POST') {
      const b = await safeJson(request);
      const stmt = db.prepare(`
        INSERT INTO hr_employees
          (employeeId, firstName, lastName, nationalId, gender, phone, email,
           department, position, dob, salaryType, salaryValue, startDate, lastDate, status,
           CreateBy, UpdateBy)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, ?)
      `);
      const info = await stmt.bind(
        b.employeeId, b.firstName, b.lastName, b.nationalId, b.gender, b.phone, b.email,
        b.department, b.position, b.dob, b.salaryType, numOrNull(b.salaryValue),
        b.startDate, b.lastDate, b.status ?? 'Active',
        user, user
      ).run();
      const row = await db.prepare(`SELECT * FROM hr_employees WHERE id=?`).bind(info.meta.last_row_id).first();
      return json(row, 201);
    }
    if (method === 'PUT' && id) {
      const b = await safeJson(request);
      // Only update known columns
      const up = {
        employeeId: b.employeeId,
        firstName: b.firstName,
        lastName: b.lastName,
        nationalId: b.nationalId,
        gender: b.gender,
        phone: b.phone,
        email: b.email,
        department: b.department,
        position: b.position,
        dob: b.dob,
        salaryType: b.salaryType,
        salaryValue: numOrNull(b.salaryValue),
        startDate: b.startDate,
        lastDate: b.lastDate,
        status: b.status ?? 'Active',
        UpdateBy: user
      };
      const keys = Object.keys(up).filter(k => up[k] !== undefined);
      const setSql = keys.map(k => `${k}=?`).join(', ');
      const sql = `UPDATE hr_employees SET ${setSql}, UpdateDate=datetime('now') WHERE id=?`;
      const binds = keys.map(k => up[k]);
      binds.push(id);
      await db.prepare(sql).bind(...binds).run();
      const row = await db.prepare(`SELECT * FROM hr_employees WHERE id=?`).bind(id).first();
      return json(row || { ok: true });
    }
    if (method === 'DELETE' && id) {
      await db.prepare(`DELETE FROM hr_employees WHERE id=?`).bind(id).run();
      return json({ ok: true });
    }
  }

  // ====== GENERIC HANDLER (works for all other tables) ======
  const cols = await columnsOf(db, table);

  if (method === 'GET' && !id) {
    const rs = await db.prepare(`SELECT * FROM ${ident(table)} ORDER BY id DESC`).all();
    return json(rs.results || []);
  }
  if (method === 'GET' && id) {
    const row = await db.prepare(`SELECT * FROM ${ident(table)} WHERE id=?`).bind(id).first();
    return row ? json(row) : json({ error: 'Not found' }, 404);
  }
  if (method === 'POST') {
    const b = await safeJson(request);
    // map only known columns; add CreateBy/UpdateBy
    const payload = withAudit(b, user, cols);
    const keys = Object.keys(payload);
    const q = `INSERT INTO ${ident(table)} (${keys.map(ident).join(', ')})
               VALUES (${keys.map(_ => '?').join(', ')})`;
    const info = await db.prepare(q).bind(...keys.map(k => payload[k])).run();
    const row = await db.prepare(`SELECT * FROM ${ident(table)} WHERE id=?`).bind(info.meta.last_row_id).first();
    return json(row, 201);
  }
  if (method === 'PUT' && id) {
    const b = await safeJson(request);
    const payload = withAuditUpdate(b, user, cols);
    const keys = Object.keys(payload);
    if (keys.length === 0) return json({ error: 'No updatable fields' }, 400);
    const setSql = keys.map(k => `${ident(k)}=?`).join(', ');
    const sql = `UPDATE ${ident(table)} SET ${setSql}, UpdateDate=datetime('now') WHERE id=?`;
    const binds = keys.map(k => payload[k]); binds.push(id);
    await db.prepare(sql).bind(...binds).run();
    const row = await db.prepare(`SELECT * FROM ${ident(table)} WHERE id=?`).bind(id).first();
    return json(row || { ok: true });
  }
  if (method === 'DELETE' && id) {
    await db.prepare(`DELETE FROM ${ident(table)} WHERE id=?`).bind(id).run();
    return json({ ok: true });
  }

  return json({ error: 'Unsupported route or method' }, 404);
};

// ---------- helpers ----------
  const json = (data, status = 200) =>
    new Response(typeof data === 'string' ? data : JSON.stringify(data), {
      status,
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, X-User',
        'Access-Control-Allow-Credentials': 'true'
      }
    });

const isNumeric = (s) => /^\d+$/.test(String(s || ''));

const safeJson = async (req) => {
  try { return await req.json(); } catch { return {}; }
};

const numOrNull = (v) => (v === undefined || v === null || v === '' ? null : Number(v));

const ident = (name) => name.replace(/[^a-zA-Z0-9_]/g, ''); // very simple safer identifier

async function columnsOf(db, table) {
  const rs = await db.prepare(`PRAGMA table_info(${ident(table)})`).all();
  return (rs.results || []).map(r => r.name);
}

function withAudit(b, user, cols) {
  const reserved = new Set(['id', 'CreateDate', 'UpdateDate']);
  const out = {};
  for (const k of Object.keys(b || {})) {
    if (!cols.includes(k)) continue;
    if (reserved.has(k)) continue;
    out[k] = b[k];
  }
  if (cols.includes('CreateBy')) out.CreateBy = user;
  if (cols.includes('UpdateBy')) out.UpdateBy = user;
  return out;
}

function withAuditUpdate(b, user, cols) {
  const reserved = new Set(['id', 'CreateDate', 'UpdateDate']);
  const out = {};
  for (const k of Object.keys(b || {})) {
    if (!cols.includes(k)) continue;
    if (reserved.has(k)) continue;
    out[k] = b[k];
  }
  if (cols.includes('UpdateBy')) out.UpdateBy = user;
  return out;
}
