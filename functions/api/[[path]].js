/* ===== GEO (Single-table) – helpers only, ไม่แตะ router อื่น ===== */
const __geoJson = (data, status=200, headers={}) =>
  new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...headers }
  });

async function __geoEnsureSingle(db) {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS geo_admin (
      id        INTEGER PRIMARY KEY,
      parent_id INTEGER,
      level     TEXT NOT NULL CHECK(level IN ('province','amphure','tambon')),
      name      TEXT NOT NULL,
      zipcode   TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_geo_admin_level  ON geo_admin(level);
    CREATE INDEX IF NOT EXISTS idx_geo_admin_parent ON geo_admin(parent_id);
    CREATE INDEX IF NOT EXISTS idx_geo_admin_zip    ON geo_admin(zipcode);
  `);
}

// seed เล็กน้อยเพื่อให้ dropdown ใช้งานได้ทันที (ปลอดภัย: OR IGNORE)
async function __geoMaybeSeed(db){
  const c = await db.prepare(`SELECT COUNT(*) n FROM geo_admin`).first();
  if (c?.n > 0) return;
  await db.batch([
    db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(10,null,'province','กรุงเทพมหานคร',null),
    db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(11,null,'province','สมุทรปราการ',null),
    db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(12,null,'province','นนทบุรี',null),
    db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(1001,10,'amphure','พระนคร',null),
    db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(1012,10,'amphure','ห้วยขวาง',null),
    db.prepare(`INSERT OR IGNORE INTO geo_admin(id,parent_id,level,name,zipcode) VALUES(?,?,?,?,?)`).bind(101201,1012,'tambon','ห้วยขวาง','10310')
  ]);
}

// handler เฉพาะ /api/geo/*
async function __handleGeo(ctx, path, db){
  if (!path.startsWith('geo/')) return null; // ปล่อยให้ router เดิมทำงาน
  const cors = ctx.baseHeaders || { "content-type": "application/json; charset=utf-8" };

  await __geoEnsureSingle(db);
  await __geoMaybeSeed(db);

  // /api/geo/provinces
  if (path === 'geo/provinces') {
    const rs = await db.prepare(
      `SELECT id, name FROM geo_admin WHERE level='province' ORDER BY name`
    ).all();
    return (typeof json === 'function' ? json(rs.results || rs || []) : __geoJson(rs.results || rs || [], 200, cors));
  }

  // /api/geo/amphures?province_id=10
  if (path.startsWith('geo/amphures')) {
    const url = new URL(ctx.request.url);
    const pid = url.searchParams.get('province_id');
    if (!pid)
      return (typeof json === 'function'
        ? json({ error:'province_id required' }, 400)
        : __geoJson({ error:'province_id required' }, 400, cors));
    const rs = await db.prepare(
      `SELECT id, name, parent_id AS province_id
         FROM geo_admin
        WHERE level='amphure' AND parent_id=?
        ORDER BY name`
    ).bind(pid).all();
    return (typeof json === 'function' ? json(rs.results || rs || []) : __geoJson(rs.results || rs || [], 200, cors));
  }

  // /api/geo/tambons?amphure_id=...
  if (path.startsWith('geo/tambons')) {
    const url = new URL(ctx.request.url);
    const aid = url.searchParams.get('amphure_id');
    if (!aid)
      return (typeof json === 'function'
        ? json({ error:'amphure_id required' }, 400)
        : __geoJson({ error:'amphure_id required' }, 400, cors));
    const rs = await db.prepare(
      `SELECT id, name, zipcode, parent_id AS amphure_id
         FROM geo_admin
        WHERE level='tambon' AND parent_id=?
        ORDER BY name`
    ).bind(aid).all();
    return (typeof json === 'function' ? json(rs.results || rs || []) : __geoJson(rs.results || rs || [], 200, cors));
  }

  // สถานะรวม
  if (path === 'geo/status') {
    const p = await db.prepare(`SELECT COUNT(*) n FROM geo_admin WHERE level='province'`).first();
    const a = await db.prepare(`SELECT COUNT(*) n FROM geo_admin WHERE level='amphure'`).first();
    const t = await db.prepare(`SELECT COUNT(*) n FROM geo_admin WHERE level='tambon'`).first();
    const payload = { provinces:p?.n||0, amphures:a?.n||0, tambons:t?.n||0 };
    return (typeof json === 'function' ? json(payload) : __geoJson(payload, 200, cors));
  }

  return (typeof json === 'function' ? json({ error:'Not Found' }, 404) : __geoJson({ error:'Not Found' }, 404, cors));
}
/* ===== /GEO helpers ===== */
export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const db = env.DB;                      // D1 binding
  const path = url.pathname.replace(/^\/api\/?/, '').trim();

  // ===== CORS / helpers =====
  const origin = request.headers.get("Origin") || "*";
  const baseHeaders = {
    "content-type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-User",
  };
  const send = (data, status=200) => new Response(JSON.stringify(data), { status, headers: baseHeaders });
  const err  = (msg, status=400)   => send({ error: msg }, status);

  // ตอบ preflight ก่อนเสมอ
  if (method === "OPTIONS") return new Response(null, { status: 204, headers: baseHeaders });
  if (!url.pathname.startsWith("/api")) return err("Not found", 404);

  // ===== GEO first (ใช้ตารางเดียว) =====
  {
    const r = await __handleGeo({ request, baseHeaders }, path, db);
    if (r) return r; // จบที่นี่ถ้าเป็น /api/geo/*
  }

  // ===== Common helpers ที่ส่วนอื่นต้องใช้ =====
  const seg = path.split('/').filter(Boolean);           // ["hr","employees",":id"]
  const idFromPath = seg.length >= 3 ? decodeURIComponent(seg[2]) : null;
  const [domain, resource, id] = [seg[0] || "", seg[1] || "", seg[2] || null];

  const q = (name, def = "") => (url.searchParams.get(name) ?? def).trim();

  const readBody = async () => {
    const ct = (request.headers.get("content-type") || "").toLowerCase();
    if (ct.includes("application/json")) { try { return await request.json(); } catch { return {}; } }
    if (ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data")) {
      const fd = await request.formData(); return Object.fromEntries(fd.entries());
    }
    const raw = await request.text(); try { return JSON.parse(raw); } catch { return {}; }
  };

  const safeIdent = (name) => { if (!/^[A-Za-z0-9_]+$/.test(name)) throw new Error(`Invalid identifier: ${name}`); return name; };
  const getColumns = async (table) => (await db.prepare(`PRAGMA table_info(${table})`).all()).results || [];
  const hasColumn = async (table, col) => (await getColumns(table)).some(c => c.name === col);
  const getPK = async (table) => {
    const cols = await getColumns(table);
    const pk = cols.find(c=>c.pk===1)?.name; if (pk) return pk;
    if (cols.some(c=>c.name.toLowerCase()==="id")) return "id";
    return "rowid";
  };
  const user = request.headers.get("x-user") || "system";
  const addAuditOnCreate = async (table, obj) => {
    const o = { ...obj };
    if (await hasColumn(table,"CreateDate")) o.CreateDate = o.CreateDate ?? {__raw:"datetime('now')"};
    if (await hasColumn(table,"UpdateDate")) o.UpdateDate = o.UpdateDate ?? {__raw:"datetime('now')"};
    if (await hasColumn(table,"CreateBy"))   o.CreateBy   = o.CreateBy   ?? user;
    if (await hasColumn(table,"UpdateBy"))   o.UpdateBy   = o.UpdateBy   ?? user;
    return o;
  };
  const addAuditOnUpdate = async (table, obj) => {
    const o = { ...obj };
    if (await hasColumn(table,"UpdateDate")) o.UpdateDate = {__raw:"datetime('now')"};
    if (await hasColumn(table,"UpdateBy"))   o.UpdateBy   = user;
    return o;
  };
  const buildInsert = (table, dataObj) => {
    const keys = Object.keys(dataObj); if (!keys.length) throw new Error("Empty object");
    const cols = keys.map(k=>`"${safeIdent(k)}"`).join(", ");
    const vals = keys.map(k=> dataObj[k]?.__raw ? dataObj[k].__raw : "?").join(", ");
    const bind = keys.filter(k=>!dataObj[k]?.__raw).map(k=>dataObj[k]);
    return { sql: `INSERT INTO ${table} (${cols}) VALUES (${vals}) RETURNING *`, bind };
  };
  const buildUpdate = (table, dataObj, idField) => {
    const keys = Object.keys(dataObj); if (!keys.length) throw new Error("Empty object");
    const sets = keys.map(k=> dataObj[k]?.__raw ? `"${safeIdent(k)}"=${dataObj[k].__raw}` : `"${safeIdent(k)}"=?`).join(", ");
    const bind = keys.filter(k=>!dataObj[k]?.__raw).map(k=>dataObj[k]);
    return { sql: `UPDATE ${table} SET ${sets} WHERE ${idField}=? RETURNING *`, bind };
  };

  // ===== HR: Employees =====
  if (seg[0] === "hr" && seg[1] === "employees") {
    const table="hr_employees", pk=await getPK(table);
    if (method==="GET" && !idFromPath) {
      const rs = await db.prepare(`SELECT * FROM ${table} ORDER BY ${pk} DESC`).all();
      return send(rs.results || []);
    }
    if (method==="POST") {
      const body=await addAuditOnCreate(table, await readBody());
      const {sql,bind}=buildInsert(table,body);
      const row=await db.prepare(sql).bind(...bind).first();
      return send(row);
    }
    if ((method==="PUT"||method==="PATCH") && idFromPath) {
      const body=await addAuditOnUpdate(table, await readBody());
      const {sql,bind}=buildUpdate(table,body,pk);
      const row=await db.prepare(sql).bind(...bind,idFromPath).first();
      return row?send(row):err("not found",404);
    }
    if (method==="DELETE" && idFromPath) {
      const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(idFromPath).first();
      return row?send(row):err("not found",404);
    }
    return err("method not allowed",405);
  }

  // ===== HR: Attendance =====
  if (seg[0] === "hr" && seg[1] === "attendance") {
    const table="hr_attendance", pk=await getPK(table);
    if (method==="GET" && !idFromPath) {
      const p = {
        search: q("search"), empId: q("empId"), position: q("position"),
        leaveType: q("leaveType"), status: q("status"),
        from: q("from"), to: q("to"),
        limit: Math.min(+q("limit","20")||20, 1000),
        offset: Math.max(+q("offset","0")||0, 0),
      };
      const bind=[]; let sql=`SELECT * FROM ${table}`; const wh=[];
      if (p.search)   { wh.push(`(LOWER(fullName) LIKE ? OR empId LIKE ?)`); bind.push(`%${p.search.toLowerCase()}%`, `%${p.search}%`); }
      if (p.empId)    { wh.push(`empId = ?`); bind.push(p.empId); }
      if (p.position) { wh.push(`position = ?`); bind.push(p.position); }
      if (p.leaveType){ wh.push(`leaveType = ?`); bind.push(p.leaveType); }
      if (p.status)   { wh.push(`status = ?`); bind.push(p.status); }
      if (p.from)     { wh.push(`date >= ?`); bind.push(p.from); }
      if (p.to)       { wh.push(`date <= ?`); bind.push(p.to); }
      if (wh.length) sql += ` WHERE ` + wh.join(" AND ");
      const total = (await db.prepare(`SELECT COUNT(*) c FROM (${sql})`).bind(...bind).first())?.c || 0;
      sql += ` ORDER BY date DESC, ${pk} DESC LIMIT ${p.limit} OFFSET ${p.offset}`;
      const rs = await db.prepare(sql).bind(...bind).all();
      return send({ data: rs.results || [], total });
    }
    if (method==="POST") {
      const body=await addAuditOnCreate(table, await readBody());
      const {sql,bind}=buildInsert(table,body);
      const row=await db.prepare(sql).bind(...bind).first();
      return send(row);
    }
    if ((method==="PUT"||method==="PATCH") && idFromPath) {
      const body=await addAuditOnUpdate(table, await readBody());
      const {sql,bind}=buildUpdate(table,body,pk);
      const row=await db.prepare(sql).bind(...bind,idFromPath).first();
      return row?send(row):err("not found",404);
    }
    if (method==="DELETE" && idFromPath) {
      const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(idFromPath).first();
      return row?send(row):err("not found",404);
    }
    return err("method not allowed",405);
  }

  // ===== HR: Timeclock (บล็อคเดิมของคุณ ใช้ domain/resource/id ที่นิยามไว้แล้ว) =====
  if (domain === 'hr' && resource === 'timeclock') {
    const table = 'hr_timeclock';
    const pk = 'id';
    const idFromPath_tc = id;

    const json = (data, status = 200) =>
      new Response(JSON.stringify(data), {
        status,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': origin }
      });

    const toLocalYMD = (d = new Date(), offsetMin = 7 * 60) => {
      const t = new Date(d.getTime() + offsetMin * 60000);
      return t.toISOString().slice(0, 10);
    };

    // POST /api/hr/timeclock  (Clock In)
    if (method === 'POST') {
      try {
        const b = await request.json().catch(() => ({}));
        const empId = (b.empId || '').trim();
        if (!empId) return json({ ok: false, error: 'empId required' }, 400);

        const nowISO = new Date().toISOString();
        const ymd = (b.date && String(b.date).trim()) || toLocalYMD();

        const open = await db
          .prepare(
            `SELECT ${pk} AS id FROM ${table}
             WHERE empId=? AND date=? AND (outAt IS NULL OR outAt='' OR outAt='null')
             ORDER BY ${pk} DESC LIMIT 1`
          )
          .bind(empId, ymd)
          .first();
        if (open) return json({ ok: false, reason: 'already_open', id: open.id }, 409);

        await db
          .prepare(
            `INSERT INTO ${table}
               (empId, date, inAt, outAt, hours, note, geo, CreateDate)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .bind(empId, ymd, nowISO, null, 0, b.note || '', b.geo || '', nowISO)
          .run();

        const last = await db.prepare(`SELECT last_insert_rowid() AS lid`).first();
        const row = await db
          .prepare(`SELECT * FROM ${table} WHERE ${pk}=?`)
          .bind(last.lid)
          .first();

        return json({ ok: true, row });
      } catch (e) {
        console.error('[timeclock POST]', e);
        return json({ ok: false, error: String(e) }, 500);
      }
    }

    // PUT/PATCH /api/hr/timeclock/:id  (Clock Out / Update)
    if ((method === 'PUT' || method === 'PATCH') && idFromPath_tc) {
      try {
        const b = await request.json().catch(() => ({}));
        const cur = await db
          .prepare(`SELECT * FROM ${table} WHERE ${pk}=?`)
          .bind(idFromPath_tc)
          .first();
        if (!cur) return json({ ok: false, error: 'not found' }, 404);

        let outAtISO = b.outAt ? new Date(b.outAt).toISOString() : null;
        if (!outAtISO && (b.clockOutNow === true || b.clockOutNow === 'true' || b.clockOutNow === '1')) {
          outAtISO = new Date().toISOString();
        }

        let hoursVal = cur.hours || 0;
        const inAtISO = cur.inAt || b.inAt || null;
        if (inAtISO && outAtISO) {
          const a = new Date(inAtISO).getTime();
          const c = new Date(outAtISO).getTime();
          if (Number.isFinite(a) && Number.isFinite(c) && c >= a) {
            hoursVal = +(((c - a) / (1000 * 60 * 60)).toFixed(2));
          }
        }

        const dateStr =
          (cur.date && String(cur.date).trim()) ||
          (b.date && String(b.date).trim()) ||
          toLocalYMD();

        await db
          .prepare(
            `UPDATE ${table}
                SET empId = COALESCE(?, empId),
                    date  = COALESCE(?, date),
                    inAt  = COALESCE(?, inAt),
                    outAt = COALESCE(?, outAt),
                    hours = COALESCE(?, hours),
                    note  = COALESCE(?, note),
                    geo   = COALESCE(?, geo)
              WHERE ${pk}=?`
          )
          .bind(
            b.empId ?? null,
            dateStr ?? null,
            b.inAt ?? null,
            outAtISO ?? null,
            hoursVal ?? null,
            b.note ?? null,
            b.geo ?? null,
            idFromPath_tc
          )
          .run();

        const row = await db
          .prepare(`SELECT * FROM ${table} WHERE ${pk}=?`)
          .bind(idFromPath_tc)
          .first();

        return json({ ok: true, row });
      } catch (e) {
        console.error('[timeclock PUT]', e);
        return json({ ok: false, error: String(e) }, 500);
      }
    }

    // GET /api/hr/timeclock?empId=..&from=YYYY-MM-DD&to=YYYY-MM-DD
    if (method === 'GET') {
      try {
        const sp = new URLSearchParams(url.search);
        const empId = (sp.get('empId') || '').trim();
        const from = (sp.get('from') || '').trim();
        const to = (sp.get('to') || '').trim();

        let sql = `SELECT * FROM ${table} WHERE 1=1`;
        const p = [];
        if (empId) { sql += ` AND empId=?`; p.push(empId); }
        if (from)  { sql += ` AND date>=?`; p.push(from); }
        if (to)    { sql += ` AND date<=?`; p.push(to); }
        sql += ` ORDER BY ${pk} DESC LIMIT 500`;

        const { results } = await db.prepare(sql).bind(...p).all();
        return json({ ok: true, data: results });
      } catch (e) {
        console.error('[timeclock GET]', e);
        return json({ ok: false, error: String(e) }, 500);
      }
    }

    // DELETE /api/hr/timeclock/:id
    if (method === 'DELETE' && idFromPath_tc) {
      try {
        await db.prepare(`DELETE FROM ${table} WHERE ${pk}=?`).bind(idFromPath_tc).run();
        return json({ ok: true });
      } catch (e) {
        console.error('[timeclock DELETE]', e);
        return json({ ok: false, error: String(e) }, 500);
      }
    }

    return json({ ok: false, error: 'Method not allowed' }, 405);
  }

  // ===== Sales: Customers =====
  if (seg[0] === "sales" && seg[1] === "customers") {
    const table="sales_customers", pk=await getPK(table);
    if (method==="GET" && !idFromPath) {
      const search=q("search"); let sql=`SELECT * FROM ${table}`, bind=[];
      if (search) { sql+=` WHERE (LOWER(code) LIKE ? OR LOWER(firstName) LIKE ? OR LOWER(lastName) LIKE ? OR nationalId LIKE ?)`; bind=[...Array(3).fill(`%${search.toLowerCase()}%`), `%${search}%`]; }
      sql+=` ORDER BY ${pk} DESC LIMIT ${Math.min(+q("limit","100")||100,1000)} OFFSET ${Math.max(+q("offset","0")||0,0)}`;
      const rs=await db.prepare(sql).bind(...bind).all(); return send(rs.results||[]);
    }
    if (method==="POST") { const body=await addAuditOnCreate(table, await readBody()); const {sql,bind}=buildInsert(table,body); const row=await db.prepare(sql).bind(...bind).first(); return send(row); }
    if ((method==="PUT"||method==="PATCH") && idFromPath) { const body=await addAuditOnUpdate(table, await readBody()); const {sql,bind}=buildUpdate(table,body,pk); const row=await db.prepare(sql).bind(...bind,idFromPath).first(); return row?send(row):err("not found",404); }
    if (method==="DELETE" && idFromPath) { const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(idFromPath).first(); return row?send(row):err("not found",404); }
    return err("method not allowed",405);
  }

  // ===== Sales: Quotations =====
  if (seg[0] === "sales" && seg[1] === "quotations") {
    const T_HEAD  = "sales_quotations";
    const T_ITEMS = "sales_quotationitems";
    const headPK  = await getPK(T_HEAD);

    const parseCustomer = (s) => { try { return s && typeof s === "string" ? JSON.parse(s) : (s || {}); } catch { return {}; } };
    const parseItems    = (s) => { try { return s && typeof s === "string" ? JSON.parse(s) : (Array.isArray(s) ? s : []); } catch { return []; } };

    async function genQNoSafe(dateStr){
      const d = dateStr ? new Date(dateStr) : new Date();
      const y = d.getFullYear();
      const m = String(d.getMonth()+1).padStart(2,'0');
      const day = String(d.getDate()).padStart(2,'0');
      const prefix = `Q${y}${m}${day}-`;
      const row = await db.prepare(`
        SELECT MAX(CAST(SUBSTR(qNo, LENGTH(?) + 1) AS INTEGER)) AS maxrun
        FROM ${T_HEAD}
        WHERE qNo LIKE ? || '%'
      `).bind(prefix, prefix).first();
      const next = Number(row?.maxrun || 0) + 1;
      return `${prefix}${String(next).padStart(3,'0')}`;
    }
    async function qNoExists(qno){
      if(!qno) return false;
      const r = await db.prepare(`SELECT 1 FROM ${T_HEAD} WHERE qNo=? LIMIT 1`).bind(qno).first();
      return !!r;
    }

    if (method === "GET" && !idFromPath) {
      const rs = await db.prepare(`SELECT * FROM ${T_HEAD} ORDER BY ${headPK} DESC`).all();
      return send(rs.results || []);
    }

    if (method === "GET" && idFromPath) {
      const head = await db.prepare(`SELECT * FROM ${T_HEAD} WHERE ${headPK}=?`).bind(idFromPath).first();
      if (!head) return err("not found", 404);

      const itemsRow = await db.prepare(`
        SELECT qNo, itemCode, itemName, qty, unitPrice, lineTotal
        FROM ${T_ITEMS}
        WHERE qNo = ?
        ORDER BY id ASC
      `).bind(head.qNo).all();

      const items = (itemsRow.results || []).map(r => {
        const qty   = Number(r.qty || 0);
        const price = Number(r.unitPrice || 0);
        const line  = Number(r.lineTotal || 0);
        const discount = Math.max(0, (qty * price) - line);
        return { service: r.itemCode || "", tooth: r.itemName || "", qty, price, discount: +discount.toFixed(2) };
      });

      const customer = {
        code       : head.customerCode || "",
        firstName  : head.customerFirstName || "",
        lastName   : head.customerLastName || "",
        nationalId : head.customerNationalId || "",
        age        : Number(head.customerAge || 0),
      };

      return send({ ...head, customer, items });
    }

    if (method === "POST") {
      const body  = await readBody();
      const cust  = parseCustomer(body.customer);
      const items = parseItems(body.items);

      let qNo = (body.qNo || "").trim();
      if (!qNo || await qNoExists(qNo)) qNo = await genQNoSafe(body.qDate || null);

      const totalBefore = items.reduce((s, it) => s + (Number(it.qty || 0) * Number(it.price || 0)), 0);
      const discount    = items.reduce((s, it) => s + Number(it.discount || 0), 0);
      const grandTotal  = +(totalBefore - discount).toFixed(2);

      const headObj = await addAuditOnCreate(T_HEAD, {
        qNo, qDate: body.qDate || null, status: body.confirmed ? "Confirmed" : "Draft",
        customerCode: cust.code || "", customerFirstName: cust.firstName || "", customerLastName: cust.lastName || "",
        customerNationalId: cust.nationalId || "", customerAge: Number(cust.age || 0),
        totalBeforeDiscount: totalBefore, discount, grandTotal, note: body.note || ""
      });
      const { sql: sqlH, bind: bindH } = buildInsert(T_HEAD, headObj);
      const head = await db.prepare(sqlH).bind(...bindH).first();

      if (items.length) {
        const ins = db.prepare(`
          INSERT INTO ${T_ITEMS}
            (qNo, itemCode, itemName, qty, unitPrice, lineTotal, CreateDate)
          VALUES
            (?,   ?,        ?,        ?,   ?,         ?,         datetime('now'))
        `);
        for (const it of items) {
          const qty  = Number(it.qty || 0);
          const price= Number(it.price || 0);
          const disc = Number(it.discount || 0);
          const line = Math.max(0, qty * price - disc);
          await ins.bind(head.qNo, it.service || "", it.tooth || "", qty, price, line).run();
        }
      }
      return send(head);
    }

    if ((method === "PUT" || method === "PATCH") && idFromPath) {
      const body  = await readBody();
      const cust  = parseCustomer(body.customer);
      const items = parseItems(body.items);

      const totalBefore = items.reduce((s, it) => s + (Number(it.qty || 0) * Number(it.price || 0)), 0);
      const discount    = items.reduce((s, it) => s + Number(it.discount || 0), 0);
      const grandTotal  = +(totalBefore - discount).toFixed(2);

      const updObj = await addAuditOnUpdate(T_HEAD, {
        qNo: body.qNo || null, qDate: body.qDate || null, status: body.confirmed ? "Confirmed" : "Draft",
        customerCode: cust.code || "", customerFirstName: cust.firstName || "", customerLastName: cust.lastName || "",
        customerNationalId: cust.nationalId || "", customerAge: Number(cust.age || 0),
        totalBeforeDiscount: totalBefore, discount, grandTotal, note: body.note || ""
      });
      const { sql, bind } = buildUpdate(T_HEAD, updObj, headPK);
      const head = await db.prepare(sql).bind(...bind, idFromPath).first();
      if (!head) return err("not found", 404);

      await db.prepare(`DELETE FROM ${T_ITEMS} WHERE qNo=?`).bind(head.qNo).run();
      if (items.length) {
        const ins = db.prepare(`
          INSERT INTO ${T_ITEMS}
            (qNo, itemCode, itemName, qty, unitPrice, lineTotal, CreateDate)
          VALUES
            (?,   ?,        ?,        ?,   ?,         ?,         datetime('now'))
        `);
        for (const it of items) {
          const qty  = Number(it.qty || 0);
          const price= Number(it.price || 0);
          const disc = Number(it.discount || 0);
          const line = Math.max(0, qty * price - disc);
          await ins.bind(head.qNo, it.service || "", it.tooth || "", qty, price, line).run();
        }
      }
      return send(head);
    }

    if (method === "DELETE" && idFromPath) {
      const head = await db.prepare(`DELETE FROM ${T_HEAD} WHERE ${headPK}=? RETURNING *`).bind(idFromPath).first();
      if (head) await db.prepare(`DELETE FROM ${T_ITEMS} WHERE qNo=?`).bind(head.qNo).run();
      return head ? send(head) : err("not found", 404);
    }

    return err("method not allowed", 405);
  }

  // ===== Sales: Orders =====
  if (seg[0]==="sales" && seg[1]==="orders") {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS sales_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        docNo TEXT, date TEXT,
        customerId TEXT, customerName TEXT,
        status TEXT, total REAL, note TEXT, refNo TEXT,
        payload TEXT,
        CreateDate TEXT DEFAULT (datetime('now')),
        CreateBy   TEXT,
        UpdateDate TEXT DEFAULT (datetime('now')),
        UpdateBy   TEXT
      );
    `);
    const table="sales_orders", pk=await getPK(table);

    if (method==="GET" && !idFromPath) {
      const search=q("search"), from=q("from"), to=q("to"),
            limit=Math.min(+q("limit","50")||50,1000), offset=Math.max(+q("offset","0")||0,0);
      const bind=[]; let sql=`SELECT * FROM ${table}`; const wh=[];
      if (search) { wh.push(`(LOWER(docNo) LIKE ? OR LOWER(customerName) LIKE ? OR customerId LIKE ?)`); bind.push(`%${search.toLowerCase()}%`,`%${search.toLowerCase()}%`, `%${search}%`); }
      if (from)   { wh.push(`date>=?`); bind.push(from); }
      if (to)     { wh.push(`date<=?`); bind.push(to); }
      if (wh.length) sql+=` WHERE `+wh.join(" AND ");
      const total=(await db.prepare(`SELECT COUNT(*) c FROM (${sql})`).bind(...bind).first())?.c||0;
      sql+=` ORDER BY date DESC, ${pk} DESC LIMIT ${limit} OFFSET ${offset}`;
      const rs=await db.prepare(sql).bind(...bind).all();
      return send({ data: rs.results||[], total });
    }
    if (method==="POST") {
      const b = await readBody();
      const payload = await addAuditOnCreate(table, {
        docNo: b.docNo||null, date: b.date||null,
        customerId: b.customerId||null, customerName: b.customerName||null,
        status: b.status||"Open", total: Number(b.total||0),
        note: b.note||"", refNo: b.refNo||"",
        payload: typeof b.payload==="string" ? b.payload : JSON.stringify(b.payload ?? {})
      });
      const {sql,bind}=buildInsert(table,payload);
      const row=await db.prepare(sql).bind(...bind).first();
      return send(row);
    }
    if ((method==="PUT"||method==="PATCH") && idFromPath) {
      const b = await readBody();
      const payload = await addAuditOnUpdate(table, {
        docNo: b.docNo||null, date: b.date||null,
        customerId: b.customerId||null, customerName: b.customerName||null,
        status: b.status||"Open", total: Number(b.total||0),
        note: b.note||"", refNo: b.refNo||"",
        payload: typeof b.payload==="string" ? b.payload : JSON.stringify(b.payload ?? {})
      });
      const {sql,bind}=buildUpdate(table,payload,pk);
      const row=await db.prepare(sql).bind(...bind, idFromPath).first();
      return row?send(row):err("not found",404);
    }
    if (method==="DELETE" && idFromPath) {
      const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(idFromPath).first();
      return row?send(row):err("not found",404);
    }
    return err("method not allowed",405);
  }

  // ----- fallback -----
  return err(`No route for: ${seg.join("/")}`, 404);
};
