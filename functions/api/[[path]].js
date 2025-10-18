// functions/api/[[path]].js
export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const db = env.DB;                      // D1 binding: DB
  const user = request.headers.get("x-user") || "system";

  // ----- helpers -----
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

  if (method === "OPTIONS") return new Response(null, { status: 204, headers: baseHeaders });
  if (!url.pathname.startsWith("/api")) return err("Not found", 404);

  const seg = url.pathname.replace(/^\/api\/?/, "").split("/").filter(Boolean); // ["hr","employees","<id>"]
  const idFromPath = seg.length >= 3 ? decodeURIComponent(seg[2]) : null;

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

  // ----- health -----
  if (seg.length === 0) return send({ service:"bizapp-api", time: new Date().toISOString() });

  // ===== GEO (หน้าใช้ province_id & amphure_id) =====
  if (seg[0] === "geo") {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS geo_provinces (id INTEGER PRIMARY KEY, name_th TEXT, name_en TEXT);
      CREATE TABLE IF NOT EXISTS geo_amphures (id INTEGER PRIMARY KEY, province_id INTEGER, name_th TEXT, name_en TEXT);
      CREATE TABLE IF NOT EXISTS geo_tambons  (id INTEGER PRIMARY KEY, amphure_id  INTEGER, zip_code TEXT, name_th TEXT, name_en TEXT);
    `);
    if (seg[1] === "provinces" && method === "GET") {
      const rs = await db.prepare(`SELECT id,name_th,name_en FROM geo_provinces ORDER BY name_th`).all();
      return send(rs.results || []);
    }
    if (seg[1] === "amphures" && method === "GET") {
      const pid = q("province_id");
      const st = pid
        ? db.prepare(`SELECT id,province_id,name_th,name_en FROM geo_amphures WHERE province_id=? ORDER BY name_th`).bind(+pid)
        : db.prepare(`SELECT id,province_id,name_th,name_en FROM geo_amphures ORDER BY name_th`);
      const rs = await st.all(); return send(rs.results || []);
    }
    if (seg[1] === "tambons" && method === "GET") {
      const aid = q("amphure_id");
      const st = aid
        ? db.prepare(`SELECT id,amphure_id,zip_code,name_th,name_en FROM geo_tambons WHERE amphure_id=? ORDER BY name_th`).bind(+aid)
        : db.prepare(`SELECT id,amphure_id,zip_code,name_th,name_en FROM geo_tambons ORDER BY name_th`);
      const rs = await st.all(); return send(rs.results || []);
    }
    if (seg[1] === "seed" && (method === "POST" || method === "GET")) {
      const c = await db.prepare(`SELECT COUNT(*) c FROM geo_provinces`).first();
      if (c?.c > 0) return send({ seeded: false, reason: "already" });
      const base="https://raw.githubusercontent.com/kongvut/thai-province-data/master";
      const [p,a,t] = await Promise.all([fetch(`${base}/api_province.json`), fetch(`${base}/api_amphure.json`), fetch(`${base}/api_tambon.json`)]);
      if (!p.ok || !a.ok || !t.ok) return err("seed fetch failed", 500);
      const [P,A,T] = [await p.json(), await a.json(), await t.json()];
      const insP = db.prepare(`INSERT INTO geo_provinces(id,name_th,name_en) VALUES (?,?,?)`);
      for (const r of P) await insP.bind(r.id,r.name_th,r.name_en).run();
      const insA = db.prepare(`INSERT INTO geo_amphures(id,province_id,name_th,name_en) VALUES (?,?,?,?)`);
      for (const r of A) await insA.bind(r.id,r.province_id,r.name_th,r.name_en).run();
      const insT = db.prepare(`INSERT INTO geo_tambons(id,amphure_id,zip_code,name_th,name_en) VALUES (?,?,?,?,?)`);
      for (const r of T) await insT.bind(r.id,r.amphure_id,String(r.zip_code||""),r.name_th,r.name_en).run();
      return send({ seeded:true, provinces:P.length, amphures:A.length, tambons:T.length });
    }
    return err("geo not found", 404);
  }

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

  // ===== HR: Attendance (filters ตรงหน้า) =====
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

// ===== HR: Timeclock =====
if (domain === 'hr' && resource === 'timeclock') {
  const table = 'hr_timeclock';
  const pk = 'id';
  const idFromPath = id; // มาจาก path resolver ของคุณ

  // helper: คืน YYYY-MM-DD ตามเวลาไทย (+07:00)
  const toLocalYMD = (d = new Date(), offsetMinutes = 7 * 60) => {
    const t = new Date(d.getTime() + offsetMinutes * 60000);
    return t.toISOString().slice(0, 10);
  };

  if (method === 'POST') {
    // ⏱ Clock In
    const b = await request.json().catch(() => ({}));

    const now = new Date();
    const isoNow = now.toISOString();
    const ymd = (b.date && String(b.date).trim()) || toLocalYMD();

    const empId = (b.empId || '').trim();
    if (!empId) {
      return new Response(JSON.stringify({ error: 'empId required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // กันรายการค้าง (ยังไม่ out) ของวันเดียวกัน–พนักงานเดียวกัน
    const open = await db
      .prepare(
        `SELECT ${pk} as id, inAt
           FROM ${table}
          WHERE empId = ? AND date = ?
            AND (outAt IS NULL OR outAt = '' OR outAt = 'null')
          ORDER BY ${pk} DESC
          LIMIT 1`
      )
      .bind(empId, ymd)
      .first();

    if (open) {
      return new Response(JSON.stringify({
        ok: false,
        reason: 'already_open',
        id: open.id,
        inAt: open.inAt
      }), { status: 409, headers: { 'Content-Type': 'application/json' } });
    }

    // Insert แถว Clock In — บังคับ date + inAt เสมอ
    const inserted = await db
      .prepare(
        `INSERT INTO ${table}
           (empId, date, inAt, outAt, hours, note, geo, CreateDate)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING *;`
      )
      .bind(
        empId,
        ymd,
        isoNow,
        null,                // outAt
        0,                   // hours
        b.note || '',
        b.geo || '',
        isoNow               // CreateDate
      )
      .first();

    return new Response(JSON.stringify({ ok: true, row: inserted }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  if ((method === 'PUT' || method === 'PATCH') && idFromPath) {
    // ⏹ Clock Out (หรืออัปเดต record)
    const b = await request.json().catch(() => ({}));

    const current = await db
      .prepare(`SELECT * FROM ${table} WHERE ${pk} = ?`)
      .bind(idFromPath)
      .first();

    if (!current) {
      return new Response(JSON.stringify({ error: 'not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // แปลง outAt → ISO ถ้าส่งมา หรือถ้าขอ clockOutNow
    let outAtISO = b.outAt ? new Date(b.outAt).toISOString() : null;
    const flag = b.clockOutNow;
    if (!outAtISO && (flag === true || flag === 'true' || flag === '1')) {
      outAtISO = new Date().toISOString();
    }

    // ให้มี date เสมอ
    const dateStr =
      (current.date && String(current.date).trim()) ||
      (b.date && String(b.date).trim()) ||
      toLocalYMD();

    // คำนวณชั่วโมงเมื่อมี inAt + outAt
    let hoursVal = current.hours || 0;
    const inAtISO = current.inAt || b.inAt;
    if (inAtISO && outAtISO) {
      const a = new Date(inAtISO).getTime();
      const c = new Date(outAtISO).getTime();
      if (Number.isFinite(a) && Number.isFinite(c) && c >= a) {
        hoursVal = +(((c - a) / (1000 * 60 * 60)).toFixed(2));
      }
    }

    // อัปเดตด้วย COALESCE — ถ้าไม่ได้ส่งค่ามา ให้คงค่าเดิม
    const updated = await db
      .prepare(
        `UPDATE ${table}
            SET empId     = COALESCE(?, empId),
                date      = COALESCE(?, date),
                inAt      = COALESCE(?, inAt),
                outAt     = COALESCE(?, outAt),
                hours     = COALESCE(?, hours),
                note      = COALESCE(?, note),
                geo       = COALESCE(?, geo)
          WHERE ${pk} = ?
          RETURNING *;`
      )
      .bind(
        b.empId ?? null,
        dateStr ?? null,
        b.inAt ?? null,
        outAtISO ?? null,
        hoursVal ?? null,
        b.note ?? null,
        b.geo ?? null,
        idFromPath
      )
      .first();

    return new Response(JSON.stringify({ ok: true, row: updated }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // (ถ้าต้องการ) GET รายการ (เช่น ?empId=E001&from=2025-10-01&to=2025-10-31)
  if (method === 'GET') {
    const q = new URLSearchParams(url.search);
    const empId = (q.get('empId') || '').trim();
    const from = (q.get('from') || '').trim();
    const to   = (q.get('to')   || '').trim();

    let sql = `SELECT * FROM ${table} WHERE 1=1`;
    const params = [];

    if (empId) { sql += ` AND empId = ?`; params.push(empId); }
    if (from)  { sql += ` AND date >= ?`; params.push(from); }
    if (to)    { sql += ` AND date <= ?`; params.push(to); }

    sql += ` ORDER BY ${pk} DESC LIMIT 500`;

    const { results } = await db.prepare(sql).bind(...params).all();
    return new Response(JSON.stringify({ ok: true, data: results }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // (ถ้าต้องการ) ลบ record
  if (method === 'DELETE' && idFromPath) {
    await db.prepare(`DELETE FROM ${table} WHERE ${pk} = ?`).bind(idFromPath).run();
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // เมธอดไม่รองรับ
  return new Response(JSON.stringify({ error: 'Method not allowed' }), {
    status: 405,
    headers: { 'Content-Type': 'application/json' }
  });
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

  // ===== Sales: Quotations (หน้าโพสต์ customer/items เป็น JSON string) =====
  if (seg[0] === "sales" && seg[1] === "quotations") {
    const T_HEAD="sales_quotations", T_ITEMS="sales_quotationitems";
    const headPK=await getPK(T_HEAD);

    const parseCustomer = (s) => { try{ return s && typeof s==="string" ? JSON.parse(s) : (s||{}); }catch{ return {}; } };
    const parseItems    = (s) => { try{ return s && typeof s==="string" ? JSON.parse(s) : (Array.isArray(s)?s:[]); }catch{ return []; } };

    if (method==="GET" && !idFromPath) {
      const rs=await db.prepare(`SELECT * FROM ${T_HEAD} ORDER BY ${headPK} DESC`).all();
      return send(rs.results||[]);
    }

    if (method==="POST") {
      const body = await readBody();
      const cust = parseCustomer(body.customer);
      const items= parseItems(body.items);
      const totalBefore = items.reduce((s,it)=> s + (Number(it.qty||0)*Number(it.price||0)), 0);
      const discount    = items.reduce((s,it)=> s + Number(it.discount||0), 0);
      const grandTotal  = +(totalBefore - discount).toFixed(2);

      const headObj = await addAuditOnCreate(T_HEAD, {
        qNo: body.qNo || null,
        qDate: body.qDate || null,
        status: body.confirmed ? "Confirmed" : "Draft",
        customerCode: cust.code || "",
        customerFirstName: cust.firstName || "",
        customerLastName: cust.lastName || "",
        customerNationalId: cust.nationalId || "",
        customerAge: Number(cust.age || 0),
        totalBeforeDiscount: totalBefore,
        discount, grandTotal,
        note: body.note || ""
      });
      const {sql:sqlH, bind:bindH} = buildInsert(T_HEAD, headObj);
      const head = await db.prepare(sqlH).bind(...bindH).first();

      if (items.length) {
        const ins = db.prepare(`
          INSERT INTO ${T_ITEMS} (qNo, itemCode, itemName, qty, unitPrice, lineTotal, remark, CreateDate, UpdateDate, CreateBy, UpdateBy)
          VALUES (?, ?, ?, ?, ?, ?, '', datetime('now'), datetime('now'), ?, ?)
        `);
        for (const it of items) {
          const qty=Number(it.qty||0), price=Number(it.price||0), disc=Number(it.discount||0);
          const line=Math.max(0, qty*price - disc);
          await ins.bind(head.qNo, it.service||"", it.tooth||"", qty, price, line, user, user).run();
        }
      }
      return send(head);
    }

    if ((method==="PUT"||method==="PATCH") && idFromPath) {
      const body = await readBody();
      const cust = parseCustomer(body.customer);
      const items= parseItems(body.items);
      const totalBefore = items.reduce((s,it)=> s + (Number(it.qty||0)*Number(it.price||0)), 0);
      const discount    = items.reduce((s,it)=> s + Number(it.discount||0), 0);
      const grandTotal  = +(totalBefore - discount).toFixed(2);

      const updObj = await addAuditOnUpdate(T_HEAD, {
        qNo: body.qNo || null,
        qDate: body.qDate || null,
        status: body.confirmed ? "Confirmed" : "Draft",
        customerCode: cust.code || "",
        customerFirstName: cust.firstName || "",
        customerLastName: cust.lastName || "",
        customerNationalId: cust.nationalId || "",
        customerAge: Number(cust.age || 0),
        totalBeforeDiscount: totalBefore,
        discount, grandTotal,
        note: body.note || ""
      });
      const {sql,bind}=buildUpdate(T_HEAD, updObj, headPK);
      const head=await db.prepare(sql).bind(...bind, idFromPath).first();
      if (!head) return err("not found",404);

      await db.prepare(`DELETE FROM ${T_ITEMS} WHERE qNo=?`).bind(head.qNo).run();
      if (items.length) {
        const ins = db.prepare(`
          INSERT INTO ${T_ITEMS} (qNo, itemCode, itemName, qty, unitPrice, lineTotal, remark, CreateDate, UpdateDate, CreateBy, UpdateBy)
          VALUES (?, ?, ?, ?, ?, ?, '', datetime('now'), datetime('now'), ?, ?)
        `);
        for (const it of items) {
          const qty=Number(it.qty||0), price=Number(it.price||0), disc=Number(it.discount||0);
          const line=Math.max(0, qty*price - disc);
          await ins.bind(head.qNo, it.service||"", it.tooth||"", qty, price, line, user, user).run();
        }
      }
      return send(head);
    }

    if (method==="DELETE" && idFromPath) {
      const head=await db.prepare(`DELETE FROM ${T_HEAD} WHERE ${headPK}=? RETURNING *`).bind(idFromPath).first();
      if (head) await db.prepare(`DELETE FROM ${T_ITEMS} WHERE qNo=?`).bind(head.qNo).run();
      return head?send(head):err("not found",404);
    }

    return err("method not allowed",405);
  }

  // ===== Sales: Orders (หน้าใช้ตารางนี้ + payload) =====
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
