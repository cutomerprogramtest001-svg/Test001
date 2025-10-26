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

  // --- path segments & helpers (ต้องมี ก่อนใช้ seg/q/idFromPath) ---
  const seg = path.split('/').filter(Boolean);             // ["geo","provinces"] | ["hr","employees",":id"] ...
  const [domain, resource, id] = seg;
  const idFromPath = seg.length >= 3 ? decodeURIComponent(seg[2]) : null;

  const q = (name, def = "") => (url.searchParams.get(name) ?? def).trim();


  // ===== GEO (single-table: geo_flat) =====
if (seg[0] === "geo") {
  // ใช้ตารางเดียว geo_flat
  await db.exec(`
    CREATE TABLE IF NOT EXISTS geo_flat (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      province    TEXT NOT NULL,
      district    TEXT NOT NULL,
      subdistrict TEXT NOT NULL,
      zipcode     TEXT,
      latitude    REAL,
      longitude   REAL
    );
    CREATE INDEX IF NOT EXISTS idx_geo_flat_p   ON geo_flat(province);
    CREATE INDEX IF NOT EXISTS idx_geo_flat_d   ON geo_flat(district);
    CREATE INDEX IF NOT EXISTS idx_geo_flat_s   ON geo_flat(subdistrict);
    CREATE INDEX IF NOT EXISTS idx_geo_flat_zip ON geo_flat(zipcode);
  `);

  // GET /api/geo/provinces -> [{id,name_th,name_en}]
  if (seg[1] === "provinces" && method === "GET") {
    const rs = await db.prepare(
      `SELECT DISTINCT province AS name FROM geo_flat ORDER BY province`
    ).all();
    const data = (rs.results || []).map((r, i) => ({
      id: i + 1,
      name: r.name,          // ✅ UI มองเห็นแน่นอน
      nameTh: r.name,        // (สำรองแบบ camelCase)
      nameEn: r.name
    }));
    return send(data);
  }

  // GET /api/geo/amphures?province_id=<ชื่อจังหวัดหรือเลข index>
  if (seg[1] === "amphures" && method === "GET") {
    const pid = q("province_id");
    if (!pid) return err("province_id required", 400);

    // รองรับทั้ง 'กรุงเทพมหานคร' (ชื่อ) หรือ '1' (เลข index จาก dropdown)
    let provinceName = pid;
    if (/^\d+$/.test(pid)) {
      const prov = await db.prepare(
        `SELECT DISTINCT province AS name FROM geo_flat ORDER BY province LIMIT 1 OFFSET ?`
      ).bind(Math.max(parseInt(pid,10)-1,0)).first();
      provinceName = prov?.name || "";
    }

    const rs = await db.prepare(
      `SELECT DISTINCT district AS name
         FROM geo_flat
        WHERE province=?
        ORDER BY district`
    ).bind(provinceName).all();

    const data = (rs.results || []).map((r, i) => ({
      id: i + 1,
      name: r.name,      // ✅ ให้ UI ใช้ได้ทันที
      nameTh: r.name,
      nameEn: r.name
    }));

    return send(data);
  }

  // GET /api/geo/tambons?amphure_id=<ชื่อเขต/อำเภอ>&province_id=<ชื่อหรือเลข>
  if (seg[1] === "tambons" && method === "GET") {
    const aid = q("amphure_id");
    const pid = q("province_id");
    if (!aid || !pid) return err("amphure_id & province_id required", 400);

    let provinceName = pid;
    if (/^\d+$/.test(pid)) {
      const prov = await db.prepare(
        `SELECT DISTINCT province AS name FROM geo_flat ORDER BY province LIMIT 1 OFFSET ?`
      ).bind(Math.max(parseInt(pid,10)-1,0)).first();
      provinceName = prov?.name || "";
    }

    const rs = await db.prepare(
      `SELECT subdistrict AS name, zipcode
         FROM geo_flat
        WHERE province=? AND district=?
        GROUP BY subdistrict, zipcode
        ORDER BY subdistrict`
    ).bind(provinceName, aid).all();

    const data = (rs.results || []).map((r, i) => ({
      id: i + 1,
      name: r.name,                  // ✅ ให้ UI ใช้ได้ทันที
      nameTh: r.name,
      nameEn: r.name,
      zipcode: r.zipcode || ""       // ✅ UI ใช้คีย์ zipcode อยู่แล้ว
    }));

    return send(data);
  }

  return err("geo not found", 404);
}
// ===== /GEO (geo_flat) =====


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
  const idFromPath = id;

  const json = (data, status = 200) =>
    new Response(JSON.stringify(data), {
      status,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
    });

  const toLocalYMD = (d = new Date(), offsetMin = 7 * 60) => {
    // ให้ date เป็นเวลาไทย (+07:00)
    const t = new Date(d.getTime() + offsetMin * 60000);
    return t.toISOString().slice(0, 10);
  };

  // CORS preflight (กันไว้ก่อน)
  if (method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      }
    });
  }

  // ---------- POST /api/hr/timeclock  (Clock In) ----------
  if (method === 'POST') {
    try {
      const b = await request.json().catch(() => ({}));

      const empId = (b.empId || '').trim();
      if (!empId) return json({ ok: false, error: 'empId required' }, 400);

      const nowISO = new Date().toISOString();
      const ymd = (b.date && String(b.date).trim()) || toLocalYMD();

      // กันแถวค้าง (ยังไม่ออกงาน)
      const open = await db
        .prepare(
          `SELECT ${pk} AS id FROM ${table}
           WHERE empId=? AND date=? AND (outAt IS NULL OR outAt='' OR outAt='null')
           ORDER BY ${pk} DESC LIMIT 1`
        )
        .bind(empId, ymd)
        .first();
      if (open) return json({ ok: false, reason: 'already_open', id: open.id }, 409);

      // INSERT (ไม่ใช้ RETURNING) แล้ว SELECT ตาม last_insert_rowid()
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

  // ---------- PUT/PATCH /api/hr/timeclock/:id  (Clock Out / Update) ----------
  if ((method === 'PUT' || method === 'PATCH') && idFromPath) {
    try {
      const b = await request.json().catch(() => ({}));

      const cur = await db
        .prepare(`SELECT * FROM ${table} WHERE ${pk}=?`)
        .bind(idFromPath)
        .first();
      if (!cur) return json({ ok: false, error: 'not found' }, 404);

      // อนุญาตทั้ง outAt จาก body หรือสั่งออกเดี๋ยวนี้
      let outAtISO = b.outAt ? new Date(b.outAt).toISOString() : null;
      if (!outAtISO && (b.clockOutNow === true || b.clockOutNow === 'true' || b.clockOutNow === '1')) {
        outAtISO = new Date().toISOString();
      }

      // คำนวณชั่วโมง (ถ้ามี inAt และ outAt)
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
          idFromPath
        )
        .run();

      const row = await db
        .prepare(`SELECT * FROM ${table} WHERE ${pk}=?`)
        .bind(idFromPath)
        .first();

      return json({ ok: true, row });
    } catch (e) {
      console.error('[timeclock PUT]', e);
      return json({ ok: false, error: String(e) }, 500);
    }
  }

  // ---------- GET /api/hr/timeclock?empId=..&from=YYYY-MM-DD&to=YYYY-MM-DD ----------
  if (method === 'GET') {
    try {
      const q = new URLSearchParams(url.search);
      const empId = (q.get('empId') || '').trim();
      const from = (q.get('from') || '').trim();
      const to = (q.get('to') || '').trim();

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

  // ---------- DELETE /api/hr/timeclock/:id ----------
  if (method === 'DELETE' && idFromPath) {
    try {
      await db.prepare(`DELETE FROM ${table} WHERE ${pk}=?`).bind(idFromPath).run();
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

// ===== Sales: Quotations (หน้าโพสต์ customer/items เป็น JSON string) =====
if (seg[0] === "sales" && seg[1] === "quotations") {
  const T_HEAD  = "sales_quotations";
  const T_ITEMS = "sales_quotationitems";
  const headPK  = await getPK(T_HEAD);

  const parseCustomer = (s) => { try { return s && typeof s === "string" ? JSON.parse(s) : (s || {}); } catch { return {}; } };
  const parseItems    = (s) => { try { return s && typeof s === "string" ? JSON.parse(s) : (Array.isArray(s) ? s : []); } catch { return []; } };

  // ---------- Helpers: safe quotation number ----------
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

  // LIST: GET /api/sales/quotations
  if (method === "GET" && !idFromPath) {
    const rs = await db.prepare(`SELECT * FROM ${T_HEAD} ORDER BY ${headPK} DESC`).all();
    return send(rs.results || []);
  }

  // GET by id: /api/sales/quotations/:id  -> head + items (คำนวณ discount กลับ)
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
      return {
        service : r.itemCode || "",
        tooth   : r.itemName || "",
        qty, price,
        discount: +discount.toFixed(2)
      };
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

  // CREATE: POST /api/sales/quotations
  if (method === "POST") {
    const body  = await readBody();
    const cust  = parseCustomer(body.customer);
    const items = parseItems(body.items);

    // สร้าง qNo แบบปลอดชน ถ้าไม่ส่งมา หรือส่งมาซ้ำ
    let qNo = (body.qNo || "").trim();
    if (!qNo || await qNoExists(qNo)) {
      qNo = await genQNoSafe(body.qDate || null);
    }

    const totalBefore = items.reduce((s, it) => s + (Number(it.qty || 0) * Number(it.price || 0)), 0);
    const discount    = items.reduce((s, it) => s + Number(it.discount || 0), 0);
    const grandTotal  = +(totalBefore - discount).toFixed(2);

    const headObj = await addAuditOnCreate(T_HEAD, {
      qNo,
      qDate                  : body.qDate || null,
      status                 : body.confirmed ? "Confirmed" : "Draft",
      customerCode           : cust.code || "",
      customerFirstName      : cust.firstName || "",
      customerLastName       : cust.lastName || "",
      customerNationalId     : cust.nationalId || "",
      customerAge            : Number(cust.age || 0),
      totalBeforeDiscount    : totalBefore,
      discount,
      grandTotal,
      note                   : body.note || ""
    });
    const { sql: sqlH, bind: bindH } = buildInsert(T_HEAD, headObj);
    const head = await db.prepare(sqlH).bind(...bindH).first();

    if (items.length) {
      // ใช้เฉพาะคอลัมน์ที่มีจริงในสคีมา
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

  // UPDATE: PUT/PATCH /api/sales/quotations/:id
  if ((method === "PUT" || method === "PATCH") && idFromPath) {
    const body  = await readBody();
    const cust  = parseCustomer(body.customer);
    const items = parseItems(body.items);

    const totalBefore = items.reduce((s, it) => s + (Number(it.qty || 0) * Number(it.price || 0)), 0);
    const discount    = items.reduce((s, it) => s + Number(it.discount || 0), 0);
    const grandTotal  = +(totalBefore - discount).toFixed(2);

    const updObj = await addAuditOnUpdate(T_HEAD, {
      qNo                    : body.qNo || null,
      qDate                  : body.qDate || null,
      status                 : body.confirmed ? "Confirmed" : "Draft",
      customerCode           : cust.code || "",
      customerFirstName      : cust.firstName || "",
      customerLastName       : cust.lastName || "",
      customerNationalId     : cust.nationalId || "",
      customerAge            : Number(cust.age || 0),
      totalBeforeDiscount    : totalBefore,
      discount,
      grandTotal,
      note                   : body.note || ""
    });
    const { sql, bind } = buildUpdate(T_HEAD, updObj, headPK);
    const head = await db.prepare(sql).bind(...bind, idFromPath).first();
    if (!head) return err("not found", 404);

    // เคลียร์ items เดิมของ qNo (ใช้ qNo หลังอัปเดต)
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

  // DELETE: /api/sales/quotations/:id  (ลบหัว + ไอเท็ม)
  if (method === "DELETE" && idFromPath) {
    const head = await db.prepare(`DELETE FROM ${T_HEAD} WHERE ${headPK}=? RETURNING *`).bind(idFromPath).first();
    if (head) await db.prepare(`DELETE FROM ${T_ITEMS} WHERE qNo=?`).bind(head.qNo).run();
    return head ? send(head) : err("not found", 404);
  }

  return err("method not allowed", 405);
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
