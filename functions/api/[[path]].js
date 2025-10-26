/* ===== GEO (Single-table) – helpers only, ไม่แตะ router อื่น ===== */
const __geoJson = (data, status=200, headers={}) =>
  new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", ...headers }
  });


// handler เฉพาะ /api/geo/*
async function __handleGeo(ctx, path, db){
  if (!path.startsWith('geo/')) return null;
  const cors = ctx.baseHeaders || { "content-type": "application/json; charset=utf-8" };

  // === Provinces from geo_flat ===
  if (path === 'geo/provinces') {
    const rs = await db.prepare(
      `SELECT province AS name, MIN(id) AS id
         FROM geo_flat
        WHERE province IS NOT NULL AND TRIM(province) <> ''
        GROUP BY province
        ORDER BY name COLLATE NOCASE`
    ).all();
    return __geoJson({ data: (rs.results || rs || []) }, 200, cors);
  }

  // === Amphures by province_id (province pseudo-id from MIN(id)) ===
  if (path.startsWith('geo/amphures')) {
    const u = ctx.request ? new URL(ctx.request.url) : null;
    const pid = u ? (u.searchParams.get('province_id') || '').trim() : '';
    if (!pid) return __geoJson({ error:'province_id required' }, 400, cors);

    // province_id มาจาก MIN(id) ในกลุ่มจังหวัด → ใช้ id นี้หา "province name"
    const row = await db.prepare(`SELECT province FROM geo_flat WHERE id = ?`).bind(pid).first();
    if (!row?.province) return __geoJson({ data: [] }, 200, cors);

    const rs = await db.prepare(
      `SELECT district AS name, MIN(id) AS id
         FROM geo_flat
        WHERE province = ?
          AND district IS NOT NULL AND TRIM(district) <> ''
        GROUP BY district
        ORDER BY name COLLATE NOCASE`
    ).bind(row.province).all();
    return __geoJson({ data: (rs.results || rs || []) }, 200, cors);
  }

  // === Tambons by amphure_id (amphure pseudo-id from MIN(id)) ===
  if (path.startsWith('geo/tambons')) {
    const u = ctx.request ? new URL(ctx.request.url) : null;
    const aid = u ? (u.searchParams.get('amphure_id') || '').trim() : '';
    if (!aid) return __geoJson({ error:'amphure_id required' }, 400, cors);

    // amphure_id มาจาก MIN(id) ในกลุ่มอำเภอ → ใช้ id นี้หา "province & district"
    const row = await db.prepare(`SELECT province, district FROM geo_flat WHERE id = ?`).bind(aid).first();
    if (!row?.province || !row?.district) return __geoJson({ data: [] }, 200, cors);

    const rs = await db.prepare(
      `SELECT subdistrict AS name,
              MIN(id) AS id,
              MAX(COALESCE(zipcode,'')) AS zipcode
         FROM geo_flat
        WHERE province = ? AND district = ?
          AND subdistrict IS NOT NULL AND TRIM(subdistrict) <> ''
        GROUP BY subdistrict
        ORDER BY name COLLATE NOCASE`
    ).bind(row.province, row.district).all();
    return __geoJson({ data: (rs.results || rs || []) }, 200, cors);
  }

  // (ทางเลือก) สถานะรวมสำหรับ sanity check
  if (path === 'geo/status') {
    const p = await db.prepare(`SELECT COUNT(DISTINCT province) n FROM geo_flat WHERE TRIM(province)<>''`).first();
    const a = await db.prepare(`SELECT COUNT(DISTINCT province||'>'||district) n FROM geo_flat WHERE TRIM(district)<>''`).first();
    const t = await db.prepare(`SELECT COUNT(DISTINCT province||'>'||district||'>'||subdistrict) n FROM geo_flat WHERE TRIM(subdistrict)<>''`).first();
    return __geoJson({ data: { provinces:p?.n||0, amphures:a?.n||0, tambons:t?.n||0 } }, 200, cors);
  }

  return null;
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
  // ===== Mount routers เฉพาะที่ต้องการจริง ๆ =====
  // (เราใช้ Sales: Quotations block เดิมอยู่แล้ว จึงไม่ต้อง quotationsRouter ซ้ำ)
  {
    const r = await saleOrdersRouter({ request, url, path, db, send, err });
    if (r) return r;
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
        // GET /api/sales/customers/:code/credit
    if (method==="GET" && idFromPath && seg[3]==="credit") {
      const code = decodeURIComponent(idFromPath);
      const row = await db.prepare(`
        SELECT code, creditDays, creditLimit, paymentType
        FROM sales_customers
        WHERE code=? LIMIT 1
      `).bind(code).first();
      return send({
        code,
        creditDays : Number(row?.creditDays ?? 0),
        creditLimit: Number(row?.creditLimit ?? 0),
        paymentType: row?.paymentType || 'cash'
      });
    }
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
      const status    = (q("status") || "").trim();          // e.g. "Confirm"
      const withItems = q("withItems") === "1";

      let sql = `SELECT * FROM ${T_HEAD}`;
      let bind = [];
      if (status) {
        // รองรับทั้ง Confirm / Confirmed
        sql += ` WHERE LOWER(status) IN (?, ?)`;
        bind = [status.toLowerCase(), (status.toLowerCase()==="confirm"?"confirmed":"confirm")];
      }
      sql += ` ORDER BY ${headPK} DESC`;

      const heads = (await db.prepare(sql).bind(...bind).all()).results || [];
      if (!withItems || !heads.length) return send(heads);

      const qnos = heads.map(h => h.qNo).filter(Boolean);
      let items = [];
      if (qnos.length) {
        const qs = qnos.map(()=>'?').join(',');
        items = (await db.prepare(
          `SELECT qNo, itemCode, itemName, qty, unitPrice, lineTotal
             FROM ${T_ITEMS} WHERE qNo IN (${qs}) ORDER BY id ASC`
        ).bind(...qnos).all()).results || [];
      }
      const byQ = {};
      for (const it of items) (byQ[it.qNo] ||= []).push(it);
      for (const h of heads) h.items = byQ[h.qNo] || [];
      return send(heads);
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

  // =============== QUOTATIONS ROUTER (pending for Sale Order) ===============
async function quotationsRouter({ request, url, path, db, send, err }) {
  if (!path.startsWith("sales/quotations")) return null;

  // GET /api/sales/quotations?status=Confirm&withItems=1
  if (path === "sales/quotations" && request.method === "GET") {
    const status = (url.searchParams.get("status") || "").trim();
    const withItems = (url.searchParams.get("withItems") || "0") === "1";

    let rows = [];
    if (status) {
      rows = (await db.prepare(
        `SELECT * FROM sales_quotations WHERE status=? ORDER BY id DESC`
      ).bind(status).all()).results || [];
    } else {
      rows = (await db.prepare(
        `SELECT * FROM sales_quotations ORDER BY id DESC`
      ).all()).results || [];
    }

    if (!withItems) return send(rows);

    // ดึง items ทั้งหมดในครั้งเดียว แล้ว group
    const ids = rows.map(r => r.qNo).filter(Boolean);
    let items = [];
    if (ids.length) {
      const qs = ids.map(()=>"?").join(",");
      items = (await db.prepare(
        `SELECT * FROM sales_quotationitems WHERE qNo IN (${qs})`
      ).bind(...ids).all()).results || [];
    }
    const byQ = {};
    for (const it of items) {
      (byQ[it.qNo] ||= []).push(it);
    }
    for (const q of rows) q.items = byQ[q.qNo] || [];
    return send(rows);
  }

  return null;
}
// ===================== SALE ORDERS ROUTER =====================
async function saleOrdersRouter({ request, url, path, db, send, err }) {
  if (!path.startsWith("sales/orders")) return null;

  // GET /api/sales/orders?search=&page=&size=
  if (path === "sales/orders" && request.method === "GET") {
    const q = (url.searchParams.get("search") || "").trim();
    const page = parseInt(url.searchParams.get("page") || "1", 10);
    const size = Math.min(parseInt(url.searchParams.get("size") || "20", 10), 100);
    const off  = (page - 1) * size;

    let base = `SELECT * FROM sales_saleorders`;
    let where = "";
    let bind = [];
    if (q) {
      where = ` WHERE soNo LIKE ? OR customerCode LIKE ? `;
      bind = [`%${q}%`, `%${q}%`];
    }
    const rows = await db.prepare(`${base}${where} ORDER BY id DESC LIMIT ? OFFSET ?`)
                         .bind(...bind, size, off).all();
    return send(rows.results || []);
  }

  // POST /api/sales/orders (สร้างจาก Quotation ที่ confirm)
    // GET /api/sales/orders/next-no?date=YYYY-MM-DD
  if (path === "sales/orders/next-no" && request.method === "GET") {
    const q = url.searchParams;
    const ymd = (q.get("date") || new Date().toISOString().slice(0,10)).replace(/-/g,'');
    const last = await db.prepare(
      `SELECT soNo FROM sales_saleorders
       WHERE soNo LIKE ? ORDER BY id DESC LIMIT 1`
    ).bind(`SO${ymd}-%`).first();

    let run = 1;
    if (last?.soNo) {
      const m = last.soNo.match(/-(\d{4})$/);
      if (m) run = parseInt(m[1],10) + 1;
    }
    const soNo = `SO${ymd}-${String(run).padStart(4,'0')}`;
    return send({ soNo });
  }

  // GET /api/sales/orders/next-no?date=YYYY-MM-DD
if (path === "sales/orders/next-no" && request.method === "GET") {
  const q = url.searchParams;
  const ymd = (q.get("date") || new Date().toISOString().slice(0,10)).replace(/-/g,'');
  const last = await db.prepare(
    `SELECT soNo FROM sales_saleorders
     WHERE soNo LIKE ? ORDER BY id DESC LIMIT 1`
  ).bind(`SO${ymd}-%`).first();

  let run = 1;
  if (last?.soNo) {
    const m = last.soNo.match(/-(\d{3,4})$/);
    if (m) run = parseInt(m[1],10) + 1;
  }
  const soNo = `SO${ymd}-${String(run).padStart(4,'0')}`;
  return send({ soNo });
}


  // POST /api/sales/orders
if (path === "sales/orders" && request.method === "POST") {
  try {
    const b = await (async()=>{
      try{ return await request.json(); } catch { return {}; }
    })();

    // --- 1) gen soNo (ใช้ soNo ที่ได้รับถ้าไม่ใช่ TMP) ---
    const soDate = (b.soDate || new Date().toISOString().slice(0,10));
    let soNo = (b.soNo || "").trim();
    const ymd = soDate.replace(/-/g,'');
    if (!soNo || /-TMP$/i.test(soNo)) {
      const last = await db.prepare(
        `SELECT soNo FROM sales_saleorders WHERE soNo LIKE ? ORDER BY id DESC LIMIT 1`
      ).bind(`SO${ymd}-%`).first();
      let run = 1;
      if (last?.soNo) {
        const m = last.soNo.match(/-(\d{3,4})$/);
        if (m) run = parseInt(m[1],10)+1;
      }
      soNo = `SO${ymd}-${String(run).padStart(4,'0')}`;
    }

    // --- 2) หา creditDays ของลูกค้า (ถ้ามี) ---
    let creditDays = 0;
    if (b.customerCode) {
      try {
        const c = await db.prepare(`SELECT creditDays FROM sales_customers WHERE code=? LIMIT 1`).bind(b.customerCode).first();
        creditDays = Number(c?.creditDays || 0);
      } catch(e){ creditDays = 0; }
    }

    // --- 3) คำนวณ dueDate = deliveryDate + creditDays ---
    const deliveryDate = (b.deliveryDate || "").toString().trim();
    let dueDate = "";
    if (deliveryDate) {
      const dt = new Date(deliveryDate);
      if (Number.isFinite(creditDays) && creditDays>0) dt.setDate(dt.getDate() + Number(creditDays));
      dueDate = dt.toISOString().slice(0,10);
    } else {
      dueDate = (b.dueDate || "").toString().trim(); // ถ้าส่งมาใช้ค่านั้น
    }

    // --- 4) payment / balance calculations ---
    const paymentType = (b.paymentType || "FULL").toString().toUpperCase(); // FULL | DEPOSIT
    const grandTotal  = Number(b.grandTotal || 0);
    let depositAmount = Number(b.depositAmount || 0);
    const depositPercent = b.depositPercent != null ? Number(b.depositPercent) : null;
    const installmentCount = b.installmentCount != null ? Number(b.installmentCount) : (Array.isArray(b.paymentPlan?.schedule) ? b.paymentPlan.schedule.length : null);

    if (paymentType === "DEPOSIT") {
      if ((depositAmount||0) <= 0 && (depositPercent||0) > 0) {
        depositAmount = +(grandTotal * depositPercent / 100).toFixed(2);
      }
    } else {
      depositAmount = 0;
    }
    const totalPaid = (paymentType === "FULL") ? grandTotal : depositAmount;
    const balance   = Math.max(0, +(grandTotal - totalPaid).toFixed(2));

    // --- 5) แปลงค่าว่าเป็น JSON string (paymentPlan) หรือ null ---
    const paymentPlanStr = b.paymentPlan ? JSON.stringify(b.paymentPlan) : null;

    // --- 6) Insert head (ระวังจำนวน placeholder ให้ตรงกับ bind) ---
    // Columns: soNo, soDate, status, customerCode, billTo, shipTo, paymentTerm,
    // totalBeforeDiscount, discount, grandTotal, note,
    // deliveryDate, dueDate, paymentType, depositAmount, depositPercent, installmentCount,
    // totalPaid, balance, paymentPlan, CreateDate
    const insHead = await db.prepare(`
      INSERT INTO sales_saleorders
        (soNo, soDate, status, customerCode, billTo, shipTo, paymentTerm,
         totalBeforeDiscount, discount, grandTotal, note,
         deliveryDate, dueDate, paymentType, depositAmount, depositPercent, installmentCount,
         totalPaid, balance, paymentPlan, CreateDate)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
    `).bind(
      soNo,
      soDate,
      (b.status || "Open"),
      (b.customerCode || ""),
      (b.billTo || ""),
      (b.shipTo || ""),
      (b.paymentTerm || ""),
      Number(b.totalBeforeDiscount || 0),
      Number(b.discount || 0),
      grandTotal,
      (b.note || ""),
      deliveryDate || null,
      dueDate || null,
      paymentType,
      (isFinite(depositAmount) ? depositAmount : 0),
      (depositPercent != null ? depositPercent : null),
      (installmentCount != null ? installmentCount : null),
      totalPaid,
      balance,
      paymentPlanStr
    ).run();

    if (!insHead || !insHead.success) {
      return send({ error: "Insert sale order head failed", detail: insHead }, 500);
    }

    // --- 7) หาค่า id ที่เพิ่ง insert (ผูก items) ---
    const head = await db.prepare(`SELECT id FROM sales_saleorders WHERE soNo=? LIMIT 1`).bind(soNo).first();
    const soId = head?.id || null;

    // --- 8) ถ้ามี items ให้ insert ทีละรายการ ---
    if (Array.isArray(b.items) && b.items.length>0) {
      const stmt = await db.prepare(`
        INSERT INTO sales_saleorderitems
          (soNo, itemCode, itemName, qty, uom, unitPrice, lineTotal, remark, CreateDate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
      `);
      for (const it of b.items) {
        const qty  = Number(it.qty || 0);
        const price= Number(it.unitPrice || 0);
        const line = Number((it.lineTotal != null) ? it.lineTotal : +(qty * price).toFixed(2));
        const res = await stmt.bind(
          soNo,
          (it.itemCode || ""),
          (it.itemName || ""),
          qty,
          (it.uom || ""),
          price,
          line,
          (it.remark || "")
        ).run();
        if (!res || !res.success) {
          return send({ error: "Insert sale order item failed", item: it, detail: res }, 500);
        }
      }
    }

    // --- 9) ถ้าต้องการ patch Quotation ให้หลุด pending (ไม่บังคับ) ---
    if (b.refQuotationNo) {
      try {
        // ถ้า table/endpoint ใช้ id มากกว่า qNo ให้ปรับโค้ดนี้
        await db.prepare(`UPDATE sales_quotations SET confirmed = 0 WHERE qNo = ?`).bind(b.refQuotationNo).run();
      } catch(e) { /* ไม่บล็อกการบันทึก SO หาก patch Q ล้ม */ }
    }

    // --- 10) ส่งผลลัพธ์กลับ ---
    return send({ ok: true, soNo, soId, dueDate, balance });
  } catch (err) {
    console.error("POST /api/sales/orders error:", err);
    // ถ้ามีฟังก์ชัน send ให้ใช้ ถ้าไม่มีกลับ Response ธรรมดา
    try { return send({ error: err?.message || String(err) }, 500); }
    catch(e){ return new Response(JSON.stringify({ error: err?.message || String(err) }), { status:500, headers:{'Content-Type':'application/json'} }); }
  }
}

  return null;
}

  // ----- fallback -----
  return err(`No route for: ${seg.join("/")}`, 404);
};
