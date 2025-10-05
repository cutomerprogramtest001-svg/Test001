// functions/api/[[path]].js
export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const db = env.DB;                              // D1 binding = DB
  const user = request.headers.get("x-user") || "system";

  // ---------- helpers ----------
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

  const pathSeg = url.pathname.replace(/^\/api\/?/, "").split("/").filter(Boolean); // e.g. ["hr","employees","123"]
  const tailId = (pathSeg.length>=3 && /^\d+$/.test(pathSeg[2])) ? pathSeg[2] : null;

  // ---------- CORS ----------
  if (method === "OPTIONS") return new Response(null, { status: 204, headers: baseHeaders });
  if (!url.pathname.startsWith("/api")) return json({ ok:false, error:"Not found" }, 404);

  // ---------- health ----------
  if (pathSeg.length === 0) {
    return json({ ok:true, service:"bizapp-api", time:new Date().toISOString() });
  }

  // ---------- GEO (ใช้พารามิเตอร์แบบ underscore ให้ตรงหน้า) ----------
  if (pathSeg[0] === "geo") {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS geo_provinces (id INTEGER PRIMARY KEY, name_th TEXT, name_en TEXT);
      CREATE TABLE IF NOT EXISTS geo_amphures (id INTEGER PRIMARY KEY, province_id INTEGER, name_th TEXT, name_en TEXT);
      CREATE TABLE IF NOT EXISTS geo_tambons  (id INTEGER PRIMARY KEY, amphure_id  INTEGER, zip_code TEXT, name_th TEXT, name_en TEXT);
    `);
    if (pathSeg[1] === "provinces" && method === "GET") {
      const rs = await db.prepare(`SELECT id,name_th,name_en FROM geo_provinces ORDER BY name_th`).all();
      return json(rs.results||[]);
    }
    if (pathSeg[1] === "amphures" && method === "GET") {
      const pid = q("province_id"); const st = pid? db.prepare(`SELECT id,province_id,name_th,name_en FROM geo_amphures WHERE province_id=? ORDER BY name_th`).bind(+pid)
                                                  : db.prepare(`SELECT id,province_id,name_th,name_en FROM geo_amphures ORDER BY name_th`);
      const rs = await st.all(); return json(rs.results||[]);
    }
    if (pathSeg[1] === "tambons" && method === "GET") {
      const aid = q("amphure_id"); const st = aid? db.prepare(`SELECT id,amphure_id,zip_code,name_th,name_en FROM geo_tambons WHERE amphure_id=? ORDER BY name_th`).bind(+aid)
                                                 : db.prepare(`SELECT id,amphure_id,zip_code,name_th,name_en FROM geo_tambons ORDER BY name_th`);
      const rs = await st.all(); return json(rs.results||[]);
    }
    // seed (ดึงชุดข้อมูลจาก repo สาธารณะ ถ้าจำเป็นค่อยให้ผมเพิ่ม endp seed แบบ offline)
    if (pathSeg[1] === "seed" && (method==="POST"||method==="GET")) {
      const c = await db.prepare(`SELECT COUNT(*) c FROM geo_provinces`).first();
      if (c?.c>0) return json({ ok:true, data:{seeded:false, reason:"already"} });
      const base="https://raw.githubusercontent.com/kongvut/thai-province-data/master";
      const [p,a,t] = await Promise.all([fetch(`${base}/api_province.json`), fetch(`${base}/api_amphure.json`), fetch(`${base}/api_tambon.json`)]);
      if (!p.ok||!a.ok||!t.ok) return json({ ok:false, error:"seed fetch failed" }, 500);
      const [P,A,T]=[await p.json(), await a.json(), await t.json()];
      const insP=db.prepare(`INSERT INTO geo_provinces(id,name_th,name_en) VALUES (?,?,?)`);
      for(const r of P) await insP.bind(r.id,r.name_th,r.name_en).run();
      const insA=db.prepare(`INSERT INTO geo_amphures(id,province_id,name_th,name_en) VALUES (?,?,?,?)`);
      for(const r of A) await insA.bind(r.id,r.province_id,r.name_th,r.name_en).run();
      const insT=db.prepare(`INSERT INTO geo_tambons(id,amphure_id,zip_code,name_th,name_en) VALUES (?,?,?,?,?)`);
      for(const r of T) await insT.bind(r.id,r.amphure_id,String(r.zip_code||""),r.name_th,r.name_en).run();
      return json({ ok:true, data:{seeded:true, provinces:P.length, amphures:A.length, tambons:T.length }});
    }
    return json({ ok:false, error:"geo not found" }, 404);
  }

  // ---------- HR ----------
  if (pathSeg[0]==="hr" && pathSeg[1]==="employees") {
    const table="hr_employees", pk=await getPK(table);
    if (method==="GET" && !tailId) { const rs=await db.prepare(`SELECT * FROM ${table} ORDER BY ${pk} DESC`).all(); return json(rs.results||[]); }
    if (method==="POST") { const body=await addAuditOnCreate(table, await readBody()); const {sql,bind}=buildInsert(table,body); const row=await db.prepare(sql).bind(...bind).first(); return json({data:row}); }
    if ((method==="PUT"||method==="PATCH") && tailId) { const body=await addAuditOnUpdate(table, await readBody()); const {sql,bind}=buildUpdate(table,body,pk); const row=await db.prepare(sql).bind(...bind, tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    if (method==="DELETE" && tailId) { const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    return json({ ok:false, error:"method not allowed" },405);
  }

  if (pathSeg[0]==="hr" && pathSeg[1]==="attendance") {
    const table="hr_attendance", pk=await getPK(table);
    if (method==="GET" && !tailId) {
      const from=q("from"), to=q("to"), emp=q("emp"); const bind=[]; let sql=`SELECT * FROM ${table}`;
      const wh=[]; if(from){ wh.push(`date>=?`); bind.push(from);} if(to){wh.push(`date<=?`);bind.push(to);} if(emp){wh.push(`empId=?`);bind.push(emp);}
      if (wh.length) sql+=` WHERE `+wh.join(" AND "); sql+=` ORDER BY date DESC, ${pk} DESC`;
      const rs=await db.prepare(sql).bind(...bind).all(); return json(rs.results||[]);
    }
    if (method==="POST") { const body=await addAuditOnCreate(table, await readBody()); const {sql,bind}=buildInsert(table,body); const row=await db.prepare(sql).bind(...bind).first(); return json({data:row}); }
    if ((method==="PUT"||method==="PATCH") && tailId) { const body=await addAuditOnUpdate(table, await readBody()); const {sql,bind}=buildUpdate(table,body,pk); const row=await db.prepare(sql).bind(...bind,tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    if (method==="DELETE" && tailId) { const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    return json({ ok:false, error:"method not allowed" },405);
  }

  if (pathSeg[0]==="hr" && pathSeg[1]==="timeclock") {
    const table="hr_timeclock", pk=await getPK(table);
    if (method==="GET" && !tailId) {
      const from=q("from"), to=q("to"), emp=q("emp"); const bind=[]; let sql=`SELECT * FROM ${table}`;
      const wh=[]; if(from){ wh.push(`date>=?`); bind.push(from);} if(to){wh.push(`date<=?`);bind.push(to);} if(emp){wh.push(`empId=?`);bind.push(emp);}
      if (wh.length) sql+=` WHERE `+wh.join(" AND "); sql+=` ORDER BY date DESC, ${pk} DESC`;
      const rs=await db.prepare(sql).bind(...bind).all(); return json(rs.results||[]);
    }
    if (method==="POST") {
      const body = await readBody();
      // เติมชั่วโมงถ้าให้ in/out มา
      if (!body.hours && body.inAt && body.outAt) {
        try {
          const a = new Date(`1970-01-01T${body.inAt}:00Z`).getTime();
          const b = new Date(`1970-01-01T${body.outAt}:00Z`).getTime();
          if (Number.isFinite(a)&&Number.isFinite(b)) body.hours = Math.max(0, (b-a)/(1000*60*60));
        } catch {}
      }
      const payload=await addAuditOnCreate(table, body);
      const {sql,bind}=buildInsert(table,payload); const row=await db.prepare(sql).bind(...bind).first(); return json({data:row});
    }
    if ((method==="PUT"||method==="PATCH") && tailId) { const payload=await addAuditOnUpdate(table, await readBody()); const {sql,bind}=buildUpdate(table,payload,pk); const row=await db.prepare(sql).bind(...bind,tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    if (method==="DELETE" && tailId) { const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    return json({ ok:false, error:"method not allowed" },405);
  }

  // ---------- SALES: customers ----------
  if (pathSeg[0]==="sales" && pathSeg[1]==="customers") {
    const table="sales_customers", pk=await getPK(table);
    if (method==="GET" && !tailId) {
      const search=q("search"); let sql=`SELECT * FROM ${table}`, bind=[];
      if (search) { sql+=` WHERE (LOWER(code) LIKE ? OR LOWER(firstName) LIKE ? OR LOWER(lastName) LIKE ? OR nationalId LIKE ?)`; bind=[...Array(3).fill(`%${search.toLowerCase()}%`), `%${search}%`]; }
      sql+=` ORDER BY ${pk} DESC LIMIT ${Math.min(+q("limit","100")||100,1000)} OFFSET ${Math.max(+q("offset","0")||0,0)}`;
      const rs=await db.prepare(sql).bind(...bind).all(); return json(rs.results||[]);
    }
    if (method==="POST") { const body=await addAuditOnCreate(table, await readBody()); const {sql,bind}=buildInsert(table,body); const row=await db.prepare(sql).bind(...bind).first(); return json({data:row}); }
    if ((method==="PUT"||method==="PATCH") && tailId) { const body=await addAuditOnUpdate(table, await readBody()); const {sql,bind}=buildUpdate(table,body,pk); const row=await db.prepare(sql).bind(...bind,tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    if (method==="DELETE" && tailId) { const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(tailId).first(); return row?json({data:row}):json({ok:false,error:"not found"},404); }
    return json({ ok:false, error:"method not allowed" },405);
  }

  // ---------- SALES: quotations (หน้าเว็บส่ง customer/items เป็น JSON string) ----------
  if (pathSeg[0]==="sales" && pathSeg[1]==="quotations") {
    const T_HEAD="sales_quotations", T_ITEMS="sales_quotationitems";
    const headPK=await getPK(T_HEAD), itemPK=await getPK(T_ITEMS);

    const parseCustomer = (s) => { try{ return s && typeof s==="string" ? JSON.parse(s) : (s||{}); }catch{ return {}; } };
    const parseItems    = (s) => { try{ return s && typeof s==="string" ? JSON.parse(s) : (Array.isArray(s)?s:[]); }catch{ return []; } };

    if (method==="GET" && !tailId) {
      const rs=await db.prepare(`SELECT * FROM ${T_HEAD} ORDER BY ${headPK} DESC`).all();
      return json(rs.results||[]);
    }

    if (method==="POST") {
      const body = await readBody();
      const cust = parseCustomer(body.customer);
      const items= parseItems(body.items);

      // summary
      const totalBefore = items.reduce((s,it)=> s + (Number(it.qty||0)*Number(it.price||0)), 0);
      const discount    = items.reduce((s,it)=> s + Number(it.discount||0), 0);
      const grandTotal  = +(totalBefore - discount).toFixed(2);

      // insert head
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
        discount: discount,
        grandTotal: grandTotal,
        note: body.note || ""
      });
      const {sql:sqlH, bind:bindH} = buildInsert(T_HEAD, headObj);
      const head = await db.prepare(sqlH).bind(...bindH).first();

      // insert items
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
      return json({ data: head });
    }

    if ((method==="PUT"||method==="PATCH") && tailId) {
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
        discount: discount,
        grandTotal: grandTotal,
        note: body.note || ""
      });
      const {sql,bind}=buildUpdate(T_HEAD, updObj, headPK);
      const head=await db.prepare(sql).bind(...bind, tailId).first();
      if (!head) return json({ ok:false, error:"not found" },404);

      // replace items of this qNo
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
      return json({ data: head });
    }

    if (method==="DELETE" && tailId) {
      const head=await db.prepare(`DELETE FROM ${T_HEAD} WHERE ${headPK}=? RETURNING *`).bind(tailId).first();
      if (head) await db.prepare(`DELETE FROM ${T_ITEMS} WHERE qNo=?`).bind(head.qNo).run();
      return head?json({data:head}):json({ok:false,error:"not found"},404);
    }

    return json({ ok:false, error:"method not allowed" },405);
  }

  // ---------- SALES: orders (หน้าเว็บต้องการตารางที่มี payload) ----------
  if (pathSeg[0]==="sales" && pathSeg[1]==="orders") {
    // สร้างตาราง (ถ้าไม่เคยมี) ให้ตรงกับหน้า
    await db.exec(`
      CREATE TABLE IF NOT EXISTS sales_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        docNo TEXT,         -- soNo
        date TEXT,          -- soDate
        customerId TEXT,
        customerName TEXT,
        status TEXT,
        total REAL,
        note TEXT,
        refNo TEXT,
        payload TEXT,       -- เก็บฟอร์มเต็ม (JSON)
        CreateDate TEXT DEFAULT (datetime('now')),
        CreateBy   TEXT,
        UpdateDate TEXT DEFAULT (datetime('now')),
        UpdateBy   TEXT
      );
    `);
    const table="sales_orders", pk=await getPK(table);

    if (method==="GET" && !tailId) {
      const search=q("search"), from=q("from"), to=q("to");
      const bind=[]; let sql=`SELECT * FROM ${table}`;
      const wh=[];
      if (search) { wh.push(`(LOWER(docNo) LIKE ? OR LOWER(customerName) LIKE ? OR customerId LIKE ?)`); bind.push(`%${search.toLowerCase()}%`,`%${search.toLowerCase()}%`, `%${search}%`); }
      if (from)   { wh.push(`date>=?`); bind.push(from); }
      if (to)     { wh.push(`date<=?`); bind.push(to); }
      if (wh.length) sql+=` WHERE `+wh.join(" AND ");
      sql+=` ORDER BY date DESC, ${pk} DESC LIMIT ${Math.min(+q("limit","50")||50,1000)} OFFSET ${Math.max(+q("offset","0")||0,0)}`;
      const rs=await db.prepare(sql).bind(...bind).all(); return json(rs.results||[]);
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
      return json({ data: row });
    }

    if ((method==="PUT"||method==="PATCH") && tailId) {
      const b = await readBody();
      const payload = await addAuditOnUpdate(table, {
        docNo: b.docNo||null, date: b.date||null,
        customerId: b.customerId||null, customerName: b.customerName||null,
        status: b.status||"Open", total: Number(b.total||0),
        note: b.note||"", refNo: b.refNo||"",
        payload: typeof b.payload==="string" ? b.payload : JSON.stringify(b.payload ?? {})
      });
      const {sql,bind}=buildUpdate(table,payload,pk);
      const row=await db.prepare(sql).bind(...bind, tailId).first();
      return row?json({data:row}):json({ok:false,error:"not found"},404);
    }

    if (method==="DELETE" && tailId) {
      const row=await db.prepare(`DELETE FROM ${table} WHERE ${pk}=? RETURNING *`).bind(tailId).first();
      return row?json({data:row}):json({ok:false,error:"not found"},404);
    }

    return json({ ok:false, error:"method not allowed" },405);
  }

  // ---------- fallback ----------
  return json({ ok:false, error:`No route for: ${pathSeg.join('/')}` },404);
};
