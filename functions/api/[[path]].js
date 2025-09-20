// functions/api/[[path]].js
// ONE FILE to serve ALL 45+ tables and every module/button in index.html
// D1 binding name: DB
// It supports:
//   - Exact table routes:       /api/<table> and /api/<table>/<id>
//   - Module alias routes:      /api/{hr|sales|purch|inv}/<alias> and /api/{...}/<alias>/<id>
//   - Generic discovery routes: /api/tables , /api/tables/<table> , /api/tables/<table>/<id> , /api/tables/<table>/columns
// Auto-handles audit columns when present: CreateDate, CreateBy, UpdateDate, UpdateBy
// Accepts 'x-user' header for CreateBy/UpdateBy
//
// EXTRA: UI-aware normalizers map payloads from index.html to correct schema columns,
// so you don't need to change the frontend bindings (e.g., salaryValue -> salary, address object -> flat cols).

export async function onRequest(ctx) {
  try {
    const { request, env } = ctx;
    const url = new URL(request.url);
    const full = url.pathname.replace(/^\/+|\/+$/g, "");
    const parts = full.split("/");
    if (parts[0] !== "api") return json({ error: "Not Found" }, 404);

    const method = request.method.toUpperCase();
    const seg = parts.slice(1);
    const user = request.headers.get("x-user") || "system";

    // Module alias first (more friendly for the frontend in index.html)
    // /api/<module>/<alias>  or  /api/<module>/<alias>/<id>
    if (seg.length >= 2 && MODULE_ALIAS[seg[0]] && MODULE_ALIAS[seg[0]][seg[1]]) {
      const table = MODULE_ALIAS[seg[0]][seg[1]];
      const id = seg[2] && isFinite(+seg[2]) ? Math.trunc(+seg[2]) : null;

      if (id == null) {
        if (method === "GET")  return json(await listTable(env.DB, table, request));
        if (method === "POST") {
          const bodyRaw = await safeJson(request);
          const body = normalizeForTable(table, bodyRaw);
          await insertRow(env.DB, table, body, user, ctx);
          return json({ ok: true });
        }
        return json({ error: "Method Not Allowed" }, 405);
      } else {
        if (method === "PUT") {
          const bodyRaw = await safeJson(request);
          const body = normalizeForTable(table, bodyRaw);
          await updateRow(env.DB, table, id, body, user);
          return json({ ok: true });
        }
        if (method === "DELETE") {
          await deleteRow(env.DB, table, id);
          return json({ ok: true });
        }
        return json({ error: "Method Not Allowed" }, 405);
      }
    }

    // Exact table routes: /api/<table>  or  /api/<table>/<id>
    if (seg.length >= 1 && TABLE_SET.has(seg[0])) {
      const table = seg[0];
      const id = seg[1] && isFinite(+seg[1]) ? Math.trunc(+seg[1]) : null;

      if (id == null) {
        if (method === "GET")  return json(await listTable(env.DB, table, request));
        if (method === "POST") {
          const raw = await safeJson(request);
          const body = normalizeForTable(table, raw);
          await insertRow(env.DB, table, body, user, ctx);
          return json({ ok: true });
        }
        return json({ error: "Method Not Allowed" }, 405);
      } else {
        if (method === "PUT") {
          const raw = await safeJson(request);
          const body = normalizeForTable(table, raw);
          await updateRow(env.DB, table, id, body, user);
          return json({ ok: true });
        }
        if (method === "DELETE") {
          await deleteRow(env.DB, table, id);
          return json({ ok: true });
        }
        return json({ error: "Method Not Allowed" }, 405);
      }
    }

    // Generic routes: /api/tables
    if (seg[0] === "tables") {
      if (seg.length === 1) {
        if (method !== "GET") return json({ error: "Method Not Allowed" }, 405);
        const tables = await listUserTables(env.DB);
        return json(tables);
      }
      const table = safeIdent(seg[1]);
      if (!TABLE_SET.has(table)) return json({ error: `Table '${table}' not found` }, 404);

      if (seg.length === 2) {
        if (method === "GET")  return json(await listTable(env.DB, table, request));
        if (method === "POST") {
          const raw = await safeJson(request);
          const body = normalizeForTable(table, raw);
          await insertRow(env.DB, table, body, user, ctx);
          return json({ ok: true });
        }
        return json({ error: "Method Not Allowed" }, 405);
      }
      if (seg.length === 3 && seg[2] === "columns") {
        if (method !== "GET") return json({ error: "Method Not Allowed" }, 405);
        return json(await getCols(env.DB, table));
      }
      if (seg.length === 3) {
        const id = isFinite(+seg[2]) ? Math.trunc(+seg[2]) : null;
        if (id == null) return json({ error: "Bad id" }, 400);
        if (method === "PUT") {
          const raw = await safeJson(request);
          const body = normalizeForTable(table, raw);
          await updateRow(env.DB, table, id, body, user);
          return json({ ok: true });
        }
        if (method === "DELETE") {
          await deleteRow(env.DB, table, id);
          return json({ ok: true });
        }
        return json({ error: "Method Not Allowed" }, 405);
      }
    }

    return json({ error: "Not Found" }, 404);
  } catch (e) {
    return json({ error: String(e?.message ?? e) }, 500);
  }
}

/* =========================
   MODULE ALIASES (45 tables)
   ========================= */

const HR = {
  employees:           "hr_employees",
  attendance:          "hr_attendance",
  timeclock:           "hr_timeclock",
  payrollperiods:      "hr_payrollperiods",
  payslips:            "hr_payslips",
  payslipitems:        "hr_payslipitems",
  evaluations:         "hr_evaluations",
  jobopenings:         "hr_jobopenings",
  candidates:          "hr_candidates",
  interviews:          "hr_interviews",
};
const SALES = {
  customers:           "sales_customers",
  quotations:          "sales_quotations",
  quotationitems:      "sales_quotationitems",
  saleorders:          "sales_saleorders",
  saleorderitems:      "sales_saleorderitems",
  contacts:            "sales_contacts",
  appointments:        "sales_appointments",
  salesleads:          "sales_salesleads",
  conversionstats:     "sales_conversionstats",
};
const PURCH = {
  suppliers:           "purch_suppliers",
  pr:                  "purch_pr",
  pr_items:            "purch_pr_items",
  rfq:                 "purch_rfq",
  rfq_items:           "purch_rfq_items",
  po:                  "purch_po",
  po_items:            "purch_po_items",
  grn:                 "purch_grn",
  grn_items:           "purch_grn_items",
  invoices:            "purch_invoices",
  audittrail:          "purch_audittrail",
};
const INV = {
  products:            "inv_products",
  inventorymovements:  "inv_inventorymovements",
  stockcount:          "inv_stockcount",
  stockcountaudit:     "inv_stockcountaudit",
  stockcountaudititems:"inv_stockcountaudititems",
  damage:              "inv_damage",
  onhandsnapshots:     "inv_onhandsnapshots",
  productbatches:      "inv_productbatches",
  pickingorders:       "inv_pickingorders",
  pickingitems:        "inv_pickingitems",
  dispatches:          "inv_dispatches",
  dispatchitems:       "inv_dispatchitems",
  transfers:           "inv_transfers",
  transferitems:       "inv_transferitems",
  inventorykpi:        "inv_inventorykpi",
};

const MODULE_ALIAS = { hr: HR, sales: SALES, purch: PURCH, inv: INV };

const TABLE_SET = new Set([
  // HR
  "hr_employees","hr_attendance","hr_timeclock","hr_payrollperiods","hr_payslips","hr_payslipitems","hr_evaluations","hr_jobopenings","hr_candidates","hr_interviews",
  // Sales
  "sales_customers","sales_quotations","sales_quotationitems","sales_saleorders","sales_saleorderitems","sales_contacts","sales_appointments","sales_salesleads","sales_conversionstats",
  // Purchasing
  "purch_suppliers","purch_pr","purch_pr_items","purch_rfq","purch_rfq_items","purch_po","purch_po_items","purch_grn","purch_grn_items","purch_invoices","purch_audittrail",
  // Inventory
  "inv_products","inv_inventorymovements","inv_stockcount","inv_stockcountaudit","inv_stockcountaudititems","inv_damage","inv_onhandsnapshots","inv_productbatches","inv_pickingorders","inv_pickingitems","inv_dispatches","inv_dispatchitems","inv_transfers","inv_transferitems","inv_inventorykpi",
]);

/* =========================
   UI-AWARE NORMALIZERS
   ========================= */

function normalizeForTable(table, body){
  if (!body || typeof body !== "object") return {};

  // Common cleanup
  const b = { ...body };

  // Flatten address if present (from customers form)
  if (b.addr && typeof b.addr === "object") {
    const a = b.addr;
    b.addr_no = a.no ?? b.addr_no;
    b.addr_road = a.road ?? b.addr_road;
    b.addr_soi = a.soi ?? b.addr_soi;
    b.provinceId = a.provinceId ?? b.provinceId;
    b.provinceName = a.provinceName ?? b.provinceName;
    b.amphureId = a.amphureId ?? b.amphureId;
    b.amphureName = a.amphureName ?? b.amphureName;
    b.tambonId = a.tambonId ?? b.tambonId;
    b.tambonName = a.tambonName ?? b.tambonName;
    b.postcode = a.postcode ?? b.postcode;
    delete b.addr;
  }

  // Flatten medical fields if present
  if (b.medical && typeof b.medical === "object") {
    const m = b.medical;
    const join = (x)=> Array.isArray(x) ? x.join(",") : (x||"");
    b.diseases = join(m.diseases);
    b.diseaseOther = m.diseaseOther || "";
    b.allergies = join(m.allergies);
    b.allergyOther = m.allergyOther || "";
    delete b.medical;
  }

  switch (table) {
    case "hr_employees": {
      // From employeeApp: salaryValue -> salary; salaryDisplay is UI-only
      if (b.salaryValue != null && b.salary == null) {
        b.salary = b.salaryValue; delete b.salaryValue;
      }
      delete b.salaryDisplay;
      return b;
    }
    case "hr_attendance": {
      // align keys
      return b;
    }
    case "hr_timeclock": {
      // compute hours if missing and both inAt/outAt exist
      if (!b.hours && b.inAt && b.outAt) {
        b.hours = hoursBetween(b.inAt, b.outAt);
      }
      return b;
    }

    case "sales_customers": {
      // already flattened above
      return b;
    }

    case "sales_quotations": {
      // index might send 'customer' object
      if (b.customer && typeof b.customer === "object") {
        const c = b.customer;
        b.customerCode = c.code ?? b.customerCode;
        b.customerFirstName = c.firstName ?? b.customerFirstName;
        b.customerLastName  = c.lastName ?? b.customerLastName;
        b.customerNationalId= c.nationalId ?? b.customerNationalId;
        b.customerAge       = c.age ?? b.customerAge;
        delete b.customer;
      }
      return b;
    }

    case "sales_saleorders": {
      // allow header+items together
      return b;
    }

    case "purch_pr": {
      // from prApp: date->prDate, requestedBy->requester, department vs dept, note->remark
      if (b.date && !b.prDate) { b.prDate = b.date; delete b.date; }
      if (b.requestedBy && !b.requester) { b.requester = b.requestedBy; delete b.requestedBy; }
      if (b.dept && !b.department) { b.department = b.dept; delete b.dept; }
      if (b.note && !b.remark) { b.remark = b.note; /* keep note if needed */ }
      return b;
    }
    case "purch_rfq": {
      if (b.date && !b.rfqDate) { b.rfqDate = b.date; delete b.date; }
      if (b.linkPRNo && !b.prNo) { b.prNo = b.linkPRNo; delete b.linkPRNo; }
      return b;
    }
    case "purch_po": {
      if (b.date && !b.poDate) { b.poDate = b.date; delete b.date; }
      if (b.supplier && !b.supplierCode) { b.supplierCode = b.supplier; }
      return b;
    }
    case "purch_grn": {
      if (b.date && !b.grnDate) { b.grnDate = b.date; delete b.date; }
      return b;
    }

    default:
      return b;
  }
}

// Helper for hours calc (HH:mm to decimal hours)
function hoursBetween(inAt, outAt){
  try {
    const a = new Date(`1970-01-01T${inAt}:00Z`).getTime();
    const b = new Date(`1970-01-01T${outAt}:00Z`).getTime();
    return ((b-a)/(1000*60*60)).toFixed(2);
  } catch { return ""; }
}

/* =========================
   HEADER→ITEMS Support
   ========================= */

// If a POST body for a header table contains `items` (array), we will also insert rows into its item table.
const HEADER_ITEMS = {
  sales_quotations:     { itemsTable: "sales_quotationitems", key: "qNo",
    map: (hItem) => ({
      qNo: hItem.qNo,             // ensure set by caller below
      itemCode: hItem.itemCode ?? hItem.code ?? "",
      itemName: hItem.itemName ?? hItem.name ?? "",
      qty: Number(hItem.qty ?? 0),
      unitPrice: Number(hItem.unitPrice ?? hItem.price ?? 0),
      lineTotal: Number(hItem.lineTotal ?? (Number(hItem.qty ?? 0) * Number(hItem.unitPrice ?? hItem.price ?? 0)))
    })
  },
  sales_saleorders:     { itemsTable: "sales_saleorderitems", key: "soNo",
    map: (hItem) => ({
      soNo: hItem.soNo,
      itemCode: hItem.itemCode ?? hItem.code ?? "",
      itemName: hItem.itemName ?? hItem.name ?? "",
      qty: Number(hItem.qty ?? 0),
      uom: hItem.uom ?? hItem.unit ?? "",
      unitPrice: Number(hItem.unitPrice ?? hItem.price ?? 0),
      lineTotal: Number(hItem.lineTotal ?? (Number(hItem.qty ?? 0) * Number(hItem.unitPrice ?? hItem.price ?? 0))),
      remark: hItem.remark ?? hItem.note ?? ""
    })
  },
  purch_pr:            { itemsTable: "purch_pr_items", key: "prNo",
    map: (it) => ({
      prNo: it.prNo,
      itemCode: it.itemCode ?? it.code ?? "",
      itemDesc: it.itemDesc ?? it.name ?? "",
      qty: Number(it.qty ?? 0),
      uom: it.uom ?? it.unit ?? "",
      targetDate: it.targetDate ?? it.requiredDate ?? "",
      remark: it.remark ?? it.note ?? ""
    })
  },
  purch_rfq:           { itemsTable: "purch_rfq_items", key: "rfqNo",
    map: (it) => ({
      rfqNo: it.rfqNo,
      supplierCode: it.supplierCode ?? "",
      itemCode: it.itemCode ?? it.code ?? "",
      itemDesc: it.itemDesc ?? it.name ?? "",
      qty: Number(it.qty ?? 0),
      uom: it.uom ?? it.unit ?? "",
      unitPrice: Number(it.unitPrice ?? 0),
      currency: it.currency ?? "THB",
      leadTimeDays: it.leadTimeDays ?? ""
    })
  },
  purch_po:            { itemsTable: "purch_po_items", key: "poNo",
    map: (it) => ({
      poNo: it.poNo,
      itemCode: it.itemCode ?? it.code ?? "",
      itemDesc: it.itemDesc ?? it.name ?? "",
      qtyOrder: Number(it.qtyOrder ?? it.qty ?? 0),
      uom: it.uom ?? it.unit ?? "",
      unitPrice: Number(it.unitPrice ?? it.price ?? 0),
      amount: Number(it.amount ?? (Number(it.qty ?? it.qtyOrder ?? 0) * Number(it.unitPrice ?? it.price ?? 0))),
      currency: it.currency ?? "THB"
    })
  },
  purch_grn:           { itemsTable: "purch_grn_items", key: "grnNo",
    map: (it) => ({
      grnNo: it.grnNo,
      poLineId: it.poLineId ?? "",
      itemCode: it.itemCode ?? it.code ?? "",
      itemName: it.itemName ?? it.name ?? "",
      qtyReceive: Number(it.qtyReceive ?? it.qty ?? 0),
      uom: it.uom ?? it.unit ?? "",
      location: it.location ?? "",
      lotNo: it.lotNo ?? "",
      expireDate: it.expireDate ?? "",
      remark: it.remark ?? it.note ?? ""
    })
  },
  inv_pickingorders:   { itemsTable: "inv_pickingitems", key: "pickNo",
    map: (it) => ({
      pickNo: it.pickNo,
      itemCode: it.itemCode ?? it.code ?? "",
      itemName: it.itemName ?? it.name ?? "",
      qty: Number(it.qty ?? 0),
      uom: it.uom ?? it.unit ?? "",
      location: it.location ?? "",
      remark: it.remark ?? it.note ?? ""
    })
  },
  inv_dispatches:      { itemsTable: "inv_dispatchitems", key: "dispatchNo",
    map: (it) => ({
      dispatchNo: it.dispatchNo,
      itemCode: it.itemCode ?? it.code ?? "",
      itemName: it.itemName ?? it.name ?? "",
      qty: Number(it.qty ?? 0),
      uom: it.uom ?? it.unit ?? "",
      location: it.location ?? "",
      remark: it.remark ?? it.note ?? ""
    })
  },
  inv_transfers:       { itemsTable: "inv_transferitems", key: "transferNo",
    map: (it) => ({
      transferNo: it.transferNo,
      itemCode: it.itemCode ?? it.code ?? "",
      itemName: it.itemName ?? it.name ?? "",
      qty: Number(it.qty ?? 0),
      uom: it.uom ?? it.unit ?? "",
      fromLocation: it.fromLocation ?? "",
      toLocation: it.toLocation ?? "",
      remark: it.remark ?? it.note ?? ""
    })
  },
};

// Wrap insertRow to also insert items if payload contains { items: [...] }
async function insertRow(DB, table, body, user, ctx){
  const cols = await getCols(DB, table);
  const keys = Object.keys(body).filter(k => cols.includes(k) && k !== "id");
  let vals = keys.map(k => body[k]);

  const nowCols = [];
  if (cols.includes("CreateDate") && !keys.includes("CreateDate")) { keys.push("CreateDate"); nowCols.push("CreateDate"); }
  if (cols.includes("CreateBy")   && !keys.includes("CreateBy"))   { keys.push("CreateBy");   vals.push(user); }
  if (cols.includes("UpdateDate") && !keys.includes("UpdateDate")) { keys.push("UpdateDate"); nowCols.push("UpdateDate"); }
  if (cols.includes("UpdateBy")   && !keys.includes("UpdateBy"))   { keys.push("UpdateBy");   vals.push(user); }

  if (!keys.length) throw new Error("No valid columns to insert");

  const colList = keys.map(safeIdent).join(", ");
  const placeholders = keys.map(k => nowCols.includes(k) ? "datetime('now')" : "?").join(", ");
  const bindVals = vals.filter((_,i)=>!nowCols.includes(keys[i]));

  const stmts = [];
  stmts.push(DB.prepare(`INSERT INTO ${table} (${colList}) VALUES (${placeholders})`).bind(...bindVals));

  // If header has items, insert them too
  const hi = HEADER_ITEMS[table];
  if (Array.isArray(body.items) && hi) {
    // decide header key value (like qNo/soNo/poNo)
    const headerKey = hi.key;
    const headerVal = body[headerKey] ?? body.no ?? body.code ?? body.id;
    for (const rawIt of body.items) {
      const it = hi.map({ ...rawIt, [headerKey]: headerVal });
      const itCols = await getCols(DB, hi.itemsTable);
      const itKeys = Object.keys(it).filter(k=>itCols.includes(k) && k!=="id");
      let itVals = itKeys.map(k=>it[k]);
      const itNow = [];
      if (itCols.includes("CreateDate")) { itKeys.push("CreateDate"); itNow.push("CreateDate"); }
      if (itCols.includes("CreateBy"))   { itKeys.push("CreateBy");   itVals.push(user); }
      if (itCols.includes("UpdateDate")) { itKeys.push("UpdateDate"); itNow.push("UpdateDate"); }
      if (itCols.includes("UpdateBy"))   { itKeys.push("UpdateBy");   itVals.push(user); }
      const itColsSql = itKeys.map(safeIdent).join(", ");
      const itPh = itKeys.map(k => itNow.includes(k) ? "datetime('now')" : "?").join(", ");
      const itBind = itVals.filter((_,i)=>!itNow.includes(itKeys[i]));
      stmts.push(DB.prepare(`INSERT INTO ${hi.itemsTable} (${itColsSql}) VALUES (${itPh})`).bind(...itBind));
    }
  }

  if (typeof DB.batch === "function") {
    await DB.batch(stmts);
  } else {
    for (const s of stmts) await s.run();
  }
}

async function updateRow(DB, table, id, body, user){
  const cols = await getCols(DB, table);
  const keys = Object.keys(body).filter(k => cols.includes(k) && k !== "id" && k !== "CreateDate" && k !== "CreateBy");
  const sets = keys.map(k => `${safeIdent(k)}=?`);
  const vals = keys.map(k => body[k]);

  if (cols.includes("UpdateDate")) sets.push(`UpdateDate=datetime('now')`);
  if (cols.includes("UpdateBy"))   { sets.push(`UpdateBy=?`); vals.push(user); }

  if (!sets.length) throw new Error("No valid columns to update");

  await DB.prepare(`UPDATE ${table} SET ${sets.join(", ")} WHERE id=?`).bind(...vals, id).run();
}

async function deleteRow(DB, table, id){
  await DB.prepare(`DELETE FROM ${table} WHERE id=?`).bind(id).run();
}

/* =========================
   SELECT helpers
   ========================= */

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
async function safeJson(request){ const t=await request.text(); if(!t) return {}; try{return JSON.parse(t);}catch{ throw new Error("Invalid JSON body"); } }
function safeIdent(name=""){ if(!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new Error("Bad identifier"); return name; }
function clampInt(v,min,max,dflt){ const n=parseInt(v??""); return Number.isFinite(n)?Math.min(max,Math.max(min,n)):dflt; }

async function listUserTables(DB){
  const { results } = await DB.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).all();
  return (results||[]).map(r=>r.name);
}
async function getCols(DB, table){
  const { results } = await DB.prepare(`PRAGMA table_info(${table})`).all();
  return (results||[]).map(r=>r.name);
}
async function orderClause(DB, table, want, desc){
  const cols = await getCols(DB, table);
  const by = want && cols.includes(want) ? want : (cols.includes("CreateDate") ? "CreateDate" : (cols.includes("id") ? "id" : null));
  return by ? ` ORDER BY ${by} ${desc ? "DESC" : "ASC"}` : "";
}
async function listTable(DB, table, request){
  const url = new URL(request.url);
  const limit   = clampInt(url.searchParams.get("limit"), 1, 500, 100);
  const offset  = clampInt(url.searchParams.get("offset"), 0, 1e9, 0);
  const orderBy = url.searchParams.get("orderBy");
  const desc    = url.searchParams.get("desc") === "1";
  const orderSql = await orderClause(DB, table, orderBy, desc);
  const { results } = await DB.prepare(`SELECT * FROM ${table}${orderSql} LIMIT ? OFFSET ?`).bind(limit, offset).all();
  return results || [];
}
