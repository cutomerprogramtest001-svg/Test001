// functions/api/[[path]].js
export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  // Expect paths like: /api/hr/employees, /api/employees, /api/tables, /api/tables/<table>, /api/tables/<table>/columns
  if (!url.pathname.startsWith('/api')) {
    return json({ error: 'Not found' }, 404);
  }

  const path = url.pathname.replace(/^\/api\/?/, '').trim(); // strip /api/
  const method = request.method.toUpperCase();
  const user = request.headers.get('x-user') || 'system';
  const db = env.DB; // <-- bind D1 as "DB" in Pages > Settings > Functions > D1

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
    headers: { 'content-type': 'application/json' }
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
