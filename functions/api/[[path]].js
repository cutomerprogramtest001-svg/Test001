// functions/api/[[path]].js
export const onRequest = async (ctx) => {
  const { request, env } = ctx;
  const url = new URL(request.url);
  // Expect paths like: /api/hr/employees, /api/employees, /api/tables, /api/tables/<table>, /api/tables/<table>/columns
  if (!url.pathname.startsWith('/api')) {
    return json({ error: 'Not found' }, 404);
  }

  const path = url.pathname.replace(/^\/api\/?/, '').trim(); // strip /api/
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
