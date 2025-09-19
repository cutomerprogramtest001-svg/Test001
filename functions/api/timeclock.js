// GET /api/timeclock
// POST /api/timeclock

export async function onRequestGet({ env }) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM timeclock ORDER BY date DESC"
  ).all();
  return json(results);
}

export async function onRequestPost({ env, request }) {
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    INSERT INTO timeclock (
      empId, empName, date, inAt, outAt, note, geo,
      CreateDate, CreateBy, UpdateDate, UpdateBy
    ) VALUES (?, ?, ?, ?, ?, ?, ?,
              datetime('now'), ?, datetime('now'), ?)
  `).bind(
    body.empId, body.empName, body.date, body.inAt, body.outAt, body.note, body.geo,
    user, user
  ).run();

  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
