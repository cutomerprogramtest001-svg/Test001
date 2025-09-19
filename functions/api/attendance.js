// GET /api/attendance
// POST /api/attendance

export async function onRequestGet({ env }) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM attendance ORDER BY date DESC"
  ).all();
  return json(results);
}

export async function onRequestPost({ env, request }) {
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    INSERT INTO attendance (
      empId, fullName, position, date, checkIn, checkOut,
      leaveType, status, note,
      CreateDate, CreateBy, UpdateDate, UpdateBy
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
              datetime('now'), ?, datetime('now'), ?)
  `).bind(
    body.empId, body.fullName, body.position, body.date, body.checkIn, body.checkOut,
    body.leaveType, body.status, body.note,
    user, user
  ).run();

  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}
