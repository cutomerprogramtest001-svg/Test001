// GET /api/quotes
// POST /api/quotes

export async function onRequestGet({ env }) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM quotes ORDER BY CreateDate DESC"
  ).all();
  return json(results);
}

export async function onRequestPost({ env, request }) {
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    INSERT INTO quotes (
      qNo, qDate, customer, items, note, confirmed,
      CreateDate, CreateBy, UpdateDate, UpdateBy
    ) VALUES (?, ?, ?, ?, ?, ?,
              datetime('now'), ?, datetime('now'), ?)
  `).bind(
    body.qNo, body.qDate, body.customer, body.items, body.note, body.confirmed ? 1 : 0,
    user, user
  ).run();

  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
