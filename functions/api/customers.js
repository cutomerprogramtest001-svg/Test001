// GET /api/customers
// POST /api/customers

export async function onRequestGet({ env }) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM customers ORDER BY CreateDate DESC"
  ).all();
  return json(results);
}

export async function onRequestPost({ env, request }) {
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    INSERT INTO customers (
      code, firstName, lastName, nationalId, gender, birthday, age,
      phone, email, provinceId, provinceName, postcode, medical,
      CreateDate, CreateBy, UpdateDate, UpdateBy
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              datetime('now'), ?, datetime('now'), ?)
  `).bind(
    body.code, body.firstName, body.lastName, body.nationalId, body.gender, body.birthday, body.age,
    body.phone, body.email, body.provinceId, body.provinceName, body.postcode,
    body.medical, user, user
  ).run();

  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
