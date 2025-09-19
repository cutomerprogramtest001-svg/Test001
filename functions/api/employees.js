// GET /api/employees
// POST /api/employees

export async function onRequestGet({ env }) {
  const { results } = await env.DB.prepare(
    "SELECT * FROM employees ORDER BY datetime(CreateDate) DESC"
  ).all();
  return json(results);
}

export async function onRequestPost({ env, request }) {
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    INSERT INTO employees (
      employeeId, firstName, lastName, nationalId, gender, phone, email,
      department, position, startDate, lastDate, dob,
      salaryType, salaryValue,
      CreateDate, CreateBy, UpdateDate, UpdateBy
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              datetime('now'), ?, datetime('now'), ?)
  `).bind(
    body.employeeId, body.firstName, body.lastName, body.nationalId, body.gender,
    body.phone, body.email, body.department, body.position,
    body.startDate, body.lastDate, body.dob,
    body.salaryType, Number(body.salaryValue||0),
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
