// PUT /api/employees/:id
// DELETE /api/employees/:id

export async function onRequestPut({ env, request, params }) {
  const id = params.id;
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    UPDATE employees SET
      firstName=?, lastName=?, nationalId=?, gender=?, phone=?, email=?,
      department=?, position=?, startDate=?, lastDate=?, dob=?,
      salaryType=?, salaryValue=?,
      UpdateDate=datetime('now'), UpdateBy=?
    WHERE id=?
  `).bind(
    body.firstName, body.lastName, body.nationalId, body.gender, body.phone, body.email,
    body.department, body.position, body.startDate, body.lastDate, body.dob,
    body.salaryType, Number(body.salaryValue||0),
    user, id
  ).run();

  return json({ ok: true });
}

export async function onRequestDelete({ env, params }) {
  await env.DB.prepare("DELETE FROM employees WHERE id=?").bind(params.id).run();
  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}
