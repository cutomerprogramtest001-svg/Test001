export async function onRequestPut({ env, request, params }) {
  const id = params.id;
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    UPDATE customers SET
      code=?, firstName=?, lastName=?, nationalId=?, gender=?, birthday=?, age=?,
      phone=?, email=?, provinceId=?, provinceName=?, postcode=?, medical=?,
      UpdateDate=datetime('now'), UpdateBy=?
    WHERE id=?
  `).bind(
    body.code, body.firstName, body.lastName, body.nationalId, body.gender, body.birthday, body.age,
    body.phone, body.email, body.provinceId, body.provinceName, body.postcode,
    body.medical, user, id
  ).run();

  return json({ ok: true });
}

export async function onRequestDelete({ env, params }) {
  await env.DB.prepare("DELETE FROM customers WHERE id=?").bind(params.id).run();
  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
