export async function onRequestPut({ env, request, params }) {
  const id = params.id;
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    UPDATE attendance SET
      empId=?, fullName=?, position=?, date=?, checkIn=?, checkOut=?,
      leaveType=?, status=?, note=?,
      UpdateDate=datetime('now'), UpdateBy=?
    WHERE id=?
  `).bind(
    body.empId, body.fullName, body.position, body.date, body.checkIn, body.checkOut,
    body.leaveType, body.status, body.note,
    user, id
  ).run();

  return json({ ok: true });
}

export async function onRequestDelete({ env, params }) {
  await env.DB.prepare("DELETE FROM attendance WHERE id=?").bind(params.id).run();
  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
