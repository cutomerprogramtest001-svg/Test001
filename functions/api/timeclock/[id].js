export async function onRequestPut({ env, request, params }) {
  const id = params.id;
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    UPDATE timeclock SET
      empId=?, empName=?, date=?, inAt=?, outAt=?, note=?, geo=?,
      UpdateDate=datetime('now'), UpdateBy=?
    WHERE id=?
  `).bind(
    body.empId, body.empName, body.date, body.inAt, body.outAt, body.note, body.geo,
    user, id
  ).run();

  return json({ ok: true });
}

export async function onRequestDelete({ env, params }) {
  await env.DB.prepare("DELETE FROM timeclock WHERE id=?").bind(params.id).run();
  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
