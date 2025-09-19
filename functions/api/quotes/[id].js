export async function onRequestPut({ env, request, params }) {
  const id = params.id;
  const user = request.headers.get("x-user") || "system";
  const body = await request.json();

  await env.DB.prepare(`
    UPDATE quotes SET
      qNo=?, qDate=?, customer=?, items=?, note=?, confirmed=?,
      UpdateDate=datetime('now'), UpdateBy=?
    WHERE id=?
  `).bind(
    body.qNo, body.qDate, body.customer, body.items, body.note, body.confirmed ? 1 : 0,
    user, id
  ).run();

  return json({ ok: true });
}

export async function onRequestDelete({ env, params }) {
  await env.DB.prepare("DELETE FROM quotes WHERE id=?").bind(params.id).run();
  return json({ ok: true });
}

function json(data, status=200) {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}
