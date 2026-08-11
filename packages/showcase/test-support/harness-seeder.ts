// Wrangler 4.118's test-harness getEnv() does not settle when the environment
// contains D1. This second Worker shares the real config's database id and
// seeds through workerd, keeping the fidelity test on the harness-owned D1.
interface Env {
  DB: {
    prepare(query: string): {
      bind(...values: unknown[]): {
        run(): Promise<unknown>;
      };
      run(): Promise<unknown>;
    };
  };
}

const handler = {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== 'POST' || url.pathname !== '/seed') {
      return new Response('not found', { status: 404 });
    }

    await env.DB.prepare(
      `CREATE TABLE IF NOT EXISTS flowsafe_deployment (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        tenant_tag TEXT NOT NULL,
        provisioned_at TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `INSERT OR REPLACE INTO flowsafe_deployment
        (id, tenant_tag, provisioned_at)
       VALUES (1, ?, ?)`,
    )
      .bind('showcase', '2026-08-10T00:00:00.000Z')
      .run();
    return Response.json({ ok: true });
  },
};

export default handler;
