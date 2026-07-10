import { describe, expect, it } from 'vitest';

import type { ApprovalRole } from './contract.js';
import {
  CLIENT_CREATE_FIELDS,
  createApprovalRouter,
  TCB_ONLY_CREATE_FIELDS,
} from './router.js';
import { ApprovalService } from './service.js';
import { createTenantResolver } from './tenant-context.js';
import { InMemoryApprovalStoreFactory } from './tenant-store.js';
import type { ApprovalRecord, CreateApprovalInput } from './types.js';

// Header-based test authenticator behind the real TenantResolver (production
// wires sessions/JWTs into the same seam). The x-actor-tenant header defaults
// to 'acme' so single-tenant fixtures stay terse. allowCreate is opt-in (the
// route is off by default), so every fixture that exercises create passes it
// explicitly — mirroring the posture a host must choose deliberately.
function makeHandler(options: { allowCreate?: boolean } = {}) {
  const backend = new InMemoryApprovalStoreFactory();
  const store = backend.forTenant('acme');
  const resolve = createTenantResolver({
    authenticate: (request) => {
      const id = request.headers.get('x-actor-id');
      const role = request.headers.get('x-actor-role');
      const tenantId = request.headers.get('x-actor-tenant') ?? 'acme';
      return id && role
        ? { id, role: role as ApprovalRole, tenantId }
        : undefined;
    },
    storeFactory: backend,
    buildService: (boundStore) => new ApprovalService({ store: boundStore }),
  });
  return {
    store,
    handle: createApprovalRouter({
      resolve,
      allowCreate: options.allowCreate,
    }),
  };
}

interface ReqOptions {
  method?: string;
  body?: unknown;
  actor?: { id: string; role: string } | null;
}

function req(path: string, options: ReqOptions = {}): Request {
  const headers = new Headers();
  const actor =
    options.actor === undefined
      ? { id: 'opal', role: 'operator' }
      : options.actor;
  if (actor) {
    headers.set('x-actor-id', actor.id);
    headers.set('x-actor-role', actor.role);
  }
  let body: string | undefined;
  if (options.body !== undefined) {
    headers.set('content-type', 'application/json');
    body =
      typeof options.body === 'string'
        ? options.body
        : JSON.stringify(options.body);
  }
  return new Request(`http://queue.test${path}`, {
    method: options.method ?? (options.body === undefined ? 'GET' : 'POST'),
    headers,
    body,
  });
}

// No `connectors` — the HTTP create route rejects every capability-bearing
// field (TCB_ONLY_CREATE_FIELDS), so an HTTP-filed request is inert by
// construction: it can never mint a grant.
const CREATE_BODY = {
  workflowId: 'wf',
  runId: 'acme_run-1',
  title: 'publish launch post',
  slaSeconds: 3600,
};

async function createOne(
  handle: ReturnType<typeof makeHandler>['handle'],
  body: Record<string, unknown> = CREATE_BODY,
): Promise<ApprovalRecord> {
  const response = await handle(req('/api/approvals', { body }));
  expect(response?.status).toBe(201);
  return (await response?.json()) as ApprovalRecord;
}

describe('createApprovalRouter', () => {
  it('returns null for paths outside its base', async () => {
    // #given
    const { handle } = makeHandler();

    // #when / #then — host worker composes its own routes after
    expect(await handle(req('/runs/wf/r1'))).toBeNull();
    expect(await handle(req('/api/approvalsandmore'))).toBeNull();
  });

  it('rejects unauthenticated requests with 401', async () => {
    // #given
    const { handle } = makeHandler();

    // #when
    const response = await handle(req('/api/approvals', { actor: null }));

    // #then
    expect(response?.status).toBe(401);
  });

  it('404s the create route by default', async () => {
    // #given — allowCreate is opt-in: records are created in-process by the
    // suspend bridge in every first-party host, so the HTTP route stays off
    const { handle } = makeHandler();

    // #when
    const response = await handle(req('/api/approvals', { body: CREATE_BODY }));

    // #then
    expect(response?.status).toBe(404);
  });

  it.each(
    TCB_ONLY_CREATE_FIELDS,
  )('400s when the body supplies %s', async (field) => {
    // #given — every one of these selects capability or attribution:
    // `connectors` IS the minted grant, `requestedBy` is what the
    // separation-of-duties check compares, and stepPath + the binding pair
    // choose which leg a grant mints on
    const { handle } = makeHandler({ allowCreate: true });

    // #when
    const response = await handle(
      req('/api/approvals', {
        body: { ...CREATE_BODY, [field]: 'anything' },
      }),
    );

    // #then
    expect(response?.status).toBe(400);
    expect(await response?.json()).toMatchObject({
      error: `${field} may not be set over HTTP`,
    });
  });

  it('classifies every CreateApprovalInput field exactly once — new fields cannot slip past the allowlist', () => {
    // #given — the type-level pin: an unclassified field makes `true` not
    // assignable to the union below, so this file stops COMPILING until the
    // new field is deliberately sorted into CLIENT_CREATE_FIELDS or
    // TCB_ONLY_CREATE_FIELDS. That is the fail-closed inversion of the old
    // body spread, where a forgotten denylist entry silently granted.
    type Classified =
      | (typeof CLIENT_CREATE_FIELDS)[number]
      | (typeof TCB_ONLY_CREATE_FIELDS)[number];
    type Unclassified = Exclude<keyof CreateApprovalInput, Classified>;
    const allClassified: [Unclassified] extends [never] ? true : Unclassified =
      true;

    // #then — and the two lists never overlap (a field in both would 400)
    expect(allClassified).toBe(true);
    const overlap = CLIENT_CREATE_FIELDS.filter((field) =>
      (TCB_ONLY_CREATE_FIELDS as readonly string[]).includes(field),
    );
    expect(overlap).toEqual([]);
  });

  it('drops a body field that is in NEITHER list instead of forwarding it to service.create', async () => {
    // #given — the allowlist pick: a future CreateApprovalInput field that
    // nobody classified must be inert over HTTP, not flow through a spread
    const { handle } = makeHandler({ allowCreate: true });

    // #when
    const record = await createOne(handle, {
      ...CREATE_BODY,
      futureCapabilityField: 'attacker-controlled',
    });

    // #then — created (no 400: only TCB names reject), but the field never
    // reached the record
    expect(record).not.toHaveProperty('futureCapabilityField');
  });

  it('copies every allowlisted optional field through', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });

    // #when
    const record = await createOne(handle, {
      ...CREATE_BODY,
      summary: 'a human summary',
      priority: 'high',
      payload: { context: 42 },
    });

    // #then
    expect(record).toMatchObject({
      summary: 'a human summary',
      priority: 'high',
      payload: { context: 42 },
    });
  });

  it('forces requestedBy to the authenticated actor', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });

    // #when
    const response = await handle(
      req('/api/approvals', {
        body: CREATE_BODY,
        actor: { id: 'opal', role: 'operator' },
      }),
    );

    // #then — attribution is the authenticated identity, never the body
    expect(response?.status).toBe(201);
    expect((await response?.json()) as ApprovalRecord).toMatchObject({
      requestedBy: 'opal',
    });
  });

  it('never authors capability: an HTTP-created record has no connectors and no run scope', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });

    // #when
    const record = await createOne(handle);

    // #then — inert: nothing for the grant provider to mint from
    expect(record.connectors).toEqual([]);
    expect(record.runScoped).toBeUndefined();
    expect(record.stepPath).toBeUndefined();
  });

  it('creates with 201 and collapses duplicates to 200', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });

    // #when
    const record = await createOne(handle);
    const duplicate = await handle(
      req('/api/approvals', { body: CREATE_BODY }),
    );

    // #then
    expect(record).toMatchObject({ status: 'pending', workflowId: 'wf' });
    expect(duplicate?.status).toBe(200);
    expect(((await duplicate?.json()) as ApprovalRecord).id).toBe(record.id);
  });

  it('lists with status filters and rejects unknown statuses', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    await createOne(handle);

    // #when
    const listed = await handle(req('/api/approvals?status=pending,claimed'));
    const invalid = await handle(req('/api/approvals?status=bogus'));

    // #then
    expect(listed?.status).toBe(200);
    expect((await listed?.json()) as ApprovalRecord[]).toHaveLength(1);
    expect(invalid?.status).toBe(400);
  });

  it('serves metrics at the literal segment, not as an id', async () => {
    // #given
    const { handle } = makeHandler();

    // #when
    const response = await handle(req('/api/approvals/metrics'));

    // #then
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({ openCount: 0 });
  });

  it('gets a record by id and 404s unknown ids', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);

    // #when / #then
    const found = await handle(req(`/api/approvals/${record.id}`));
    expect(found?.status).toBe(200);
    const missing = await handle(req('/api/approvals/nope'));
    expect(missing?.status).toBe(404);
  });

  it('claims, then maps the double-claim conflict to 409 with the current status', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);
    const reviewer = { id: 'ray', role: 'reviewer' };

    // #when
    const claim = await handle(
      req(`/api/approvals/${record.id}/claim`, {
        method: 'POST',
        actor: reviewer,
      }),
    );
    const again = await handle(
      req(`/api/approvals/${record.id}/claim`, {
        method: 'POST',
        actor: reviewer,
      }),
    );

    // #then
    expect(claim?.status).toBe(200);
    expect(again?.status).toBe(409);
    expect(await again?.json()).toMatchObject({ currentStatus: 'claimed' });
  });

  it('maps role denials to 403', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);

    // #when — a viewer tries to decide
    const response = await handle(
      req(`/api/approvals/${record.id}/decide`, {
        body: { decision: 'approve' },
        actor: { id: 'vic', role: 'viewer' },
      }),
    );

    // #then
    expect(response?.status).toBe(403);
  });

  it('decides with a comment and returns the resume outcome', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);

    // #when
    const response = await handle(
      req(`/api/approvals/${record.id}/decide`, {
        body: { decision: 'approve', comment: 'lgtm' },
        actor: { id: 'ray', role: 'reviewer' },
      }),
    );

    // #then
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      record: { status: 'approved', comment: 'lgtm' },
      resume: { attempted: false },
    });
  });

  it('rejects malformed JSON bodies with 400', async () => {
    // #given — the only coverage of readJsonObject's parse path
    const { handle } = makeHandler({ allowCreate: true });

    // #when
    const response = await handle(
      req('/api/approvals', { method: 'POST', body: '{not json' }),
    );

    // #then
    expect(response?.status).toBe(400);
  });

  it('delegates via POST', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);

    // #when
    const response = await handle(
      req(`/api/approvals/${record.id}/delegate`, {
        body: { to: 'quinn' },
        actor: { id: 'ray', role: 'reviewer' },
      }),
    );

    // #then
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({ claimedBy: 'quinn' });
  });

  it('404s POST /sla/sweep — the sweep is cron-owned TCB code, not an endpoint', async () => {
    // #given — the route USED to exist and was an unfiltered cross-tenant
    // read+write behind a role check; it must never come back
    const { handle } = makeHandler();

    // #when
    const response = await handle(
      req('/api/approvals/sla/sweep', { method: 'POST' }),
    );

    // #then
    expect(response?.status).toBe(404);
  });

  it('404s unknown subroutes', async () => {
    // #given
    const { handle } = makeHandler();

    // #when
    const response = await handle(
      req('/api/approvals/x/y/z', { method: 'POST' }),
    );

    // #then
    expect(response?.status).toBe(404);
  });

  it('404s a wrong HTTP method on an otherwise valid path', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);

    // #when — DELETE on a real record
    const deleted = await handle(
      req(`/api/approvals/${record.id}`, { method: 'DELETE' }),
    );

    // #then — no method falls through to a mutation
    expect(deleted?.status).toBe(404);
  });

  it('treats a trailing slash on the base as a list, and on an id as a get', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);

    // #when
    const listed = await handle(req('/api/approvals/'));
    const got = await handle(req(`/api/approvals/${record.id}/`));

    // #then — empty trailing segments are ignored, not treated as an empty id
    expect(listed?.status).toBe(200);
    expect((await listed?.json()) as ApprovalRecord[]).toHaveLength(1);
    expect(got?.status).toBe(200);
    expect(((await got?.json()) as ApprovalRecord).id).toBe(record.id);
  });
});
