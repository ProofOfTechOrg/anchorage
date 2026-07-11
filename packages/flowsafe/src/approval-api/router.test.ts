// SPDX-License-Identifier: Apache-2.0
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
import { approvalCursor } from './types.js';

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

  it('paginates with limit and after, and 400s an out-of-range limit', async () => {
    // #given — 3 records
    const { handle } = makeHandler({ allowCreate: true });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-p1' });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-p2' });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-p3' });

    // #when — page 1 (limit 2)
    const page1Response = await handle(req('/api/approvals?limit=2'));
    const page1 = (await page1Response?.json()) as ApprovalRecord[];

    // #then
    expect(page1Response?.status).toBe(200);
    expect(page1).toHaveLength(2);

    // #when — page 2 via the cursor derived from page 1's last record
    const cursor = approvalCursor(page1[1] as ApprovalRecord);
    const page2Response = await handle(
      req(`/api/approvals?limit=2&after=${encodeURIComponent(cursor)}`),
    );

    // #then
    expect(page2Response?.status).toBe(200);
    expect(((await page2Response?.json()) as ApprovalRecord[]).length).toBe(1);

    // #then — garbage limit values 400
    expect((await handle(req('/api/approvals?limit=0')))?.status).toBe(400);
    expect((await handle(req('/api/approvals?limit=abc')))?.status).toBe(400);
    expect((await handle(req('/api/approvals?limit=1.5')))?.status).toBe(400);
    expect((await handle(req('/api/approvals?limit=501')))?.status).toBe(400);
  });

  it('lists in reviewer order when orderBy=reviewer — the bounded page is the top of the queue', async () => {
    // #given — three normals then a critical, the shape of the 2026-07-11
    // review finding: a FIFO-then-limit page hid the critical arrival
    const { handle } = makeHandler({ allowCreate: true });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-o1' });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-o2' });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-o3' });
    const critical = await createOne(handle, {
      ...CREATE_BODY,
      runId: 'acme_run-hot',
      priority: 'critical',
    });

    // #when
    const response = await handle(
      req('/api/approvals?orderBy=reviewer&limit=2'),
    );
    const page = (await response?.json()) as ApprovalRecord[];

    // #then
    expect(response?.status).toBe(200);
    expect(page).toHaveLength(2);
    expect(page[0]?.id).toBe(critical.id);
  });

  it('composes status + orderBy=reviewer + limit — reviewer-ranked, status-filtered, then bounded', async () => {
    // #given — two normals, a critical created LAST, and a fourth we approve so
    // it leaves the open set
    const { handle } = makeHandler({ allowCreate: true });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-c1' });
    await createOne(handle, { ...CREATE_BODY, runId: 'acme_run-c2' });
    const critical = await createOne(handle, {
      ...CREATE_BODY,
      runId: 'acme_run-chot',
      priority: 'critical',
    });
    const decided = await createOne(handle, {
      ...CREATE_BODY,
      runId: 'acme_run-cdone',
    });
    const decideResponse = await handle(
      req(`/api/approvals/${decided.id}/decide`, {
        body: { decision: 'approve' },
        actor: { id: 'ray', role: 'reviewer' },
      }),
    );
    expect(decideResponse?.status).toBe(200);

    // #when — all three query params at once
    const response = await handle(
      req(
        '/api/approvals?status=pending,claimed,escalated&orderBy=reviewer&limit=2',
      ),
    );
    const page = (await response?.json()) as ApprovalRecord[];

    // #then — reviewer order ranks the critical to the top BEFORE limit cuts to
    // two, and the status filter keeps the approved record out of the page
    expect(response?.status).toBe(200);
    expect(page).toHaveLength(2);
    expect(page[0]?.id).toBe(critical.id);
    expect(
      page.every((record) =>
        ['pending', 'claimed', 'escalated'].includes(record.status),
      ),
    ).toBe(true);
  });

  it('400s an unknown orderBy and the incoherent orderBy=reviewer + after combination', async () => {
    // #given
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);
    const cursor = approvalCursor(record);

    // #when / #then — unknown value
    expect((await handle(req('/api/approvals?orderBy=bogus')))?.status).toBe(
      400,
    );
    // #when / #then — cursors page FIFO order only; the combination must be
    // a 400 at the boundary, not a store throw mapped to 500
    expect(
      (
        await handle(
          req(
            `/api/approvals?orderBy=reviewer&after=${encodeURIComponent(cursor)}`,
          ),
        )
      )?.status,
    ).toBe(400);
  });

  it('400s a malformed after cursor', async () => {
    // #given
    const { handle } = makeHandler();

    // #when / #then — invalid base64
    expect(
      (await handle(req('/api/approvals?after=not%20valid%20base64!!')))
        ?.status,
    ).toBe(400);
    // #when / #then — valid base64, wrong shape (no delimiter)
    expect(
      (await handle(req(`/api/approvals?after=${btoa('no-delimiter-here')}`)))
        ?.status,
    ).toBe(400);
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

describe('createApprovalRouter triage filters', () => {
  it('passes requestedBy through to the list filter', async () => {
    // #given — HTTP create force-attributes requestedBy to the actor ('opal')
    const { handle } = makeHandler({ allowCreate: true });
    await createOne(handle);

    // #when
    const match = await handle(req('/api/approvals?requestedBy=opal'));
    const miss = await handle(req('/api/approvals?requestedBy=ada'));

    // #then
    expect((await match?.json()) as ApprovalRecord[]).toHaveLength(1);
    expect((await miss?.json()) as ApprovalRecord[]).toHaveLength(0);
  });

  it('passes createdBefore/createdAfter through as strict time bounds', async () => {
    // #given — bounds far either side of the record's real clock stamp
    const { handle } = makeHandler({ allowCreate: true });
    await createOne(handle);

    // #when / #then
    const noneBefore = await handle(
      req('/api/approvals?createdBefore=1990-01-01T00:00:00Z'),
    );
    expect((await noneBefore?.json()) as ApprovalRecord[]).toHaveLength(0);
    const allAfter = await handle(
      req('/api/approvals?createdAfter=1990-01-01T00:00:00Z'),
    );
    expect((await allAfter?.json()) as ApprovalRecord[]).toHaveLength(1);
  });

  it.each([
    'createdBefore',
    'createdAfter',
  ])('400s an unparseable %s eagerly', async (field) => {
    // #given
    const { handle } = makeHandler();

    // #when
    const response = await handle(req(`/api/approvals?${field}=yesterday-ish`));

    // #then
    expect(response?.status).toBe(400);
    expect(await response?.json()).toMatchObject({
      error: expect.stringContaining(field),
    });
  });
});

describe('createApprovalRouter batch decide', () => {
  const reviewer = { id: 'ray', role: 'reviewer' };

  it('mounts POST /batch/decide always (it is decide fan-out, not create) and reports the envelope', async () => {
    // #given — seed over HTTP with create enabled, then batch on a handler
    // built the same way (allowCreate does not gate the batch route; both
    // handlers here share nothing, so seed and decide use the same one)
    const { handle } = makeHandler({ allowCreate: true });
    const record = await createOne(handle);

    // #when — one real id, one unknown
    const response = await handle(
      req('/api/approvals/batch/decide', {
        body: { ids: [record.id, 'missing'], decision: 'approve' },
        actor: reviewer,
      }),
    );

    // #then — 200 with partial failure IN the envelope
    expect(response?.status).toBe(200);
    const envelope = (await response?.json()) as {
      results: Array<{ id: string; ok: boolean; code?: string }>;
      decided: number;
      failed: number;
    };
    expect(envelope.decided).toBe(1);
    expect(envelope.failed).toBe(1);
    expect(envelope.results).toEqual([
      expect.objectContaining({ id: record.id, ok: true }),
      expect.objectContaining({ id: 'missing', ok: false, code: 'not-found' }),
    ]);
  });

  it('is reachable when the create route is OFF (default posture)', async () => {
    // #given
    const { handle } = makeHandler();

    // #when — empty queue, unknown id: proves routing, not creation
    const response = await handle(
      req('/api/approvals/batch/decide', {
        body: { ids: ['missing'], decision: 'reject' },
        actor: reviewer,
      }),
    );

    // #then
    expect(response?.status).toBe(200);
    const envelope = (await response?.json()) as { failed: number };
    expect(envelope.failed).toBe(1);
  });

  it('403s the whole batch for a non-reviewer role', async () => {
    // #given
    const { handle } = makeHandler();

    // #when
    const response = await handle(
      req('/api/approvals/batch/decide', {
        body: { ids: ['any'], decision: 'approve' },
        actor: { id: 'vic', role: 'viewer' },
      }),
    );

    // #then
    expect(response?.status).toBe(403);
  });

  it('400s a malformed batch body', async () => {
    // #given
    const { handle } = makeHandler();

    // #when — ids not an array
    const notArray = await handle(
      req('/api/approvals/batch/decide', {
        body: { ids: 'oops', decision: 'approve' },
        actor: reviewer,
      }),
    );
    const emptyIds = await handle(
      req('/api/approvals/batch/decide', {
        body: { ids: [], decision: 'approve' },
        actor: reviewer,
      }),
    );
    const badDecision = await handle(
      req('/api/approvals/batch/decide', {
        body: { ids: ['x'], decision: 'maybe' },
        actor: reviewer,
      }),
    );

    // #then
    expect(notArray?.status).toBe(400);
    expect(emptyIds?.status).toBe(400);
    expect(badDecision?.status).toBe(400);
  });
});
