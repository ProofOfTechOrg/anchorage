// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import type { TenantContext } from '../approval-api/index.js';
import { mintThreadId, tenantOwnsMemoryId } from '../do-runner/index.js';
import {
  assertNoClientMemoryIds,
  requireOwnedMemoryId,
  TCB_ONLY_MEMORY_FIELDS,
} from './memory-boundary.js';
import { RunRouteError } from './run-route-error.js';

// The bound-context surface the guard reads. The real one comes from
// createTenantResolver (authenticate -> INV-3 -> bind); ownsMemoryId delegates
// to the PRODUCTION predicate rather than re-stating it, so the exactness pins
// below ride the real one — a hand-copied `startsWith` would keep passing if
// tenantOwnsMemoryId ever lost its trailing delimiter, which is the exact
// regression the acme/acmecorp case exists to catch.
function tenantContext(tenantId: string): TenantContext {
  return {
    tenantId,
    ownsMemoryId: (id: string) => tenantOwnsMemoryId(tenantId, id),
  } as unknown as TenantContext;
}

function statusOf(run: () => unknown): number | undefined {
  try {
    run();
  } catch (error) {
    return error instanceof RunRouteError ? error.status : undefined;
  }
  return undefined;
}

describe('assertNoClientMemoryIds', () => {
  it('400s a body naming a memory id — the client never picks whose memory it reads', () => {
    // #given / #when / #then
    for (const field of TCB_ONLY_MEMORY_FIELDS) {
      expect(
        statusOf(() => assertNoClientMemoryIds({ [field]: 'acme_t1' })),
      ).toBe(400);
    }
  });

  it('400s the id a client would MOST plausibly send: its own business key', () => {
    // #given — resourceId is business identity (a user id, an email), so the
    // natural client body carries it and the natural handler trusts it. That
    // is the leak; it must be refused, not silently salted.
    expect(() =>
      assertNoClientMemoryIds({ resourceId: 'user-1', prompt: 'hi' }),
    ).toThrow(/resourceId is server-assigned/);
  });

  it('400s a body carrying the KEY even with an undefined value', () => {
    // #given — `{ threadId: undefined }` is a caller that believes it may
    // choose; tolerating it leaves one truthiness bug between here and a leak
    expect(
      statusOf(() => assertNoClientMemoryIds({ threadId: undefined })),
    ).toBe(400);
  });

  it('passes a body that names no memory id', () => {
    // #given / #when / #then — the server mints; the body carries payload only
    expect(() =>
      assertNoClientMemoryIds({ prompt: 'hi', resource: 'not-an-id-field' }),
    ).not.toThrow();
  });

  it('ignores non-object bodies (they name no field; SHAPE is the route’s own schema business)', () => {
    // #given / #when / #then
    for (const body of [null, undefined, 'threadId', 42, ['threadId']]) {
      expect(() => assertNoClientMemoryIds(body)).not.toThrow();
    }
  });

  it('400s a NESTED memory id — the shape the first real consumer actually sends', () => {
    // #given — an agent run starts through `POST /runs` as
    // `{workflowId, inputData}`, so the natural body for a threaded run nests
    // the id one level down. A top-level-only check passes exactly the request
    // this guard exists to refuse.
    expect(
      statusOf(() =>
        assertNoClientMemoryIds({
          workflowId: 'durable-agentic-loop',
          inputData: { threadId: 'acme_t1', prompt: 'hi' },
        }),
      ),
    ).toBe(400);
  });

  it('400s a memory id nested inside an array element', () => {
    // #given — arrays are a body shape too; walking objects only would leave
    // `{messages: [{resourceId}]}` through
    expect(
      statusOf(() =>
        assertNoClientMemoryIds({ messages: [{ resourceId: 'user-1' }] }),
      ),
    ).toBe(400);
  });

  it('catches an id at ANY depth without blowing the stack', () => {
    // #given — depth is the attacker's choice, so a cap would be a bypass with
    // a number on it: nest one level past it and the id sails through. 10k deep
    // also proves the walk does not recurse (that body would overflow a stack).
    let deep: Record<string, unknown> = { threadId: 'acme_t1' };
    for (let index = 0; index < 10_000; index += 1) deep = { nested: deep };

    // #when / #then — still refused, and no RangeError
    expect(statusOf(() => assertNoClientMemoryIds(deep))).toBe(400);
  });

  it('terminates on a cyclic in-process body', () => {
    // #given — a parsed JSON body cannot cycle, but a hand-built one can; the
    // walk must not spin forever if a route ever hands it one
    const cyclic: Record<string, unknown> = { topic: 'launch' };
    cyclic.self = cyclic;

    // #when / #then
    expect(() => assertNoClientMemoryIds(cyclic)).not.toThrow();
  });

  it('passes a legitimately nested payload that names no memory id', () => {
    // #given — the walk must not false-positive on ordinary run input
    expect(() =>
      assertNoClientMemoryIds({
        workflowId: 'wf',
        inputData: { topic: 'launch', tags: ['a', 'b'], meta: { depth: 3 } },
      }),
    ).not.toThrow();
  });
});

describe('requireOwnedMemoryId', () => {
  it('returns an owned id', () => {
    // #given
    const tenant = tenantContext('acme');
    const threadId = mintThreadId('acme', () => 't1');
    // #when / #then
    expect(requireOwnedMemoryId(tenant, threadId)).toBe('acme_t1');
  });

  it('404s — never 403 — a foreign id, so the route is no existence oracle', () => {
    // #given — a guessable business key: tenant B asks for A's resource
    const tenant = tenantContext('globex');
    const foreign = mintThreadId('acme', () => 't1');

    // #when / #then — 403 would confirm the id EXISTS
    expect(statusOf(() => requireOwnedMemoryId(tenant, foreign))).toBe(404);
    expect(() => requireOwnedMemoryId(tenant, foreign)).toThrow(
      /threadId not found/,
    );
  });

  it('is exact at the tenant boundary (the acme vs acmecorp pin)', () => {
    // #given
    const tenant = tenantContext('acme');
    // #when / #then
    expect(
      statusOf(() =>
        requireOwnedMemoryId(
          tenant,
          mintThreadId('acmecorp', () => 't1'),
        ),
      ),
    ).toBe(404);
  });

  it('404s a non-string id rather than trusting a typed signature', () => {
    // #given — the id arrives from a body/path at the boundary, so its type is
    // a claim, not a fact
    const tenant = tenantContext('acme');
    // #when / #then
    expect(
      statusOf(() =>
        requireOwnedMemoryId(tenant, undefined as unknown as string),
      ),
    ).toBe(404);
  });

  it('names the field it refused, so a resource read does not 404 about threads', () => {
    // #given
    const tenant = tenantContext('globex');
    // #when / #then
    expect(() =>
      requireOwnedMemoryId(tenant, 'acme_user-1', 'resourceId'),
    ).toThrow(/resourceId not found/);
  });
});
