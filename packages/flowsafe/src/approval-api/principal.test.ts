// SPDX-License-Identifier: Apache-2.0
// The trust assertion's own tests. `trustAutomationPrincipal` is the ONLY way
// into `createAsPrincipal` and `supersedeStaleAsPrincipal`, both of which
// replace the human role gate with a kind check — so if a vouched principal can
// change kind after the vouch, automation gains create authority a human
// `viewer` does not have.
//
// The assertions that carry the invariant — kind after mutation, every forged
// shape, and the provenance on subordinate events — drive the real
// ApprovalService, because the predicate agreeing with itself proves nothing
// about the entry that consumes it. The clone, freeze, and field-pick tests
// assert the vouch's own output directly, since that output IS their subject.
import { describe, expect, it } from 'vitest';

import type { ApprovalAuditEvent } from './contract.js';
import {
  AUTOMATED_PROJECTED_ROLE,
  assertExecutionPrincipal,
  type ExecutionPrincipal,
  isTrustedAutomationPrincipal,
  principalActor,
  TRUSTED_AUTOMATION,
  type TrustedAutomationPrincipal,
  trustAutomationPrincipal,
} from './principal.js';
import { ApprovalAuthzError, ApprovalService } from './service.js';
import { InMemoryApprovalStoreFactory } from './tenant-store.js';
import type { CreateApprovalInput } from './types.js';

const CREATE: CreateApprovalInput = {
  workflowId: 'wf',
  runId: 'acme_run-1',
  title: 'publish launch post',
};

function harness(): { service: ApprovalService; events: ApprovalAuditEvent[] } {
  const backend = new InMemoryApprovalStoreFactory();
  const events: ApprovalAuditEvent[] = [];
  return {
    service: new ApprovalService({
      store: backend.forTenant('acme'),
      audit: (event) => events.push(event),
    }),
    events,
  };
}

function systemSource(): Record<string, unknown> {
  return {
    kind: 'system',
    id: 'flowsafe-system',
    tenantId: 'acme',
    purpose: 'approval-suspension-reconcile',
  };
}

/** Vouch a hand-built object, so a test can hold the pre-vouch reference. */
function vouchRaw(source: Record<string, unknown>): TrustedAutomationPrincipal {
  return trustAutomationPrincipal(source as unknown as ExecutionPrincipal);
}

describe('trustAutomationPrincipal', () => {
  it('refuses a human and a malformed principal', () => {
    // #given / #when / #then — these must use the role-authorized entries.
    expect(() =>
      trustAutomationPrincipal({
        kind: 'human',
        id: 'ada',
        tenantId: 'acme',
        role: 'admin',
      }),
    ).toThrow(/only a valid automated principal/);
    expect(() =>
      trustAutomationPrincipal({
        kind: 'system',
        id: 'sys',
        tenantId: 'acme',
        // Empty purpose — provenance is the point, so it is not optional.
        purpose: '   ',
      }),
    ).toThrow(/only a valid automated principal/);
  });

  it("returns a canonical clone, not the caller's reference", () => {
    // #given
    const source = systemSource();

    // #when
    const vouched = vouchRaw(source);

    // #then
    expect(vouched).not.toBe(source);
    expect(Object.isFrozen(vouched)).toBe(true);
    expect(
      (vouched as unknown as Record<symbol, unknown>)[TRUSTED_AUTOMATION],
    ).toBe(true);
  });

  it('drops properties the automated shape does not declare', () => {
    // #given — an attacker-supplied extra must not ride into the TCB, the same
    // reason decodeExecutionPrincipal picks fields explicitly.
    const source = { ...systemSource(), role: 'admin', injected: 'x' };

    // #when
    const vouched = vouchRaw(source);

    // #then
    expect(Object.keys(vouched).sort()).toEqual([
      'id',
      'kind',
      'purpose',
      'tenantId',
    ]);
  });

  it("keeps an agent's delegatedBy and adds it to no other kind", () => {
    // #given / #when
    const agent = trustAutomationPrincipal({
      kind: 'agent',
      id: 'planner',
      tenantId: 'acme',
      purpose: 'delegated-subtask',
      delegatedBy: 'supervisor',
    });
    const system = trustAutomationPrincipal({
      kind: 'system',
      id: 'sys',
      tenantId: 'acme',
      purpose: 'bookkeeping',
    });

    // #then
    expect(agent).toMatchObject({ delegatedBy: 'supervisor' });
    expect('delegatedBy' in system).toBe(false);
  });

  it('survives mutation of the object it was vouched from', async () => {
    // #given — the exact defect: validate once, hand the caller's own object
    // back, and the caller rewrites it into a human admin before the service
    // reads `kind`.
    const source = systemSource();
    const vouched = vouchRaw(source);

    // #when
    source.kind = 'human';
    source.role = 'admin';
    source.purpose = undefined;

    // #then — the vouched principal is unmoved, and the service still sees the
    // system principal it authorized.
    expect(vouched.kind).toBe('system');
    expect(isTrustedAutomationPrincipal(vouched)).toBe(true);

    const { service, events } = harness();
    const { record } = await service.createAsPrincipal(CREATE, vouched);
    expect(record.status).toBe('pending');
    expect(
      events.find((event) => event.action === 'approval.create')?.detail,
    ).toMatchObject({
      principalKind: 'system',
      principalId: 'flowsafe-system',
      purpose: 'approval-suspension-reconcile',
    });
  });

  it('cannot be mutated in place after vouching', () => {
    // #given
    const vouched = vouchRaw(systemSource());

    // #when — frozen, and this module is ESM (always strict), so the write
    // throws rather than silently no-op'ing.
    expect(() => {
      (vouched as unknown as Record<string, unknown>).kind = 'human';
    }).toThrow(TypeError);

    // #then
    expect(vouched.kind).toBe('system');
  });

  // A two-trap Proxy: `getOwnPropertyDescriptor` — the channel the validator
  // reads — reports one principal, while `get`, the channel any re-read uses,
  // reports another. Over an extensible target neither trap is constrained, so
  // no amount of checking makes a SECOND read trustworthy. Only returning the
  // values that were actually validated closes it.
  const VALIDATED = {
    kind: 'agent',
    id: 'planner',
    tenantId: 'acme',
    purpose: 'delegated-subtask',
  };
  const twoFaced = (lie: Record<string, unknown>): ExecutionPrincipal =>
    new Proxy({} as Record<string, unknown>, {
      getOwnPropertyDescriptor: (_target, key) =>
        typeof key === 'string' && key in VALIDATED
          ? {
              value: VALIDATED[key as keyof typeof VALIDATED],
              writable: true,
              enumerable: true,
              configurable: true,
            }
          : undefined,
      get: (_target, key) => (typeof key === 'string' ? lie[key] : undefined),
      ownKeys: () => Object.keys(VALIDATED),
    }) as unknown as ExecutionPrincipal;

  it('mints from the values it validated, not from a second read', () => {
    // #given
    const principal = twoFaced({
      kind: 'system',
      id: 'ghost-admin',
      tenantId: 'globex',
      purpose: 'x'.repeat(5000),
    });

    // #when
    const minted = trustAutomationPrincipal(principal);

    // #then — the vouched principal is the one that passed the checks: the
    // tenant it was validated against, and a purpose inside the bound.
    expect(minted).toMatchObject(VALIDATED);
    expect(minted.tenantId).not.toBe('globex');
    expect(minted.purpose).toHaveLength('delegated-subtask'.length);
  });

  it('binds the tenant it compared, and returns that same snapshot', () => {
    // #given — the wire channel claims a human admin. If assert returned its
    // argument, the caller would encode and project THAT, not what it checked.
    const principal = twoFaced({
      kind: 'human',
      id: 'ghost-admin',
      tenantId: 'acme',
      role: 'admin',
    });

    // #when
    const asserted = assertExecutionPrincipal(principal, 'acme', 'probe');

    // #then
    expect(asserted).toEqual(VALIDATED);
    expect(principalActor(asserted)).toEqual({
      id: 'planner',
      role: AUTOMATED_PROJECTED_ROLE,
      tenantId: 'acme',
    });
  });

  it.each([
    ['a literal empty purpose', ''],
    ['a control character in the purpose', 'reconcile\u0007drop'],
    ['a purpose one over the bound', 'p'.repeat(201)],
  ])('refuses %s', (_label, purpose) => {
    // #given / #when / #then — the bounds are what keep an audit row bounded
    // and a header assembly total; refusing here beats a TypeError deep in the
    // topology.
    expect(() => vouchRaw({ ...systemSource(), purpose })).toThrow(
      /only a valid automated principal/,
    );
  });

  it('accepts a purpose exactly at the bound', () => {
    // #given / #when — 200 is the documented maximum, so it must be inclusive.
    const vouched = vouchRaw({ ...systemSource(), purpose: 'p'.repeat(200) });

    // #then
    expect(vouched.purpose).toHaveLength(200);
  });
});

describe('automated service entries reject an unvouched principal', () => {
  // Every case type-asserts its way past the signature, which is exactly what
  // the runtime check exists to catch: the parameter type is erased, so an `as`
  // cast or a value rebuilt from storage arrives typed correctly.
  //
  // All but the last are FROZEN on purpose. An unfrozen fixture is denied by the
  // freeze clause alone, which would leave every other clause — kind, own-brand,
  // strict-true — passing untested and deletable while the suite stayed green.
  // Freezing isolates each fixture on the one clause it names.
  const forge = (value: object): TrustedAutomationPrincipal =>
    Object.freeze(value) as TrustedAutomationPrincipal;

  const cases: Array<[string, TrustedAutomationPrincipal]> = [
    ['an unbranded object literal', forge(systemSource())],
    [
      'a forged brand on a human principal',
      forge({
        kind: 'human',
        id: 'ada',
        tenantId: 'acme',
        role: 'admin',
        [TRUSTED_AUTOMATION]: true,
      }),
    ],
    [
      'a forged brand on a principal with no purpose',
      forge({
        kind: 'system',
        id: 'sys',
        tenantId: 'acme',
        [TRUSTED_AUTOMATION]: true,
      }),
    ],
    [
      'a truthy-but-not-true brand',
      forge({ ...systemSource(), [TRUSTED_AUTOMATION]: 1 }),
    ],
    [
      // Plain property access walks the prototype chain, so this would pass a
      // naive brand check without anyone stamping the object itself.
      'a brand inherited from a prototype',
      forge(
        Object.assign(
          Object.create({ [TRUSTED_AUTOMATION]: true }),
          systemSource(),
        ),
      ),
    ],
    [
      // Frozen, branded, and every getter currently answers CORRECTLY — and it
      // is still refused. Freeze pins a data property's value but does nothing
      // to an accessor, so a getter may answer differently on the next read;
      // the trusted entries read a principal four times in one call (shape,
      // tenant compare, principalActor, principalAuditFields). Nothing can
      // prove a getter that tells the truth now will tell it then, so the shape
      // is refused outright rather than sampled.
      'a frozen object whose fields are own getters',
      forge({
        [TRUSTED_AUTOMATION]: true,
        id: 'sys',
        tenantId: 'acme',
        get kind() {
          return 'system';
        },
        get purpose() {
          return 'p';
        },
      }),
    ],
    [
      // Every OWN property is honest data; `kind` and `purpose` come from the
      // prototype. A shape check over Reflect.ownKeys never sees them, and
      // Object.freeze does not constrain them — so only reading fields as own
      // data properties refuses this.
      'a frozen object whose fields are inherited getters',
      forge(
        Object.assign(
          Object.create({
            get kind() {
              return 'system';
            },
            get purpose() {
              return 'p';
            },
          }),
          { [TRUSTED_AUTOMATION]: true, id: 'sneaky-bot', tenantId: 'acme' },
        ),
      ),
    ],
    [
      // Same gap, delivered by a Proxy: the target is genuinely frozen and
      // genuinely owns only data properties, while `kind`/`purpose` are served
      // by a `get` trap for keys the target does not own — which carries no
      // specification invariant at all.
      'a proxy serving fields the frozen target does not own',
      new Proxy(
        Object.freeze({
          [TRUSTED_AUTOMATION]: true,
          id: 'sneaky-bot',
          tenantId: 'acme',
        }),
        {
          get: (target, key, receiver) =>
            key === 'kind'
              ? 'system'
              : key === 'purpose'
                ? 'p'
                : Reflect.get(target, key, receiver),
        },
      ) as unknown as TrustedAutomationPrincipal,
    ],
    [
      // The shape a rushed fix produces: stamp the brand, skip the minter, and
      // keep holding a live mutable reference. The one case whose subject IS
      // the freeze clause, so it is deliberately left unfrozen.
      'a branded but unfrozen object',
      {
        ...systemSource(),
        [TRUSTED_AUTOMATION]: true,
      } as unknown as TrustedAutomationPrincipal,
    ],
  ];

  it.each(cases)('denies create with %s', async (_label, principal) => {
    // #given
    const { service, events } = harness();

    // #when / #then
    await expect(service.createAsPrincipal(CREATE, principal)).rejects.toThrow(
      ApprovalAuthzError,
    );
    expect(events).toMatchObject([
      {
        action: 'approval.create',
        decision: 'denied',
        actor: null,
        reason: expect.stringContaining('not a vouched automated principal'),
      },
    ]);
  });

  it.each(cases)('denies supersede with %s', async (_label, principal) => {
    // #given
    const { service } = harness();

    // #when / #then
    await expect(
      service.supersedeStaleAsPrincipal('any-id', principal, 'stale'),
    ).rejects.toThrow(ApprovalAuthzError);
  });

  it('checks the brand before the tenant, so an unvouched principal never reaches the tenant branch', async () => {
    // #given — both checks would deny, so only the reason distinguishes which
    // ran. Ordering matters: the tenant branch calls principalActor(), which
    // reads the very fields an unvouched value has not earned trust for.
    const { service, events } = harness();
    const unvouchedAndForeign = {
      ...systemSource(),
      tenantId: 'globex',
    } as unknown as TrustedAutomationPrincipal;

    // #when / #then
    await expect(
      service.createAsPrincipal(CREATE, unvouchedAndForeign),
    ).rejects.toThrow(ApprovalAuthzError);
    expect(events).toMatchObject([
      {
        decision: 'denied',
        actor: null,
        reason: expect.stringContaining('not a vouched automated principal'),
      },
    ]);
    expect(events[0]?.reason).not.toContain('does not match the store binding');
  });

  it('denies a vouched principal bound to another tenant, with provenance', async () => {
    // #given — a real vouch, wrong tenant: the wiring-bug case that must fail
    // closed rather than act cross-tenant.
    const { service, events } = harness();
    const foreign = trustAutomationPrincipal({
      kind: 'system',
      id: 'flowsafe-system',
      tenantId: 'globex',
      purpose: 'approval-suspension-reconcile',
    });

    // #when / #then
    await expect(service.createAsPrincipal(CREATE, foreign)).rejects.toThrow(
      /tenant does not match/,
    );
    expect(events).toMatchObject([
      {
        action: 'approval.create',
        decision: 'denied',
        // A denial is an automated event too — it carries the same provenance
        // the allowed path carries.
        detail: {
          principalKind: 'system',
          principalId: 'flowsafe-system',
          purpose: 'approval-suspension-reconcile',
        },
      },
    ]);
  });
});

describe('automated provenance reaches subordinate audit events', () => {
  it('carries the principal onto a notification failure', async () => {
    // #given — the notify sink throws, so the only row describing this failure
    // is the subordinate one.
    const backend = new InMemoryApprovalStoreFactory();
    const events: ApprovalAuditEvent[] = [];
    const service = new ApprovalService({
      store: backend.forTenant('acme'),
      audit: (event) => events.push(event),
      notify: () => {
        throw new Error('sink down');
      },
    });
    const principal = trustAutomationPrincipal({
      kind: 'agent',
      id: 'planner',
      tenantId: 'acme',
      purpose: 'delegated-subtask',
      delegatedBy: 'supervisor',
    });

    // #when
    await service.createAsPrincipal(CREATE, principal);

    // #then
    expect(
      events.find((event) => event.action === 'approval.notify'),
    ).toMatchObject({
      decision: 'error',
      detail: {
        tenantId: 'acme',
        principalKind: 'agent',
        principalId: 'planner',
        purpose: 'delegated-subtask',
        delegatedBy: 'supervisor',
      },
    });
  });

  it('carries the principal onto a supersede stream failure', async () => {
    // #given
    const backend = new InMemoryApprovalStoreFactory();
    const events: ApprovalAuditEvent[] = [];
    const service = new ApprovalService({
      store: backend.forTenant('acme'),
      audit: (event) => events.push(event),
      stream: () => {
        throw new Error('hub down');
      },
    });
    const principal = trustAutomationPrincipal({
      kind: 'system',
      id: 'flowsafe-system',
      tenantId: 'acme',
      purpose: 'approval-suspension-reconcile',
    });
    const { record } = await service.createAsPrincipal(CREATE, principal);

    // #when
    await service.supersedeStaleAsPrincipal(
      record.id,
      principal,
      'fingerprint moved',
    );

    // #then
    const streamFailures = events.filter(
      (event) => event.action === 'approval.stream',
    );
    expect(streamFailures.at(-1)).toMatchObject({
      decision: 'error',
      detail: {
        tenantId: 'acme',
        principalKind: 'system',
        principalId: 'flowsafe-system',
        purpose: 'approval-suspension-reconcile',
      },
    });
  });
});
