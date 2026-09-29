// SPDX-License-Identifier: Apache-2.0
import { RequestContext } from '@mastra/core/request-context';
import { describe, expect, it } from 'vitest';

import {
  CONNECTOR_DECISIONS,
  connectorDecisionRetryable,
} from '../connector-decision.js';
import type { PermissionManifest } from '../connector-sdk/index.js';
import {
  approvalRequired,
  backgroundExecution,
  crossWorkflowIsolation,
  egressDomainAllowed,
  ISOLATION_SCOPE_CONTEXT_KEY,
  networkEgress,
  type SideEffect,
  type ToolCallContext,
  tenantIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
  type WritePermissionsPolicy,
} from './index.js';

function call(
  egress: readonly string[],
  sideEffect: SideEffect = 'read',
): ToolCallContext {
  return { connectorId: 'salesforce.export', sideEffect, egress, input: {} };
}

const matchesEveryHost = {
  startsWith: () => true,
  slice: () => '',
  toString: () => 'x',
};
const stringMethodsEntry = {
  toString: () => 'api.example.com',
  toLowerCase: () => ({ replace: () => matchesEveryHost }),
};

function hostsWith(entry: unknown): unknown[] {
  return ['api.example.com', entry];
}

function hostsWithHole(): unknown[] {
  const hosts: unknown[] = ['api.example.com'];
  hosts.length = 2;
  return hosts;
}

const nonStringHostEntries: [string, () => unknown[], string][] = [
  ['null', () => hostsWith(null), 'null'],
  ['undefined', () => hostsWith(undefined), 'undefined'],
  ['a hole', hostsWithHole, 'undefined'],
  ['a number', () => hostsWith(123), 'number'],
  ['a Symbol', () => hostsWith(Symbol('api.example.com')), 'symbol'],
  ['a String object', () => hostsWith(new String('api.example.com')), 'object'],
  [
    'a plain object with toString',
    () => hostsWith({ toString: () => 'api.example.com' }),
    'object',
  ],
  [
    'an object with its own string methods',
    () => hostsWith(stringMethodsEntry),
    'object',
  ],
];

function driftingHostList(): {
  list: readonly string[];
  reads: () => number;
} {
  let reads = 0;
  const list = new Proxy(['api.example.com'], {
    get(target, key, receiver) {
      if (key === '0') {
        reads += 1;
        return reads === 1 ? 'api.example.com' : stringMethodsEntry;
      }
      return Reflect.get(target, key, receiver);
    },
  });
  return { list, reads: () => reads };
}

class ExfilMatchingList extends Array<unknown> {
  filter(): never[] {
    return ['exfil.example'] as never[];
  }

  map(): never[] {
    return ['exfil.example'] as never[];
  }
}

// A real array whose own container methods and iterator answer with
// `answer`, never with its indexed entries.
function answeringList<T>(entries: readonly T[], answer: readonly unknown[]) {
  return Object.assign([...entries], {
    map: () => [...answer],
    some: () => answer.length > 0,
    every: () => true,
    includes: () => answer.length > 0,
    [Symbol.iterator]: function* () {
      yield* answer;
    },
  }) as T[];
}

const SIDE_EFFECT_RULE = "'read', 'write', 'destructive' or 'idempotent'";
const PATTERN_RULE =
  "a non-empty string without ':', whitespace, or a control or format character";

describe('tool policy decision metadata', () => {
  const callerContext = new RequestContext();
  callerContext.set(WORKFLOW_SCOPE_CONTEXT_KEY, 'private-caller');

  it.each([
    {
      evaluator: networkEgress({ allowedDomains: [], name: 'custom-label' }),
      context: call(['API.EXAMPLE.COM.']),
      code: 'EGRESS_HOST_NOT_ALLOWED_BY_ORG',
      policyKind: 'network-egress',
      details: { declaredHost: 'api.example.com' },
    },
    {
      evaluator: crossWorkflowIsolation({
        name: 'custom-label',
        targetScopeOf: () => 'private-target',
      }),
      context: call([]),
      code: 'WORKFLOW_SCOPE_MISSING',
      policyKind: 'cross-workflow-isolation',
      details: undefined,
    },
    {
      evaluator: crossWorkflowIsolation({
        name: 'custom-label',
        targetScopeOf: () => 'private-target',
      }),
      context: {
        ...call([]),
        requestContext: callerContext,
      },
      code: 'CROSS_WORKFLOW_ACCESS_DENIED',
      policyKind: 'cross-workflow-isolation',
      details: undefined,
    },
    {
      evaluator: tenantIsolation({ name: 'custom-label' }),
      context: call([]),
      code: 'ISOLATION_SCOPE_MISSING',
      policyKind: 'tenant-isolation',
      details: undefined,
    },
    {
      evaluator: backgroundExecution({ name: 'custom-label' }),
      context: { ...call([], 'write'), input: { _background: {} } },
      code: 'BACKGROUND_EXECUTION_DENIED',
      policyKind: 'background-execution',
      details: undefined,
    },
  ])('keeps $code independent of its diagnostic name', async ({
    evaluator,
    context,
    code,
    policyKind,
    details,
  }) => {
    const decision = await evaluator.evaluate(context);

    expect(evaluator.name).toBe('custom-label');
    expect(decision).toMatchObject({ allowed: false, code });
    if (decision.allowed || decision.code === undefined) {
      throw new Error('expected a coded denial');
    }
    expect(decision.details).toEqual(details);
    expect(CONNECTOR_DECISIONS[decision.code].policyKind).toBe(policyKind);
    expect(connectorDecisionRetryable(decision.code)).toBe(false);
  });
});

describe('networkEgress', () => {
  it('allows declared domains on the allowlist', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: ['api.openai.com'] });
    // #when / #then
    expect(await policy.evaluate(call(['api.openai.com']))).toEqual({
      allowed: true,
    });
  });

  it('denies a declared domain missing from the allowlist', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: ['api.openai.com'] });
    // #when / #then
    expect(await policy.evaluate(call(['api.evil.com']))).toEqual({
      allowed: false,
      reason: expect.stringContaining('api.evil.com'),
      code: 'EGRESS_HOST_NOT_ALLOWED_BY_ORG',
      details: { declaredHost: 'api.evil.com' },
    });
  });

  it('matches subdomains of a wildcard entry but not the apex', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: ['*.googleapis.com'] });
    // #when / #then
    expect(await policy.evaluate(call(['storage.googleapis.com']))).toEqual({
      allowed: true,
    });
    expect(await policy.evaluate(call(['googleapis.com']))).toMatchObject({
      allowed: false,
    });
  });

  it('holds the label boundary on wildcard matches', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: ['*.example.com'] });
    // #when / #then
    expect(await policy.evaluate(call(['evil-example.com']))).toMatchObject({
      allowed: false,
    });
  });

  it('denies all declared egress under an empty allowlist', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: [] });
    // #when / #then
    expect(await policy.evaluate(call(['api.openai.com']))).toMatchObject({
      allowed: false,
    });
  });

  it('allows connectors that declare no egress', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: [] });
    // #when / #then
    expect(await policy.evaluate(call([]))).toEqual({ allowed: true });
  });

  it('normalizes case and trailing dots on both sides', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: ['API.OpenAI.com.'] });
    // #when / #then
    expect(await policy.evaluate(call(['api.openai.COM']))).toEqual({
      allowed: true,
    });
  });

  it('matches uppercase wildcard entries case-insensitively', async () => {
    // #given
    const policy = networkEgress({ allowedDomains: ['*.EXAMPLE.com'] });
    // #when / #then
    expect(await policy.evaluate(call(['api.example.com']))).toEqual({
      allowed: true,
    });
  });

  it('rejects malformed allowlist entries at construction', () => {
    // #given
    const invalid = ['*', '', 'https://api.example.com', 'münchen.de'];
    // #when / #then
    for (const entry of invalid) {
      expect(() => networkEgress({ allowedDomains: [entry] })).toThrow(
        TypeError,
      );
    }
  });

  it.each(
    nonStringHostEntries,
  )('refuses %s as an allowlist entry at construction', (_label, allowedDomains, got) => {
    // #when / #then
    expect(() =>
      networkEgress({
        allowedDomains: allowedDomains() as unknown as readonly string[],
      }),
    ).toThrow(
      new TypeError(
        `networkEgress: allowedDomains entry 1 must be a string (got ${got})`,
      ),
    );
  });

  it('refuses an allowlist that is not an array', () => {
    // #when / #then
    expect(() =>
      networkEgress({
        allowedDomains: 'api.example.com' as unknown as readonly string[],
      }),
    ).toThrow(new TypeError('networkEgress: allowedDomains must be an array'));
  });

  it('matches against the allowlist entries it validated', async () => {
    // #given
    const { list, reads } = driftingHostList();
    const policy = networkEgress({ allowedDomains: list });
    // #when / #then
    expect(await policy.evaluate(call(['exfil.example']))).toMatchObject({
      allowed: false,
      code: 'EGRESS_HOST_NOT_ALLOWED_BY_ORG',
    });
    expect(await policy.evaluate(call(['api.example.com']))).toEqual({
      allowed: true,
    });
    expect(reads()).toBe(1);
  });

  it('is named network-egress unless overridden', () => {
    // #when / #then
    expect(networkEgress({ allowedDomains: [] }).name).toBe('network-egress');
    expect(
      networkEgress({ allowedDomains: [], name: 'egress-prod' }).name,
    ).toBe('egress-prod');
  });
});

describe('egressDomainAllowed', () => {
  it.each([
    ['an object with its own string methods', stringMethodsEntry],
    ['a String object', new String('exfil.example')],
    ['null', null],
  ])('matches nothing through %s in allowedDomains', (_label, entry) => {
    // #given
    const allowedDomains = [entry] as unknown as readonly string[];
    // #when / #then
    expect(egressDomainAllowed('exfil.example', allowedDomains)).toBe(false);
    expect(
      egressDomainAllowed('api.example.com', [
        ...allowedDomains,
        'api.example.com',
      ]),
    ).toBe(true);
  });

  it.each<[string, () => readonly string[]]>([
    [
      'a plain array with its own filter',
      () =>
        Object.assign(['api.vendor.example'], {
          filter: () => ['exfil.example'],
        }),
    ],
    [
      'a plain array whose own constructor overrides map',
      () =>
        Object.assign(['api.vendor.example'], {
          constructor: ExfilMatchingList,
        }),
    ],
    [
      'an Array subclass overriding filter and map',
      () => ExfilMatchingList.from(['api.vendor.example']),
    ],
  ])('matches no unlisted host through %s', (_label, list) => {
    // #given
    const allowedDomains = list();
    // #when / #then
    expect(egressDomainAllowed('exfil.example', allowedDomains)).toBe(false);
    expect(egressDomainAllowed('api.vendor.example', allowedDomains)).toBe(
      true,
    );
  });

  it('matches nothing through a list that is not an array', () => {
    // #given
    const allowedDomains = {
      length: 1,
      0: 'exfil.example',
    } as unknown as readonly string[];
    // #when / #then
    expect(egressDomainAllowed('exfil.example', allowedDomains)).toBe(false);
  });

  it('matches nothing through a domain that is not a string', () => {
    // #given
    const domain = {
      toLowerCase: () => ({
        replace: () => ({ length: 1e9, endsWith: () => true }),
      }),
    } as unknown as string;
    // #when / #then
    expect(egressDomainAllowed(domain, ['*.vendor.example'])).toBe(false);
  });

  it.each<[string, string, readonly string[]]>([
    ['an empty host against an empty entry', '', ['']],
    ['a blank host against a blank entry', ' ', [' ']],
    ['an empty host among valid entries', '', ['', 'api.example.com']],
    ['a scheme-bearing host against itself', 'https://x', ['https://x']],
  ])('matches nothing for %s, which no hostname can be', (_label, domain, allowed) => {
    // #when / #then
    expect(egressDomainAllowed(domain, allowed)).toBe(false);
  });

  it('skips a malformed entry and still matches a valid one', () => {
    // #when / #then
    expect(
      egressDomainAllowed('API.example.com.', ['', ' ', 'api.example.com']),
    ).toBe(true);
    expect(egressDomainAllowed('', [])).toBe(false);
  });
});

describe('approvalRequired', () => {
  it('requires approval for destructive connectors by default', () => {
    // #when / #then
    expect(
      approvalRequired('fs.deleteAll', { sideEffect: 'destructive' }),
    ).toBe(true);
  });

  it('lets org policy opt destructive connectors out', () => {
    // #when / #then
    expect(
      approvalRequired(
        'fs.deleteAll',
        { sideEffect: 'destructive' },
        { destructiveRequiresApproval: false },
      ),
    ).toBe(false);
  });

  it('gates writes matching a connector-id glob', () => {
    // #given
    const policy = { requireApproval: ['salesforce.*'] };
    // #when / #then
    expect(
      approvalRequired(
        'salesforce.createContact',
        { sideEffect: 'write' },
        policy,
      ),
    ).toBe(true);
    expect(
      approvalRequired('github.createIssue', { sideEffect: 'write' }, policy),
    ).toBe(false);
  });

  it('treats glob dots literally', () => {
    // #when / #then
    expect(
      approvalRequired(
        'salesforceXcreateContact',
        { sideEffect: 'write' },
        { requireApproval: ['salesforce.*'] },
      ),
    ).toBe(false);
  });

  it("gates every write-class connector under the '*' pattern", () => {
    // #when / #then
    expect(
      approvalRequired(
        'github.comment',
        { sideEffect: 'write' },
        { requireApproval: ['*'] },
      ),
    ).toBe(true);
  });

  it.each([
    ['an empty pattern', '', '""'],
    ['a trailing space', 'crm.* ', '"crm.* "'],
    ['a newline', 'crm.\n*', '"crm.\\n*"'],
    ['a tab', 'crm.\t*', '"crm.\\t*"'],
    ['a zero-width space', 'crm.​*', '"crm.​*"'],
    ['a control character', 'crm.\u0007*', '"crm.\\u0007*"'],
    ['a colon', 'tenant:crm.*', '"tenant:crm.*"'],
  ])('refuses %s in an approval pattern', (_label, pattern, got) => {
    // #when / #then — a pattern no accepted id can match never requires approval
    expect(() =>
      approvalRequired(
        'crm.assign',
        { sideEffect: 'write' },
        { requireApproval: ['crm.*', pattern] },
      ),
    ).toThrow(
      new TypeError(
        `approvalRequired: policy.requireApproval entry 1 must be ${PATTERN_RULE} (got ${got})`,
      ),
    );
  });

  it.each<[string, unknown, string]>([
    ['a number', 42, 'number'],
    ['null', null, 'null'],
    ['an object with its own split', { split: () => ['', ''] }, 'object'],
    ['a String object', new String('crm.*'), 'object'],
    ['a Symbol', Symbol('crm.*'), 'symbol'],
  ])('refuses %s as an approval pattern without calling it', (_label, entry, got) => {
    // #when / #then
    expect(() =>
      approvalRequired(
        'crm.assign',
        { sideEffect: 'write' },
        { requireApproval: [entry] as unknown as readonly string[] },
      ),
    ).toThrow(
      new TypeError(
        `approvalRequired: policy.requireApproval entry 0 must be ${PATTERN_RULE} (got ${got})`,
      ),
    );
  });

  it.each<[string, unknown]>([
    ['a string', 'crm.*'],
    ['a Set', new Set(['crm.*'])],
    ['an array-like object', { length: 1, 0: 'crm.*' }],
  ])('refuses %s as the approval pattern list', (_label, requireApproval) => {
    // #when / #then
    expect(() =>
      approvalRequired(
        'crm.assign',
        { sideEffect: 'write' },
        { requireApproval: requireApproval as readonly string[] },
      ),
    ).toThrow(
      new TypeError(
        'approvalRequired: policy.requireApproval must be an array',
      ),
    );
  });

  it('matches through index reads, not through the list its own some answers', () => {
    // #given — own some/iterator answer that nothing matches
    const requireApproval = answeringList(['crm.*'], []);
    // #when / #then
    expect(
      approvalRequired(
        'crm.assign',
        { sideEffect: 'write' },
        {
          requireApproval,
        },
      ),
    ).toBe(true);
  });

  it('validates the whole policy before any rule decides, for every connector', () => {
    // #given — a read connector and an always-approve manifest decide without
    // the pattern list
    const policy = { requireApproval: [42] as unknown as readonly string[] };
    // #when / #then
    expect(() =>
      approvalRequired('crm.lookup', { sideEffect: 'read' }, policy),
    ).toThrow(TypeError);
    expect(() =>
      approvalRequired(
        'crm.assign',
        { sideEffect: 'write', requiresApproval: true },
        policy,
      ),
    ).toThrow(TypeError);
  });

  it.each<[string, unknown, string]>([
    [
      'a string destructiveRequiresApproval',
      { destructiveRequiresApproval: 'false' },
      'approvalRequired: policy.destructiveRequiresApproval must be a boolean (got "false")',
    ],
    [
      'an unknown policy field',
      { requireAproval: ['crm.*'] },
      'approvalRequired: policy has unknown field "requireAproval" (valid fields: requireApproval, destructiveRequiresApproval)',
    ],
    [
      'a null policy',
      null,
      'approvalRequired: policy must be an object (got null)',
    ],
    [
      'a string policy',
      'crm.*',
      'approvalRequired: policy must be an object (got "crm.*")',
    ],
  ])('refuses %s', (_label, policy, message) => {
    // #when / #then
    expect(() =>
      approvalRequired(
        'crm.assign',
        { sideEffect: 'write' },
        policy as WritePermissionsPolicy,
      ),
    ).toThrow(new TypeError(message));
  });

  it.each([
    ['a mistyped side effect', 'Destructive', '"Destructive"'],
    ['a missing side effect', undefined, 'undefined'],
  ])('refuses %s', (_label, sideEffect, got) => {
    // #when / #then — an unknown side effect would skip the destructive default
    expect(() =>
      approvalRequired('fs.deleteAll', {
        sideEffect: sideEffect as SideEffect,
      }),
    ).toThrow(
      new TypeError(
        `approvalRequired: manifest.sideEffect must be ${SIDE_EFFECT_RULE} (got ${got})`,
      ),
    );
  });

  it('refuses a connector id that is not a string', () => {
    // #when / #then
    expect(() =>
      approvalRequired(42 as unknown as string, { sideEffect: 'write' }),
    ).toThrow(
      new TypeError(
        'approvalRequired: connectorId must be a string (got number)',
      ),
    );
  });

  it("lets '*' match across a line terminator in an unchecked id", () => {
    // #given — createConnector refuses this id, but approvalRequired is public
    const connectorId = 'crm\nassign';
    // #when / #then
    expect(
      approvalRequired(
        connectorId,
        { sideEffect: 'write' },
        {
          requireApproval: ['crm*'],
        },
      ),
    ).toBe(true);
    expect(
      approvalRequired(
        connectorId,
        { sideEffect: 'write' },
        {
          requireApproval: ['*'],
        },
      ),
    ).toBe(true);
  });

  it('keeps an uppercase pattern that matches an uppercase id', () => {
    // #when / #then
    expect(
      approvalRequired(
        'CRM.assign',
        { sideEffect: 'write' },
        {
          requireApproval: ['CRM.*'],
        },
      ),
    ).toBe(true);
    expect(
      approvalRequired(
        'crm.assign',
        { sideEffect: 'write' },
        {
          requireApproval: ['CRM.*'],
        },
      ),
    ).toBe(false);
  });

  it('escapes regex metacharacters in patterns', () => {
    // #given — '(', ')', '+' must match literally
    const policy = { requireApproval: ['api(v2)+.*'] };
    // #when / #then
    expect(
      approvalRequired('api(v2)+.write', { sideEffect: 'write' }, policy),
    ).toBe(true);
    expect(
      approvalRequired('apiv2v2.write', { sideEffect: 'write' }, policy),
    ).toBe(false);
  });

  it('never write-gates read connectors', () => {
    // #when / #then
    expect(
      approvalRequired(
        'salesforce.getContact',
        { sideEffect: 'read' },
        { requireApproval: ['salesforce.*'] },
      ),
    ).toBe(false);
  });

  it('treats idempotent side effects as write-class', () => {
    // #when / #then
    expect(
      approvalRequired(
        'salesforce.upsertContact',
        { sideEffect: 'idempotent' },
        { requireApproval: ['salesforce.*'] },
      ),
    ).toBe(true);
  });

  it('honors manifest.requiresApproval unconditionally', () => {
    // #when / #then
    expect(
      approvalRequired('anything.read', {
        sideEffect: 'read',
        requiresApproval: true,
      }),
    ).toBe(true);
  });

  it('refuses a misspelled manifest field, which would drop the approval it spells', () => {
    // #given
    const manifest = { sideEffect: 'write', requireApproval: true };
    // #when / #then
    expect(() =>
      approvalRequired('crm.assign', manifest as { sideEffect: SideEffect }),
    ).toThrow(
      new TypeError(
        'approvalRequired: manifest has unknown field "requireApproval" (valid fields: sideEffect, egress, egressEnforcement, idempotencyKey, requiresApproval, dryRun, rateLimit, background, requiredPermissions)',
      ),
    );
  });

  it('reads a full permission manifest, every member present', () => {
    // #given — createConnector passes the whole manifest
    const manifest: Required<PermissionManifest> = {
      sideEffect: 'write',
      egress: ['api.example.com'],
      egressEnforcement: 'enforced',
      idempotencyKey: false,
      requiresApproval: true,
      dryRun: false,
      rateLimit: '1/min',
      background: false,
      requiredPermissions: ['crm.write'],
    };
    // #when / #then
    expect(approvalRequired('crm.assign', manifest)).toBe(true);
  });

  it('requires nothing for unmatched writes', () => {
    // #when / #then
    expect(approvalRequired('github.comment', { sideEffect: 'write' })).toBe(
      false,
    );
  });

  it.each<[string, unknown, string]>([
    ['an empty string', '', '""'],
    ['0', 0, 'number'],
    ['NaN', Number.NaN, 'number'],
    ['null', null, 'null'],
    ["the string 'false'", 'false', '"false"'],
    ['1', 1, 'number'],
  ])('refuses %s as manifest.requiresApproval, which createConnector refuses too', (_label, requiresApproval, got) => {
    // #when / #then
    expect(() =>
      approvalRequired('crm.assign', {
        sideEffect: 'write',
        requiresApproval: requiresApproval as boolean,
      }),
    ).toThrow(
      new TypeError(
        `approvalRequired: manifest.requiresApproval must be a boolean when provided (got ${got})`,
      ),
    );
  });

  it('refuses a non-boolean dryRun in a full manifest', () => {
    // #when / #then
    expect(() =>
      approvalRequired('crm.assign', {
        sideEffect: 'write',
        dryRun: 'yes',
      } as unknown as PermissionManifest),
    ).toThrow(
      new TypeError(
        'approvalRequired: manifest.dryRun must be a boolean when provided (got "yes")',
      ),
    );
  });

  it('keeps the answer for a boolean requiresApproval', () => {
    // #when / #then
    expect(
      approvalRequired('crm.assign', {
        sideEffect: 'write',
        requiresApproval: true,
      }),
    ).toBe(true);
    expect(
      approvalRequired('crm.assign', {
        sideEffect: 'write',
        requiresApproval: false,
      }),
    ).toBe(false);
  });
});

describe('crossWorkflowIsolation', () => {
  function scopedCall(options: {
    scope?: unknown;
    input?: unknown;
  }): ToolCallContext {
    const requestContext = new RequestContext();
    if (options.scope !== undefined) {
      requestContext.set(WORKFLOW_SCOPE_CONTEXT_KEY, options.scope);
    }
    return {
      connectorId: 'flowsafe.readRunState',
      sideEffect: 'read',
      egress: [],
      input: options.input ?? {},
      requestContext,
    };
  }

  const policy = crossWorkflowIsolation({
    targetScopeOf: (toolCall) =>
      (toolCall.input as { workflowId?: string }).workflowId,
  });

  it('allows calls that do not address workflow state', async () => {
    // #given — the extractor finds no target scope
    // #when / #then
    expect(await policy.evaluate(scopedCall({ input: {} }))).toEqual({
      allowed: true,
    });
  });

  it("allows a call targeting the caller's own scope", async () => {
    // #when / #then
    expect(
      await policy.evaluate(
        scopedCall({ scope: 'wf-a', input: { workflowId: 'wf-a' } }),
      ),
    ).toEqual({ allowed: true });
  });

  it("denies a call targeting another workflow's scope", async () => {
    // #when / #then
    expect(
      await policy.evaluate(
        scopedCall({ scope: 'wf-a', input: { workflowId: 'wf-b' } }),
      ),
    ).toEqual({
      allowed: false,
      reason: "workflow 'wf-a' may not access state of 'wf-b'",
      code: 'CROSS_WORKFLOW_ACCESS_DENIED',
    });
  });

  it('fails closed when the caller has no minted scope', async () => {
    // #given — a targeted call without WORKFLOW_SCOPE_CONTEXT_KEY (e.g. a
    // direct invocation outside the runtime)
    // #when / #then
    expect(
      await policy.evaluate(scopedCall({ input: { workflowId: 'wf-a' } })),
    ).toMatchObject({ allowed: false, code: 'WORKFLOW_SCOPE_MISSING' });
  });

  it('fails closed on a non-string scope value', async () => {
    // #given — a corrupted/forged scope shape
    // #when / #then
    expect(
      await policy.evaluate(
        scopedCall({ scope: ['wf-a'], input: { workflowId: 'wf-a' } }),
      ),
    ).toMatchObject({ allowed: false, code: 'WORKFLOW_SCOPE_MISSING' });
  });
});

describe('tenantIsolation', () => {
  function scopedCall(scope?: unknown): ToolCallContext {
    const requestContext = new RequestContext();
    if (scope !== undefined) {
      requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, scope);
    }
    return {
      connectorId: 'crm.assign',
      sideEffect: 'write',
      egress: [],
      input: {},
      requestContext,
    };
  }

  const policy = tenantIsolation();

  it('allows a call carrying an isolation scope', async () => {
    // #when / #then
    expect(await policy.evaluate(scopedCall('acme'))).toEqual({
      allowed: true,
    });
  });

  it.each([
    ['absent scope', undefined],
    ['empty scope', ''],
    ['non-string scope', 42],
  ])('denies on %s', async (_label, scope) => {
    // #when / #then
    expect(await policy.evaluate(scopedCall(scope))).toMatchObject({
      allowed: false,
      code: 'ISOLATION_SCOPE_MISSING',
    });
  });

  it('treats a non-empty scope string as opaque and valid', async () => {
    // #when / #then
    expect(await policy.evaluate(scopedCall('anything:at all'))).toEqual({
      allowed: true,
    });
  });
});

describe('backgroundExecution', () => {
  function bgCall(sideEffect: SideEffect, input: unknown): ToolCallContext {
    return { connectorId: 'crm.assign', sideEffect, egress: [], input };
  }

  const policy = backgroundExecution();

  it('allows a write-class call with no _background override', async () => {
    // #when / #then — nothing asks for background, so nothing is denied
    expect(await policy.evaluate(bgCall('write', { topic: 'x' }))).toEqual({
      allowed: true,
    });
  });

  it.each<[string, Record<string, unknown>, string]>([
    ["'Write'", { sideEffect: 'Write' }, '"Write"'],
    ["'writes'", { sideEffect: 'writes' }, '"writes"'],
    ['null', { sideEffect: null }, 'null'],
    ['a missing side effect', {}, 'undefined'],
    ['side_effect for sideEffect', { side_effect: 'write' }, 'undefined'],
  ])('denies a call whose side effect is %s, even with no override', async (_label, fields, got) => {
    // #given
    const context = {
      connectorId: 'crm.assign',
      egress: [],
      input: { _background: true },
      ...fields,
    } as unknown as ToolCallContext;
    const withoutOverride = {
      ...context,
      input: {},
    } as ToolCallContext;

    // #when / #then
    for (const candidate of [context, withoutOverride]) {
      expect(await policy.evaluate(candidate)).toEqual({
        allowed: false,
        reason: `the call's side effect must be 'read', 'write', 'destructive' or 'idempotent' (got ${got}), so background execution cannot be ruled out`,
        code: 'BACKGROUND_EXECUTION_DENIED',
      });
    }
  });

  it.each([
    ['write', 'write' as SideEffect],
    ['destructive', 'destructive' as SideEffect],
    ['idempotent', 'idempotent' as SideEffect],
  ])('denies a %s call whose _background override would enable background', async (_label, sideEffect) => {
    // #when / #then — the model trying to flip a call off the foreground path
    expect(
      await policy.evaluate(
        bgCall(sideEffect, { topic: 'x', _background: { enabled: true } }),
      ),
    ).toMatchObject({ allowed: false, code: 'BACKGROUND_EXECUTION_DENIED' });
  });

  it('denies when _background is present with enabled undefined (defaults to background when eligible)', async () => {
    // #when / #then — enabled omitted resolves to true when a base config
    // enabled it; deny-by-default treats the bare override as a background enable
    expect(
      await policy.evaluate(bgCall('write', { _background: { timeoutMs: 5 } })),
    ).toMatchObject({ allowed: false, code: 'BACKGROUND_EXECUTION_DENIED' });
  });

  it.each<[string, unknown]>([
    ['{ enabled: false }', { enabled: false }],
    ["{ disposition: 'foreground' }", { disposition: 'foreground' }],
    [
      "{ enabled: false, disposition: 'deferred' }",
      { enabled: false, disposition: 'deferred' },
    ],
    ['null', null],
    ['undefined', undefined],
    ["the scalar 'true'", 'true'],
    ['false', false],
  ])('denies a write-class call whose _background key holds %s', async (_label, value) => {
    // #when / #then — the key's presence decides, as in the connector wrapper
    expect(
      await policy.evaluate(bgCall('write', { _background: value })),
    ).toMatchObject({ allowed: false, code: 'BACKGROUND_EXECUTION_DENIED' });
  });

  it('allows a read-only call to run in the background', async () => {
    // #when / #then — a read has no side effect whose timing the flip would move
    expect(
      await policy.evaluate(bgCall('read', { _background: { enabled: true } })),
    ).toEqual({ allowed: true });
  });

  it('honors a custom writeClass — an idempotent call passes when only destructive is gated', async () => {
    // #given
    const strict = backgroundExecution({ writeClass: ['destructive'] });
    // #when / #then
    expect(
      await strict.evaluate(
        bgCall('idempotent', { _background: { enabled: true } }),
      ),
    ).toEqual({ allowed: true });
  });

  it.each<[string, unknown, string]>([
    ['a string', 'write', 'backgroundExecution: writeClass must be an array'],
    [
      'a Set',
      new Set(['write']),
      'backgroundExecution: writeClass must be an array',
    ],
    ['an empty list', [], 'backgroundExecution: writeClass must not be empty'],
    [
      'an unknown member',
      ['write', 'rite'],
      `backgroundExecution: writeClass entry 1 must be ${SIDE_EFFECT_RULE} (got "rite")`,
    ],
    [
      'a mistyped member',
      ['Write'],
      `backgroundExecution: writeClass entry 0 must be ${SIDE_EFFECT_RULE} (got "Write")`,
    ],
    [
      'a String object member',
      [new String('write')],
      `backgroundExecution: writeClass entry 0 must be ${SIDE_EFFECT_RULE} (got object)`,
    ],
    [
      'an invalid member behind methods that answer with valid ones',
      answeringList(['rite'], ['write']),
      `backgroundExecution: writeClass entry 0 must be ${SIDE_EFFECT_RULE} (got "rite")`,
    ],
  ])('refuses %s as writeClass', (_label, writeClass, message) => {
    // #when / #then
    expect(() =>
      backgroundExecution({
        writeClass: writeClass as readonly SideEffect[],
      }),
    ).toThrow(new TypeError(message));
  });

  it('decides on the writeClass it copied by index', async () => {
    // #given — own includes/iterator answer that nothing is write-class, and
    // the caller empties its list after construction
    const writeClass = answeringList<SideEffect>(['write'], []);
    const strict = backgroundExecution({ writeClass });
    writeClass.length = 0;
    // #when / #then
    expect(
      await strict.evaluate(
        bgCall('write', { _background: { enabled: true } }),
      ),
    ).toMatchObject({ allowed: false, code: 'BACKGROUND_EXECUTION_DENIED' });
  });
});
