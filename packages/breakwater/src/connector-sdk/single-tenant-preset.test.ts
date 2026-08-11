// SPDX-License-Identifier: Apache-2.0
import { RequestContext } from '@mastra/core/request-context';
import type { Tool, ToolExecutionContext } from '@mastra/core/tools';
import { describe, expect, it } from 'vitest';

import { AuditLogger } from '../audit/index.js';
import {
  backgroundExecution,
  tenantIsolation,
} from '../policy-engine/index.js';
import {
  connectorManifest,
  createConnector,
  D1IdempotencyStore,
  D1RateLimitStore,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
  type SingleTenantConnectorPoliciesOptions,
  singleTenantConnectorPolicies,
} from './index.js';

const unusedDatabase = {
  prepare(_query: string): never {
    throw new Error('construction must not access D1');
  },
};

function productionOptions(): SingleTenantConnectorPoliciesOptions {
  return {
    durableStores: {
      idempotency: new D1IdempotencyStore(unusedDatabase),
      rateLimit: new D1RateLimitStore(unusedDatabase),
    },
    audit: {
      mode: 'production',
      logger: new AuditLogger({ sink: () => {} }),
    },
    egress: { allowedDomains: ['*.example.com'] },
    permissions: { principalPermissions: 'configured' },
  };
}

describe('singleTenantConnectorPolicies', () => {
  it('constructs a complete frozen policy set and validates the manifest', () => {
    const policies = singleTenantConnectorPolicies(productionOptions());
    const connector = createConnector({
      id: 'records.lookup',
      description: 'Look up one record',
      permissions: {
        sideEffect: 'read',
        egress: ['api.example.com'],
        idempotencyKey: true,
        rateLimit: '10/min',
        background: true,
        requiredPermissions: ['records.read'],
      },
      policies,
      execute: async () => ({ ok: true }),
    });

    expect(Object.isFrozen(policies)).toBe(true);
    expect(Object.isFrozen(policies.evaluators)).toBe(true);
    expect(policies.evaluators?.every(Object.isFrozen)).toBe(true);
    expect(Object.isFrozen(policies.networkEgress)).toBe(true);
    expect(Object.isFrozen(policies.networkEgress?.allowedDomains)).toBe(true);
    expect(policies.evaluators?.map(({ name }) => name)).toContain(
      'background-execution',
    );
    expect(connectorManifest(connector)).toMatchObject({
      egress: ['api.example.com'],
      idempotencyKey: true,
      rateLimit: '10/min',
      background: true,
      requiredPermissions: ['records.read'],
    });
  });

  it('allows an explicit unaudited development posture without isolation scope', async () => {
    const policies = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
    });
    const connector = createConnector({
      id: 'local.read',
      description: 'Read local state',
      permissions: { sideEffect: 'read' },
      policies,
      execute: async () => ({ ok: true }),
    });

    await expect(
      connector.execute?.({}, {
        requestContext: new RequestContext(),
      } as ToolExecutionContext),
    ).resolves.toEqual({ ok: true });
    expect(policies.audit).toBeUndefined();
  });

  it('rejects incomplete audit posture and non-D1 stores at the builder', () => {
    expect(() =>
      singleTenantConnectorPolicies({
        audit: { mode: 'production' },
        egress: { allowedDomains: [] },
        permissions: { principalPermissions: 'configured' },
      } as unknown as SingleTenantConnectorPoliciesOptions),
    ).toThrow(/audit\.logger/);

    expect(() =>
      singleTenantConnectorPolicies({
        audit: { mode: 'production', logger: new AuditLogger() },
        egress: { allowedDomains: [] },
        permissions: { principalPermissions: 'configured' },
      }),
    ).toThrow(/external sink/);

    expect(() =>
      singleTenantConnectorPolicies({
        durableStores: {
          idempotency: new InMemoryIdempotencyStore(),
          rateLimit: new InMemoryRateLimitStore(),
        },
        audit: { mode: 'development', allowUnaudited: true },
        egress: { allowedDomains: [] },
        permissions: { principalPermissions: 'not-configured' },
      } as unknown as SingleTenantConnectorPoliciesOptions),
    ).toThrow(/durableStores\.idempotency/);
  });

  it('requires the matching D1 store for manifest idempotency and rate limits', () => {
    const policies = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
    });

    expect(() =>
      createConnector({
        id: 'records.write',
        description: 'Write one record',
        permissions: { sideEffect: 'write', idempotencyKey: true },
        policies,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/requires D1IdempotencyStore/);
    expect(() =>
      createConnector({
        id: 'records.list',
        description: 'List records',
        permissions: { sideEffect: 'read', rateLimit: '10/min' },
        policies,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/requires D1RateLimitStore/);
  });

  it('rejects undeclared permission wiring and organization egress drift', () => {
    const policies = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: ['api.example.com'] },
      permissions: { principalPermissions: 'not-configured' },
    });

    expect(() =>
      createConnector({
        id: 'records.authorized',
        description: 'Read an authorized record',
        permissions: {
          sideEffect: 'read',
          requiredPermissions: ['records.read'],
        },
        policies,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/principal-permissions wiring/);
    expect(() =>
      createConnector({
        id: 'records.remote',
        description: 'Read a remote record',
        permissions: { sideEffect: 'read', egress: ['other.example.com'] },
        policies,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/outside the single-tenant preset organization allowlist/);
  });

  it('rejects tenant isolation even when the evaluator is renamed', () => {
    expect(() =>
      singleTenantConnectorPolicies({
        audit: { mode: 'development', allowUnaudited: true },
        egress: { allowedDomains: [] },
        permissions: { principalPermissions: 'not-configured' },
        evaluators: [tenantIsolation({ name: 'custom-scope' })],
      }),
    ).toThrow(/tenantIsolation/);
  });

  it('rejects a duplicate background-execution evaluator', () => {
    expect(() =>
      singleTenantConnectorPolicies({
        audit: { mode: 'development', allowUnaudited: true },
        egress: { allowedDomains: [] },
        permissions: { principalPermissions: 'not-configured' },
        evaluators: [backgroundExecution({ name: 'renamed-background' })],
      }),
    ).toThrow(/backgroundExecution/);
  });

  it('revalidates a branded preset after object spread', () => {
    const complete = singleTenantConnectorPolicies(productionOptions());
    const weakened = { ...complete, audit: undefined };

    expect(() =>
      createConnector({
        id: 'records.weakened',
        description: 'Read a record',
        permissions: { sideEffect: 'read' },
        policies: weakened,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset audit was replaced, removed, or added/);
  });

  it('keeps destructive approval enabled in preset mode', () => {
    expect(() =>
      singleTenantConnectorPolicies({
        audit: { mode: 'development', allowUnaudited: true },
        egress: { allowedDomains: [] },
        permissions: { principalPermissions: 'not-configured' },
        writePermissions: { destructiveRequiresApproval: false },
      } as unknown as SingleTenantConnectorPoliciesOptions),
    ).toThrow(/writePermissions\.destructiveRequiresApproval/);
  });

  it('rejects destructive approval disabled after spreading a branded preset', () => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
    });
    const weakened = {
      ...complete,
      writePermissions: { destructiveRequiresApproval: false },
    };

    expect(() =>
      createConnector({
        id: 'records.delete',
        description: 'Delete one record',
        permissions: { sideEffect: 'destructive' },
        policies: weakened,
        execute: async () => ({ deleted: true }),
      }),
    ).toThrow(/single-tenant preset writePermissions was replaced/);
  });

  it.each([
    ['non-array approval patterns', { requireApproval: 'records.*' }],
    ['empty approval pattern', { requireApproval: [''] }],
    ['unknown write-policy field', { approvalRequired: true }],
  ])('rejects %s after spreading a branded preset', (_label, replacement) => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
    });
    const malformed = { ...complete, writePermissions: replacement };

    expect(() =>
      createConnector({
        id: 'records.malformed-policy',
        description: 'Read one record',
        permissions: { sideEffect: 'read' },
        policies: malformed as never,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset writePermissions was replaced/);
  });

  it('compiles approval from the validated write-policy snapshot', async () => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
      writePermissions: { destructiveRequiresApproval: true },
    });
    const baselineWritePermissions = complete.writePermissions;
    const changing = { ...complete } as Record<PropertyKey, unknown>;
    let reads = 0;
    Object.defineProperty(changing, 'writePermissions', {
      enumerable: true,
      get: () =>
        reads++ === 0
          ? baselineWritePermissions
          : { destructiveRequiresApproval: false },
    });
    const connector = createConnector({
      id: 'records.snapshot-policy',
      description: 'Delete one record',
      permissions: { sideEffect: 'destructive' },
      policies: changing as never,
      execute: async () => ({ deleted: true }),
    }) as Tool<unknown, unknown>;

    await expect(
      connector.execute?.({}, {
        requestContext: new RequestContext(),
      } as ToolExecutionContext),
    ).rejects.toMatchObject({ policy: 'write-permissions' });
    expect(reads).toBe(1);
  });

  it('rejects organization egress replacement after spreading the preset', () => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: ['api.example.com'] },
      permissions: { principalPermissions: 'not-configured' },
    });
    const broadened = {
      ...complete,
      networkEgress: {
        allowedDomains: ['api.example.com', 'unreviewed.example.com'],
      },
    };

    expect(() =>
      createConnector({
        id: 'records.broadened-egress',
        description: 'Read a remote record',
        permissions: { sideEffect: 'read' },
        policies: broadened,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset networkEgress was replaced/);
  });

  it('rejects approval-policy removal after spreading the preset', () => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
      writePermissions: { requireApproval: ['records.*'] },
    });
    const weakened = { ...complete, writePermissions: undefined };

    expect(() =>
      createConnector({
        id: 'records.update',
        description: 'Update one record',
        permissions: { sideEffect: 'write' },
        policies: weakened,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset writePermissions was replaced/);
  });

  it('rejects background or custom evaluator removal after spreading the preset', () => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
      evaluators: [{ name: 'custom', evaluate: () => ({ allowed: true }) }],
    });
    const weakened = { ...complete, evaluators: [] };

    expect(() =>
      createConnector({
        id: 'records.no-evaluators',
        description: 'Read one record',
        permissions: { sideEffect: 'read' },
        policies: weakened,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset evaluators was replaced/);
  });

  it('rejects durable-store and audit replacement after spreading the preset', () => {
    const complete = singleTenantConnectorPolicies(productionOptions());
    const replacementAudit = new AuditLogger({ sink: () => {} });
    const replacements = [
      {
        ...complete,
        idempotencyStore: new D1IdempotencyStore(unusedDatabase),
      },
      { ...complete, rateLimitStore: new D1RateLimitStore(unusedDatabase) },
      { ...complete, audit: replacementAudit },
    ];

    for (const policies of replacements) {
      expect(() =>
        createConnector({
          id: 'records.replaced-service',
          description: 'Read one record',
          permissions: { sideEffect: 'read' },
          policies,
          execute: async () => ({ ok: true }),
        }),
      ).toThrow(/single-tenant preset .* was replaced/);
    }
  });

  it('rejects hidden permission-metadata replacement', () => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
    });
    const [brand] = Object.getOwnPropertySymbols(complete);
    if (!brand) throw new Error('preset brand is missing');
    const metadata = (complete as unknown as Record<symbol, unknown>)[brand];
    expect(Object.isFrozen(metadata)).toBe(true);
    const snapshot = (metadata as Record<string, unknown>).snapshot;
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(
      Object.isFrozen((snapshot as Record<string, unknown>).policies),
    ).toBe(true);
    const replacedMetadata = {
      ...(metadata as Record<string, unknown>),
      principalPermissions: 'configured',
    };
    const weakened = { ...complete, [brand]: replacedMetadata };

    expect(() =>
      createConnector({
        id: 'records.metadata-replaced',
        description: 'Read one protected record',
        permissions: {
          sideEffect: 'read',
          requiredPermissions: ['records.read'],
        },
        policies: weakened as never,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset metadata was replaced/);
  });

  it('rejects fetch replacement after spreading the preset', () => {
    const originalFetch = async () => new Response('original');
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
      fetch: originalFetch,
    });
    const replaced = {
      ...complete,
      fetch: async () => new Response('replacement'),
    };

    expect(() =>
      createConnector({
        id: 'records.fetch-replaced',
        description: 'Read one record',
        permissions: { sideEffect: 'read' },
        policies: replaced,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset fetch was replaced/);
  });

  it('rejects audit method mutation after preset construction', () => {
    const complete = singleTenantConnectorPolicies(productionOptions());
    Object.defineProperty(complete.audit, 'record', {
      value: () => undefined,
    });

    expect(() =>
      createConnector({
        id: 'records.audit-mutated',
        description: 'Read one record',
        permissions: { sideEffect: 'read' },
        policies: complete,
        execute: async () => ({ ok: true }),
      }),
    ).toThrow(/single-tenant preset audit\.record changed/);
  });

  it('uses the frozen evaluator snapshot after a changing accessor', async () => {
    const complete = singleTenantConnectorPolicies({
      audit: { mode: 'development', allowUnaudited: true },
      egress: { allowedDomains: [] },
      permissions: { principalPermissions: 'not-configured' },
      evaluators: [
        {
          name: 'always-deny',
          evaluate: () => ({ allowed: false, reason: 'snapshot denial' }),
        },
      ],
    });
    const baselineEvaluators = complete.evaluators;
    const changing = { ...complete } as Record<PropertyKey, unknown>;
    let reads = 0;
    Object.defineProperty(changing, 'evaluators', {
      enumerable: true,
      get: () => (reads++ === 0 ? baselineEvaluators : []),
    });
    const connector = createConnector({
      id: 'records.snapshot-evaluators',
      description: 'Read one record',
      permissions: { sideEffect: 'read' },
      policies: changing as never,
      execute: async () => ({ ok: true }),
    });

    await expect(
      connector.execute?.({}, {
        requestContext: new RequestContext(),
      } as ToolExecutionContext),
    ).rejects.toMatchObject({ policy: 'always-deny' });
    expect(reads).toBe(1);
  });
});
