import { RequestContext } from '@mastra/core/request-context';
import { describe, expect, it } from 'vitest';

import {
  approvalRequired,
  crossWorkflowIsolation,
  ISOLATION_SCOPE_CONTEXT_KEY,
  networkEgress,
  type SideEffect,
  type ToolCallContext,
  tenantIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from './index.js';

function call(
  egress: readonly string[],
  sideEffect: SideEffect = 'read',
): ToolCallContext {
  return { connectorId: 'salesforce.export', sideEffect, egress, input: {} };
}

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
    // #given — entries that could never match a declared hostname and would
    // otherwise sit in the config as silently dead lines ('*' is not an
    // allow-all; omitting the policy is)
    const invalid = ['*', '', 'https://api.example.com', 'münchen.de'];
    // #when / #then
    for (const entry of invalid) {
      expect(() => networkEgress({ allowedDomains: [entry] })).toThrow(
        TypeError,
      );
    }
  });

  it('is named network-egress unless overridden', () => {
    // #when / #then
    expect(networkEgress({ allowedDomains: [] }).name).toBe('network-egress');
    expect(
      networkEgress({ allowedDomains: [], name: 'egress-prod' }).name,
    ).toBe('egress-prod');
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

  it('treats an empty pattern as matching nothing real', () => {
    // #when / #then
    expect(
      approvalRequired(
        'salesforce.createContact',
        { sideEffect: 'write' },
        { requireApproval: [''] },
      ),
    ).toBe(false);
  });

  it('escapes regex metacharacters in patterns', () => {
    // #given — '(', ')', '+' must match literally; only '*' is a glob token
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

  it('requires nothing for unmatched writes', () => {
    // #when / #then
    expect(approvalRequired('github.comment', { sideEffect: 'write' })).toBe(
      false,
    );
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
    targetScopeOf: (call) => (call.input as { workflowId?: string }).workflowId,
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
    });
  });

  it('fails closed when the caller has no minted scope', async () => {
    // #given — a targeted call without WORKFLOW_SCOPE_CONTEXT_KEY (e.g. a
    // direct invocation outside the runtime)
    // #when / #then
    expect(
      await policy.evaluate(scopedCall({ input: { workflowId: 'wf-a' } })),
    ).toMatchObject({ allowed: false });
  });

  it('fails closed on a non-string scope value', async () => {
    // #given — a corrupted/forged scope shape
    // #when / #then
    expect(
      await policy.evaluate(
        scopedCall({ scope: ['wf-a'], input: { workflowId: 'wf-a' } }),
      ),
    ).toMatchObject({ allowed: false });
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
  ])('denies on %s — a scoped deployment must never run scope-less', async (_label, scope) => {
    // #when / #then — the evaluator runs in the PRE-EXECUTE gates loop, so
    // this denial binds dry-run requests too (the dry-run branch returns
    // before the idempotency/rate-limit machinery, where a key-side check
    // could never reach it)
    expect(await policy.evaluate(scopedCall(scope))).toMatchObject({
      allowed: false,
    });
  });

  it('never parses the scope — any non-empty string is opaque and valid', async () => {
    // #when / #then — breakwater stays tenant-agnostic; the host owns the format
    expect(await policy.evaluate(scopedCall('anything:at all'))).toEqual({
      allowed: true,
    });
  });
});
