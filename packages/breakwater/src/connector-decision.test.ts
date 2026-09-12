// SPDX-License-Identifier: Apache-2.0

import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  createSourceFile,
  forEachChild,
  isNewExpression,
  ScriptTarget,
} from 'typescript';
import { describe, expect, expectTypeOf, it } from 'vitest';
import { safeAuditErrorSummary } from './audit/safe-error.js';
import {
  CONNECTOR_DECISIONS,
  type ConnectorDecisionCode,
  type ConnectorDenialMetadata,
  ConnectorEvaluatorError,
  ConnectorInvocationError,
  ConnectorPolicyError,
  ConnectorStoreError,
  type ConnectorStoreName,
  type ConnectorStoreOperation,
  ConnectorValidationError,
  captureConnectorDenialMetadata,
  captureConnectorEvaluatorMetadata,
  connectorDecisionRetryable,
  connectorErrorDecision,
  isConnectorDecisionCode,
} from './connector-decision.js';

it('supplies explicit metadata at production policy-error constructor calls', () => {
  const sourceRoot = fileURLToPath(new URL('.', import.meta.url));
  const violations: string[] = [];
  let constructors = 0;
  for (const path of readdirSync(sourceRoot, {
    recursive: true,
    encoding: 'utf8',
  })) {
    if (!path.endsWith('.ts') || path.endsWith('.test.ts')) continue;
    const file = createSourceFile(
      path,
      readFileSync(join(sourceRoot, path), 'utf8'),
      ScriptTarget.Latest,
      true,
    );
    const visit = (node: import('typescript').Node): void => {
      if (
        isNewExpression(node) &&
        /(?:^|\.)ConnectorPolicyError$/.test(node.expression.getText(file))
      ) {
        constructors++;
        if ((node.arguments?.length ?? 0) < 4) {
          const location = file.getLineAndCharacterOfPosition(
            node.getStart(file),
          );
          violations.push(`${path}:${location.line + 1}`);
        }
      }
      forEachChild(node, visit);
    };
    visit(file);
  }
  expect(constructors).toBeGreaterThan(0);
  expect(violations).toEqual([]);
});

const catalogue = {
  CONNECTOR_ALLOWED: ['execution', false],
  PERMISSION_GRANTED: ['required-permissions', false],
  APPROVAL_GRANTED: ['write-permissions', false],
  IDEMPOTENCY_TAKEOVER: ['idempotency', false],
  EGRESS_INPUT_INVALID: ['egress-fetch', false],
  EGRESS_URL_INVALID: ['egress-fetch', false],
  EGRESS_SCHEME_NOT_ALLOWED: ['egress-fetch', false],
  EGRESS_HOST_NOT_DECLARED: ['egress-fetch', false],
  EGRESS_REDIRECT_URL_INVALID: ['egress-fetch', false],
  EGRESS_REDIRECT_SCHEME_NOT_ALLOWED: ['egress-fetch', false],
  EGRESS_REDIRECT_HOST_DENIED: ['egress-fetch', false],
  EGRESS_REDIRECT_UNVERIFIABLE: ['egress-fetch', false],
  EGRESS_REDIRECT_LIMIT_EXCEEDED: ['egress-fetch', false],
  EGRESS_REDIRECT_BODY_UNREPLAYABLE: ['egress-fetch', false],
  EGRESS_DENIED: ['egress-fetch', false],
  EGRESS_HOST_NOT_ALLOWED_BY_ORG: ['network-egress', false],
  PERMISSION_PROJECTION_INVALID: ['required-permissions', false],
  PERMISSION_MISSING: ['required-permissions', false],
  APPROVAL_GRANT_MISSING: ['write-permissions', false],
  RATE_LIMIT_EXCEEDED: ['rate-limit', true],
  IDEMPOTENCY_KEY_MISSING: ['idempotency', false],
  IDEMPOTENCY_CONFLICT: ['idempotency', true],
  IDEMPOTENCY_LEGACY_AMBIGUOUS: ['idempotency-key-migration', false],
  IDEMPOTENCY_MIGRATION_REQUIRED: ['idempotency-key-migration', false],
  DRY_RUN_UNSUPPORTED: ['dry-run', false],
  WORKFLOW_SCOPE_MISSING: ['cross-workflow-isolation', false],
  CROSS_WORKFLOW_ACCESS_DENIED: ['cross-workflow-isolation', false],
  ISOLATION_SCOPE_MISSING: ['tenant-isolation', false],
  BACKGROUND_OVERRIDE_DENIED: ['background', false],
  BACKGROUND_EXECUTION_DENIED: ['background-execution', false],
  EVALUATOR_DENIED: ['evaluator', false],
  EVALUATOR_FAILED: ['evaluator', false],
  STORE_UNAVAILABLE: ['store', true],
  STORE_COMMIT_FAILED: ['store', false],
  STORE_RELEASE_FAILED: ['store', false],
  CONNECTOR_EXECUTION_FAILED: ['execution', false],
  CONNECTOR_INPUT_INVALID: ['validation', false],
  CONNECTOR_OUTPUT_INVALID: ['validation', false],
  CONNECTOR_UNREGISTERED: ['invocation', false],
  CONNECTOR_BOUNDARY_MODIFIED: ['invocation', false],
  CONNECTOR_INVOCATION_OPTIONS_INVALID: ['invocation', false],
  CONNECTOR_BOUNDARY_UNVERIFIABLE: ['invocation', false],
} as const;

describe('connector decision catalogue', () => {
  it('pins the accepted code, category, and retryability contract', () => {
    expect(Object.keys(CONNECTOR_DECISIONS).sort()).toEqual(
      Object.keys(catalogue).sort(),
    );
    expect(Object.isFrozen(CONNECTOR_DECISIONS)).toBe(true);
    for (const [code, [policyKind, retryable]] of Object.entries(catalogue)) {
      expect(isConnectorDecisionCode(code)).toBe(true);
      if (!isConnectorDecisionCode(code)) throw new Error('unknown test code');
      expect(CONNECTOR_DECISIONS[code]).toEqual({ policyKind, retryable });
      expect(Object.isFrozen(CONNECTOR_DECISIONS[code])).toBe(true);
      expect(connectorDecisionRetryable(code)).toBe(retryable);
    }
  });

  it('keeps the public reference table executable', () => {
    const doc = readFileSync(
      new URL('../../../docs/connector-interface.md', import.meta.url),
      'utf8',
    );
    const rows = [
      ...doc.matchAll(/^\| `([A-Z_]+)` \|[^\n]*\| (true|false) \|$/gm),
    ];
    expect(rows.map((row) => row[1]).sort()).toEqual(
      Object.keys(catalogue).sort(),
    );
    for (const [, code, retryable] of rows) {
      expect(isConnectorDecisionCode(code)).toBe(true);
      if (!isConnectorDecisionCode(code)) throw new Error('unknown doc code');
      expect(CONNECTOR_DECISIONS[code].retryable).toBe(retryable === 'true');
    }
  });

  it.each([
    'constructor',
    '__proto__',
    'toString',
    '',
    'UNKNOWN',
    null,
    42,
  ])('rejects an unknown catalogue value %j without a retryable fallback', (value) => {
    expect(isConnectorDecisionCode(value)).toBe(false);
    expect(() =>
      connectorDecisionRetryable(value as ConnectorDecisionCode),
    ).toThrow('invalid connector decision code');
  });
});

describe('connector denial metadata', () => {
  it('preserves legacy policy labels and messages without inferring their code', () => {
    const error = new ConnectorPolicyError('publish', 'rate-limit', 'blocked');
    expect(error).toMatchObject({
      name: 'ConnectorPolicyError',
      message: 'connector publish denied by rate-limit: blocked',
      connector: 'publish',
      policy: 'rate-limit',
      reason: 'blocked',
      kind: 'connector-policy',
      code: 'EVALUATOR_DENIED',
      policyKind: 'evaluator',
      retryable: false,
    });
    const renamed = new ConnectorPolicyError(
      'publish',
      'custom-budget',
      'full',
      {
        code: 'RATE_LIMIT_EXCEEDED',
        details: { limit: 10, windowMs: 60_000 },
      },
    );
    expect(renamed).toMatchObject({
      policy: 'custom-budget',
      code: 'RATE_LIMIT_EXCEEDED',
      policyKind: 'rate-limit',
      retryable: true,
    });
  });

  it('detaches and freezes nested permission metadata', () => {
    const missing = ['contacts.write'];
    const details = {
      requiredPermissions: ['contacts.read', 'contacts.write'],
      missingPermissions: missing,
      permissionPolicyVersion: 'permissions-v7',
    };
    const error = new ConnectorPolicyError(
      'publish',
      'permissions',
      'blocked',
      {
        code: 'PERMISSION_MISSING',
        details,
      },
    );
    missing.push('secrets.read');
    details.requiredPermissions.length = 0;
    details.permissionPolicyVersion = 'changed';
    expect(error.details).toEqual({
      requiredPermissions: ['contacts.read', 'contacts.write'],
      missingPermissions: ['contacts.write'],
      permissionPolicyVersion: 'permissions-v7',
    });
    expect(Object.isFrozen(error.details)).toBe(true);
    const metadata = captureConnectorDenialMetadata({
      code: 'PERMISSION_MISSING',
      details: error.details,
    });
    expect(Object.isFrozen(metadata)).toBe(true);
    if (metadata.code !== 'PERMISSION_MISSING') {
      throw new Error('permission metadata code changed during capture');
    }
    expect(Object.isFrozen(metadata.details?.missingPermissions)).toBe(true);
  });

  it('retains precise details in the typed capture overload', () => {
    const captured = captureConnectorDenialMetadata({
      code: 'EGRESS_REDIRECT_HOST_DENIED',
      details: { host: 'api.example.com', hop: 1 },
    });
    expectTypeOf(captured.code).toEqualTypeOf<'EGRESS_REDIRECT_HOST_DENIED'>();
    expectTypeOf(captured.details?.host).toEqualTypeOf<
      string | null | undefined
    >();
    expect(captured.details).toEqual({ host: 'api.example.com', hop: 1 });
  });

  it.each([
    '',
    '[::1]',
    '_service.example.com',
    '-host.example.com',
    '!host.test',
    'foo.',
    '%f0%9f%8c%90',
  ])('preserves safe parsed host %j without applying declaration grammar', (host) => {
    expect(
      captureConnectorDenialMetadata({
        code: 'EGRESS_HOST_NOT_DECLARED',
        details: { host, hop: 0 },
      }).details,
    ).toEqual({ host, hop: 0 });
  });

  it.each([
    null,
    { code: 'UNKNOWN' },
    { code: '__proto__' },
    { code: 'CONNECTOR_ALLOWED' },
    { code: 'STORE_UNAVAILABLE' },
    { code: 'EVALUATOR_FAILED' },
    { code: 'EVALUATOR_DENIED', retryable: true },
    { code: 'EVALUATOR_DENIED', policyKind: 'rate-limit' },
    { code: 'EVALUATOR_DENIED', details: { key: 'secret' } },
    { code: 'RATE_LIMIT_EXCEEDED', details: { limit: -1 } },
    { code: 'RATE_LIMIT_EXCEEDED', details: { windowMs: Number.NaN } },
    {
      code: 'EGRESS_HOST_NOT_DECLARED',
      details: { host: 'https://x.test/private' },
    },
    { code: 'EGRESS_HOST_NOT_DECLARED', details: { host: 'user@x.test' } },
    {
      code: 'EGRESS_HOST_NOT_DECLARED',
      details: { host: 'host.test\nheader: secret' },
    },
    { code: 'EGRESS_HOST_NOT_DECLARED', details: { hop: 0.5 } },
    {
      code: 'EGRESS_HOST_NOT_DECLARED',
      details: { host: 'x.test', url: 'secret' },
    },
    {
      code: 'EGRESS_HOST_NOT_ALLOWED_BY_ORG',
      details: { declaredHost: '_host.test' },
    },
    { code: 'PERMISSION_MISSING', details: { missingPermissions: ['Secret'] } },
    {
      code: 'PERMISSION_MISSING',
      details: { permissionPolicyVersion: '\nsecret' },
    },
    { code: 'PERMISSION_MISSING', details: { permissions: ['secrets.read'] } },
    {
      code: 'PERMISSION_PROJECTION_INVALID',
      details: { missingPermissions: ['secrets.read'] },
    },
  ])('refuses malformed explicit metadata %j', (metadata) => {
    expect(() => captureConnectorDenialMetadata(metadata)).toThrow(
      'invalid connector decision metadata',
    );
    expect(
      () =>
        new ConnectorPolicyError(
          'publish',
          'custom',
          'blocked',
          metadata as ConnectorDenialMetadata,
        ),
    ).toThrow('invalid connector decision metadata');
  });

  it('captures legacy and explicitly coded evaluator results', () => {
    expect(
      captureConnectorEvaluatorMetadata({ allowed: false, reason: 'blocked' }),
    ).toEqual({ code: 'EVALUATOR_DENIED' });
    expect(
      captureConnectorEvaluatorMetadata({
        allowed: false,
        reason: 'blocked',
        code: 'EGRESS_HOST_NOT_ALLOWED_BY_ORG',
        details: { declaredHost: '*.example.com' },
      }),
    ).toEqual({
      code: 'EGRESS_HOST_NOT_ALLOWED_BY_ORG',
      details: { declaredHost: '*.example.com' },
    });
    expect(
      captureConnectorDenialMetadata({
        code: 'EVALUATOR_DENIED',
        details: undefined,
      }),
    ).toEqual({ code: 'EVALUATOR_DENIED' });
  });

  it.each([
    null,
    [],
    { details: undefined },
    { code: undefined },
    { code: 'CONNECTOR_ALLOWED' },
    { code: 'EVALUATOR_DENIED', retryable: false },
    { code: 'EVALUATOR_DENIED', policyKind: 'evaluator' },
    Object.create({ code: 'EVALUATOR_DENIED' }),
    Object.defineProperty({}, 'code', { get: () => 'EVALUATOR_DENIED' }),
    Object.defineProperty({ code: 'EVALUATOR_DENIED' }, 'details', {
      get: () => undefined,
    }),
  ])('fails instead of falling back for invalid evaluator metadata %j', (value) => {
    expect(() => captureConnectorEvaluatorMetadata(value)).toThrow(
      'invalid connector decision metadata',
    );
  });
});

describe('authored connector failures', () => {
  it.each([
    [
      'rate-limit',
      'increment',
      'STORE_UNAVAILABLE',
      true,
      'connector store unavailable',
    ],
    [
      'idempotency',
      'get',
      'STORE_UNAVAILABLE',
      true,
      'connector store unavailable',
    ],
    [
      'idempotency',
      'inspect',
      'STORE_UNAVAILABLE',
      true,
      'connector store unavailable',
    ],
    [
      'idempotency',
      'reserve',
      'STORE_UNAVAILABLE',
      true,
      'connector store unavailable',
    ],
    [
      'idempotency',
      'put',
      'STORE_COMMIT_FAILED',
      false,
      'connector store commit failed',
    ],
    [
      'idempotency',
      'release',
      'STORE_RELEASE_FAILED',
      false,
      'connector store release failed',
    ],
  ] as const)('classifies %s.%s without exposing its cause', (store, operation, code, retryable, message) => {
    const cause = { secret: 'store-token', circular: undefined as unknown };
    cause.circular = cause;
    const error = new ConnectorStoreError('publish', store, operation, {
      cause,
    });
    expect(error).toMatchObject({
      name: 'ConnectorStoreError',
      kind: 'connector-store',
      connector: 'publish',
      store,
      operation,
      code,
      retryable,
      message,
      policyKind: 'store',
    });
    expect(error.cause).toBe(cause);
    expect(Object.getOwnPropertyDescriptor(error, 'cause')?.enumerable).toBe(
      false,
    );
    expect(JSON.stringify(error)).not.toContain('store-token');
    expect(JSON.stringify(safeAuditErrorSummary(error))).not.toContain(
      'store-token',
    );
    expect(connectorErrorDecision(error)).toEqual({
      kind: 'connector-store',
      code,
      policyKind: 'store',
      retryable,
      details: undefined,
    });
    const sanitized = new ConnectorStoreError(
      error.connector,
      error.store,
      error.operation,
    );
    expect('cause' in sanitized).toBe(false);
    expect(connectorErrorDecision(sanitized)).toEqual(
      connectorErrorDecision(error),
    );
  });

  it.each([
    undefined,
    null,
    'secret',
    42,
    false,
  ])('preserves the actual thrown value %j as a native cause', (cause) => {
    const store = new ConnectorStoreError(
      'publish',
      'rate-limit',
      'increment',
      { cause },
    );
    const evaluator = new ConnectorEvaluatorError('publish', 'renamed', {
      cause,
    });
    expect(store.cause).toBe(cause);
    expect(evaluator.cause).toBe(cause);
    expect(evaluator).toMatchObject({
      message: 'connector policy evaluator failed',
      code: 'EVALUATOR_FAILED',
      kind: 'connector-evaluator',
      policyKind: 'evaluator',
      policy: 'renamed',
      retryable: false,
    });
    expect(JSON.stringify(safeAuditErrorSummary(evaluator))).not.toContain(
      'secret',
    );
  });

  it.each([
    ['rate-limit', 'put'],
    ['idempotency', 'increment'],
    ['custom', 'get'],
    ['idempotency', 'unknown'],
  ])('rejects impossible store metadata %s.%s', (store, operation) => {
    expect(
      () =>
        new ConnectorStoreError(
          'publish',
          store as ConnectorStoreName,
          operation as ConnectorStoreOperation,
        ),
    ).toThrow('invalid connector store operation');
  });

  it.each([
    ['input', 'CONNECTOR_INPUT_INVALID'],
    ['output', 'CONNECTOR_OUTPUT_INVALID'],
  ] as const)('retains redacted %s validation compatibility', (phase, code) => {
    const error = new ConnectorValidationError('publish', phase);
    expect(error).toMatchObject({
      name: 'ConnectorValidationError',
      kind: 'connector-validation',
      phase,
      code,
      policyKind: 'validation',
      retryable: false,
      message: 'connector invocation failed validation',
    });
    expect('cause' in error).toBe(false);
  });

  it.each([
    'CONNECTOR_UNREGISTERED',
    'CONNECTOR_BOUNDARY_MODIFIED',
    'CONNECTOR_INVOCATION_OPTIONS_INVALID',
    'CONNECTOR_BOUNDARY_UNVERIFIABLE',
  ] as const)('preserves TypeError identity and explicit %s classification', (code) => {
    const error = new ConnectorInvocationError(
      undefined,
      code,
      'existing boundary message',
    );
    expect(error).toBeInstanceOf(TypeError);
    expect(error).toMatchObject({
      name: 'ConnectorInvocationError',
      kind: 'connector-invocation',
      code,
      policyKind: 'invocation',
      retryable: false,
      message: 'existing boundary message',
    });
    expect('cause' in error).toBe(false);
  });

  it('projects authored instances without accepting structural metadata as authority', () => {
    const error = new ConnectorPolicyError(
      'publish',
      'custom',
      'secret-reason',
      { code: 'IDEMPOTENCY_CONFLICT' },
    );
    const projected = connectorErrorDecision(error);
    expect(projected).toEqual({
      kind: 'connector-policy',
      code: 'IDEMPOTENCY_CONFLICT',
      policyKind: 'idempotency',
      retryable: true,
      details: undefined,
    });
    expect(Object.isFrozen(projected)).toBe(true);
    expect(connectorErrorDecision({ ...error })).toBeUndefined();
    expect(connectorErrorDecision(new Error('secret-reason'))).toBeUndefined();
    expect(connectorErrorDecision(null)).toBeUndefined();
    expect(JSON.stringify(projected)).not.toContain('secret-reason');
    expect(JSON.stringify(safeAuditErrorSummary(error))).not.toContain(
      'secret-reason',
    );
  });
});
