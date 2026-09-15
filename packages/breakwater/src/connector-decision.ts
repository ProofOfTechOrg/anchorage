// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';
import { registerSafeAuditError } from './audit/safe-error.js';
import {
  isPermissionIdentifier,
  isPrincipalPermissions,
} from './rbac/permission.js';

/** Canonical connector policy category, independent of a diagnostic name. */
export type ConnectorPolicyName =
  | 'egress-fetch'
  | 'network-egress'
  | 'write-permissions'
  | 'required-permissions'
  | 'rate-limit'
  | 'idempotency'
  | 'idempotency-key-migration'
  | 'cross-workflow-isolation'
  | 'tenant-isolation'
  | 'background'
  | 'background-execution'
  | 'dry-run'
  | 'evaluator'
  | 'store'
  | 'validation'
  | 'invocation'
  | 'execution';

function decision<const P extends ConnectorPolicyName, const R extends boolean>(
  policyKind: P,
  retryable: R,
) {
  return Object.freeze({ policyKind, retryable });
}

/** Stable classification; retrying preserves the logical operation's identity. */
export const CONNECTOR_DECISIONS = Object.freeze({
  CONNECTOR_ALLOWED: decision('execution', false),
  PERMISSION_GRANTED: decision('required-permissions', false),
  APPROVAL_GRANTED: decision('write-permissions', false),
  IDEMPOTENCY_TAKEOVER: decision('idempotency', false),
  EGRESS_INPUT_INVALID: decision('egress-fetch', false),
  EGRESS_URL_INVALID: decision('egress-fetch', false),
  EGRESS_SCHEME_NOT_ALLOWED: decision('egress-fetch', false),
  EGRESS_HOST_NOT_DECLARED: decision('egress-fetch', false),
  EGRESS_REDIRECT_URL_INVALID: decision('egress-fetch', false),
  EGRESS_REDIRECT_SCHEME_NOT_ALLOWED: decision('egress-fetch', false),
  EGRESS_REDIRECT_HOST_DENIED: decision('egress-fetch', false),
  EGRESS_REDIRECT_UNVERIFIABLE: decision('egress-fetch', false),
  EGRESS_REDIRECT_LIMIT_EXCEEDED: decision('egress-fetch', false),
  EGRESS_REDIRECT_BODY_UNREPLAYABLE: decision('egress-fetch', false),
  EGRESS_DENIED: decision('egress-fetch', false),
  EGRESS_HOST_NOT_ALLOWED_BY_ORG: decision('network-egress', false),
  PERMISSION_PROJECTION_INVALID: decision('required-permissions', false),
  PERMISSION_MISSING: decision('required-permissions', false),
  APPROVAL_GRANT_MISSING: decision('write-permissions', false),
  RATE_LIMIT_EXCEEDED: decision('rate-limit', true),
  IDEMPOTENCY_KEY_MISSING: decision('idempotency', false),
  IDEMPOTENCY_CONFLICT: decision('idempotency', true),
  IDEMPOTENCY_LEGACY_AMBIGUOUS: decision('idempotency-key-migration', false),
  IDEMPOTENCY_MIGRATION_REQUIRED: decision('idempotency-key-migration', false),
  DRY_RUN_UNSUPPORTED: decision('dry-run', false),
  WORKFLOW_SCOPE_MISSING: decision('cross-workflow-isolation', false),
  CROSS_WORKFLOW_ACCESS_DENIED: decision('cross-workflow-isolation', false),
  ISOLATION_SCOPE_MISSING: decision('tenant-isolation', false),
  BACKGROUND_OVERRIDE_DENIED: decision('background', false),
  BACKGROUND_EXECUTION_DENIED: decision('background-execution', false),
  EVALUATOR_DENIED: decision('evaluator', false),
  EVALUATOR_FAILED: decision('evaluator', false),
  STORE_UNAVAILABLE: decision('store', true),
  STORE_COMMIT_FAILED: decision('store', false),
  STORE_RELEASE_FAILED: decision('store', false),
  CONNECTOR_EXECUTION_FAILED: decision('execution', false),
  CONNECTOR_INPUT_INVALID: decision('validation', false),
  CONNECTOR_OUTPUT_INVALID: decision('validation', false),
  CONNECTOR_UNREGISTERED: decision('invocation', false),
  CONNECTOR_BOUNDARY_MODIFIED: decision('invocation', false),
  CONNECTOR_INVOCATION_OPTIONS_INVALID: decision('invocation', false),
  CONNECTOR_BOUNDARY_UNVERIFIABLE: decision('invocation', false),
});

/** A machine-readable connector decision. */
export type ConnectorDecisionCode = keyof typeof CONNECTOR_DECISIONS;

/** Whether a value names an own member of the decision catalogue. */
export function isConnectorDecisionCode(
  value: unknown,
): value is ConnectorDecisionCode {
  return typeof value === 'string' && Object.hasOwn(CONNECTOR_DECISIONS, value);
}

/** Whether the same logical operation may be retried after its condition clears. */
export function connectorDecisionRetryable(
  code: ConnectorDecisionCode,
): boolean {
  if (!isConnectorDecisionCode(code)) {
    throw new TypeError('invalid connector decision code');
  }
  return CONNECTOR_DECISIONS[code].retryable;
}

/** A decision code that can deny connector execution. */
export type ConnectorDenialCode =
  | 'EGRESS_INPUT_INVALID'
  | 'EGRESS_URL_INVALID'
  | 'EGRESS_SCHEME_NOT_ALLOWED'
  | 'EGRESS_HOST_NOT_DECLARED'
  | 'EGRESS_REDIRECT_URL_INVALID'
  | 'EGRESS_REDIRECT_SCHEME_NOT_ALLOWED'
  | 'EGRESS_REDIRECT_HOST_DENIED'
  | 'EGRESS_REDIRECT_UNVERIFIABLE'
  | 'EGRESS_REDIRECT_LIMIT_EXCEEDED'
  | 'EGRESS_REDIRECT_BODY_UNREPLAYABLE'
  | 'EGRESS_DENIED'
  | 'EGRESS_HOST_NOT_ALLOWED_BY_ORG'
  | 'PERMISSION_PROJECTION_INVALID'
  | 'PERMISSION_MISSING'
  | 'APPROVAL_GRANT_MISSING'
  | 'RATE_LIMIT_EXCEEDED'
  | 'IDEMPOTENCY_KEY_MISSING'
  | 'IDEMPOTENCY_CONFLICT'
  | 'IDEMPOTENCY_LEGACY_AMBIGUOUS'
  | 'IDEMPOTENCY_MIGRATION_REQUIRED'
  | 'DRY_RUN_UNSUPPORTED'
  | 'WORKFLOW_SCOPE_MISSING'
  | 'CROSS_WORKFLOW_ACCESS_DENIED'
  | 'ISOLATION_SCOPE_MISSING'
  | 'BACKGROUND_OVERRIDE_DENIED'
  | 'BACKGROUND_EXECUTION_DENIED'
  | 'EVALUATOR_DENIED';

/** Copied, code-specific diagnostic fields that exclude causes and payloads. */
export type ConnectorDecisionDetails<
  Code extends ConnectorDenialCode = ConnectorDenialCode,
> =
  Code extends Exclude<
    Extract<ConnectorDenialCode, `EGRESS_${string}`>,
    'EGRESS_HOST_NOT_ALLOWED_BY_ORG'
  >
    ? Readonly<{ host?: string | null; hop?: number }>
    : Code extends 'EGRESS_HOST_NOT_ALLOWED_BY_ORG'
      ? Readonly<{ declaredHost?: string }>
      : Code extends 'PERMISSION_PROJECTION_INVALID'
        ? Readonly<{ requiredPermissions?: readonly string[] }>
        : Code extends 'PERMISSION_MISSING'
          ? Readonly<{
              requiredPermissions?: readonly string[];
              missingPermissions?: readonly string[];
              permissionPolicyVersion?: string;
            }>
          : Code extends 'RATE_LIMIT_EXCEEDED'
            ? Readonly<{ limit?: number; windowMs?: number }>
            : never;

/** Safe detail fields associated with each connector denial code. */
export type ConnectorDenialMetadata = {
  [Code in ConnectorDenialCode]: Readonly<{
    code: Code;
    details?: ConnectorDecisionDetails<Code>;
  }>;
}[ConnectorDenialCode];

const normalizedDeclaration = z
  .string()
  .refine((value) => /^(?:\*\.)?[a-z0-9][a-z0-9.-]*$/.test(value));
const normalizedHost = z.string().refine((value) => {
  if (/^\[[0-9a-f:.]+\]$/.test(value)) return true;
  if (value !== value.toLowerCase() || /[#/:<>?@[\\\]^|]/.test(value)) {
    return false;
  }
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code <= 0x20 || code === 0x7f) return false;
  }
  return true;
});
const egressDetails = z
  .strictObject({
    host: normalizedHost.nullable().optional(),
    hop: z.number().int().nonnegative().safe().optional(),
  })
  .readonly();
const permissions = z
  .array(z.custom<string>(isPermissionIdentifier))
  .readonly();
const policyVersion = z.custom<string>((value) =>
  isPrincipalPermissions({ permissions: [], policyVersion: value }),
);
const noDetails = z.never();

const denialDetails = {
  EGRESS_INPUT_INVALID: egressDetails,
  EGRESS_URL_INVALID: egressDetails,
  EGRESS_SCHEME_NOT_ALLOWED: egressDetails,
  EGRESS_HOST_NOT_DECLARED: egressDetails,
  EGRESS_REDIRECT_URL_INVALID: egressDetails,
  EGRESS_REDIRECT_SCHEME_NOT_ALLOWED: egressDetails,
  EGRESS_REDIRECT_HOST_DENIED: egressDetails,
  EGRESS_REDIRECT_UNVERIFIABLE: egressDetails,
  EGRESS_REDIRECT_LIMIT_EXCEEDED: egressDetails,
  EGRESS_REDIRECT_BODY_UNREPLAYABLE: egressDetails,
  EGRESS_DENIED: egressDetails,
  EGRESS_HOST_NOT_ALLOWED_BY_ORG: z
    .strictObject({ declaredHost: normalizedDeclaration.optional() })
    .readonly(),
  PERMISSION_PROJECTION_INVALID: z
    .strictObject({ requiredPermissions: permissions.optional() })
    .readonly(),
  PERMISSION_MISSING: z
    .strictObject({
      requiredPermissions: permissions.optional(),
      missingPermissions: permissions.optional(),
      permissionPolicyVersion: policyVersion.optional(),
    })
    .readonly(),
  APPROVAL_GRANT_MISSING: noDetails,
  RATE_LIMIT_EXCEEDED: z
    .strictObject({
      limit: z.number().int().positive().safe().optional(),
      windowMs: z.number().int().positive().safe().optional(),
    })
    .readonly(),
  IDEMPOTENCY_KEY_MISSING: noDetails,
  IDEMPOTENCY_CONFLICT: noDetails,
  IDEMPOTENCY_LEGACY_AMBIGUOUS: noDetails,
  IDEMPOTENCY_MIGRATION_REQUIRED: noDetails,
  DRY_RUN_UNSUPPORTED: noDetails,
  WORKFLOW_SCOPE_MISSING: noDetails,
  CROSS_WORKFLOW_ACCESS_DENIED: noDetails,
  ISOLATION_SCOPE_MISSING: noDetails,
  BACKGROUND_OVERRIDE_DENIED: noDetails,
  BACKGROUND_EXECUTION_DENIED: noDetails,
  EVALUATOR_DENIED: noDetails,
} satisfies {
  [Code in ConnectorDenialCode]: z.ZodType<ConnectorDecisionDetails<Code>>;
};

function invalidMetadata(): never {
  throw new TypeError('invalid connector decision metadata');
}

const explicitMetadata = z.strictObject({
  code: z.custom<ConnectorDenialCode>(
    (value) => typeof value === 'string' && Object.hasOwn(denialDetails, value),
  ),
  details: z.unknown().optional(),
});

/** @internal */
export function captureConnectorDenialMetadata<
  Code extends ConnectorDenialCode,
>(value: {
  code: Code;
  details?: Extract<ConnectorDenialMetadata, { code: Code }>['details'];
}): Extract<ConnectorDenialMetadata, { code: Code }>;
/** @internal */
export function captureConnectorDenialMetadata(
  value: unknown,
): ConnectorDenialMetadata;
export function captureConnectorDenialMetadata(
  value: unknown,
): ConnectorDenialMetadata {
  const parsed = explicitMetadata.safeParse(value);
  if (!parsed.success) return invalidMetadata();
  const { code, details } = parsed.data;
  if (details === undefined) {
    return Object.freeze({ code }) as ConnectorDenialMetadata;
  }
  const parsedDetails = denialDetails[code].safeParse(details);
  if (!parsedDetails.success) return invalidMetadata();
  return Object.freeze({
    code,
    details: parsedDetails.data,
  }) as ConnectorDenialMetadata;
}

/** @internal */
export function captureConnectorEvaluatorMetadata(
  decision: unknown,
): ConnectorDenialMetadata {
  if (
    decision === null ||
    typeof decision !== 'object' ||
    Array.isArray(decision) ||
    'retryable' in decision ||
    'policyKind' in decision
  ) {
    return invalidMetadata();
  }
  const code = Object.getOwnPropertyDescriptor(decision, 'code');
  const details = Object.getOwnPropertyDescriptor(decision, 'details');
  if (
    (code && !('value' in code)) ||
    (details && !('value' in details)) ||
    (!code && 'code' in decision) ||
    (!details && 'details' in decision)
  ) {
    return invalidMetadata();
  }
  if (!code && !details) return Object.freeze({ code: 'EVALUATOR_DENIED' });
  return captureConnectorDenialMetadata({
    code: code?.value,
    details: details?.value,
  });
}

/** Stable metadata for errors authored by the connector boundary. */
export interface ConnectorErrorDecision {
  readonly kind:
    | 'connector-policy'
    | 'connector-store'
    | 'connector-evaluator'
    | 'connector-validation'
    | 'connector-invocation';
  readonly code: ConnectorDecisionCode;
  readonly policyKind: ConnectorPolicyName;
  readonly retryable: boolean;
  readonly details?: ConnectorDecisionDetails;
}

const errorDecisions = new WeakMap<object, ConnectorErrorDecision>();

function registerDecision(
  error: Error,
  kind: ConnectorErrorDecision['kind'],
  code: ConnectorDecisionCode,
  reason: string,
  details?: ConnectorDecisionDetails,
): void {
  const { policyKind, retryable } = CONNECTOR_DECISIONS[code];
  errorDecisions.set(
    error,
    Object.freeze({ kind, code, policyKind, retryable, details }),
  );
  registerSafeAuditError(error, {
    reason,
    detail: { kind, decisionCode: code, policyKind, retryable },
  });
}

/** @internal */
export function connectorErrorDecision(
  error: unknown,
): ConnectorErrorDecision | undefined {
  return typeof error === 'object' && error !== null
    ? errorDecisions.get(error)
    : undefined;
}

/**
 * `value instanceof ctor` for a value the caller does not own. The check walks
 * the value's prototype chain, so a thrown value whose `getPrototypeOf` is a
 * trap answers with a throw; a value that cannot say what it is, is not the
 * constructor asked about.
 *
 * @internal
 */
export function isInstanceOf<T>(
  value: unknown,
  ctor: abstract new (...args: never[]) => T,
): value is T {
  try {
    return value instanceof ctor;
  } catch {
    return false;
  }
}

/** @internal */
export function readProperty<T extends object, K extends keyof T>(
  value: T,
  key: K,
): T[K] | undefined {
  try {
    return value[key];
  } catch {
    return undefined;
  }
}

/** Policy refusal; diagnostic names do not determine machine classification. */
export class ConnectorPolicyError extends Error {
  readonly kind = 'connector-policy';
  readonly connector: string;
  readonly policy: string;
  readonly reason: string;
  readonly code: ConnectorDenialCode;
  readonly policyKind: ConnectorPolicyName;
  readonly retryable: boolean;
  readonly details?: ConnectorDecisionDetails;

  constructor(
    connector: string,
    policy: string,
    reason: string,
    metadata?: ConnectorDenialMetadata,
  ) {
    super(`connector ${connector} denied by ${policy}: ${reason}`);
    const captured = captureConnectorDenialMetadata(
      metadata === undefined ? { code: 'EVALUATOR_DENIED' } : metadata,
    );
    this.name = 'ConnectorPolicyError';
    this.connector = connector;
    this.policy = policy;
    this.reason = reason;
    this.code = captured.code;
    this.policyKind = CONNECTOR_DECISIONS[this.code].policyKind;
    this.retryable = CONNECTOR_DECISIONS[this.code].retryable;
    this.details = captured.details;
    registerDecision(
      this,
      this.kind,
      this.code,
      'connector policy denied execution',
      this.details,
    );
  }
}

/** Store used at the connector enforcement boundary. */
export type ConnectorStoreName = 'rate-limit' | 'idempotency';

/** Actual store method whose invocation failed. */
export type ConnectorStoreOperation =
  | 'increment'
  | 'get'
  | 'inspect'
  | 'reserve'
  | 'put'
  | 'release';

/** Storage failure with its original thrown value available as native cause. */
export class ConnectorStoreError extends Error {
  readonly kind = 'connector-store';
  readonly connector: string;
  readonly store: ConnectorStoreName;
  readonly operation: ConnectorStoreOperation;
  readonly code:
    | 'STORE_UNAVAILABLE'
    | 'STORE_COMMIT_FAILED'
    | 'STORE_RELEASE_FAILED';
  readonly policyKind: 'store';
  readonly retryable: boolean;

  constructor(
    connector: string,
    store: ConnectorStoreName,
    operation: ConnectorStoreOperation,
    options?: ErrorOptions,
  ) {
    const code =
      operation === 'put'
        ? 'STORE_COMMIT_FAILED'
        : operation === 'release'
          ? 'STORE_RELEASE_FAILED'
          : 'STORE_UNAVAILABLE';
    const message =
      code === 'STORE_COMMIT_FAILED'
        ? 'connector store commit failed'
        : code === 'STORE_RELEASE_FAILED'
          ? 'connector store release failed'
          : 'connector store unavailable';
    super(message, options);
    if (
      (store !== 'rate-limit' && store !== 'idempotency') ||
      (store === 'rate-limit'
        ? operation !== 'increment'
        : !['get', 'inspect', 'reserve', 'put', 'release'].includes(operation))
    ) {
      throw new TypeError('invalid connector store operation');
    }
    this.name = 'ConnectorStoreError';
    this.connector = connector;
    this.store = store;
    this.operation = operation;
    this.code = code;
    this.policyKind = CONNECTOR_DECISIONS[code].policyKind;
    this.retryable = CONNECTOR_DECISIONS[code].retryable;
    registerDecision(this, this.kind, code, message);
  }
}

/** An evaluator failed before returning a valid connector decision. */
export class ConnectorEvaluatorError extends Error {
  readonly kind = 'connector-evaluator';
  readonly connector: string;
  readonly policy: string;
  readonly code = 'EVALUATOR_FAILED';
  readonly policyKind = CONNECTOR_DECISIONS.EVALUATOR_FAILED.policyKind;
  readonly retryable = CONNECTOR_DECISIONS.EVALUATOR_FAILED.retryable;

  constructor(connector: string, policy: string, options?: ErrorOptions) {
    super('connector policy evaluator failed', options);
    this.name = 'ConnectorEvaluatorError';
    this.connector = connector;
    this.policy = policy;
    registerDecision(this, this.kind, this.code, this.message);
  }
}

/** Redacted input or output validation failure from a direct connector call. */
export class ConnectorValidationError extends Error {
  readonly kind = 'connector-validation';
  readonly connector: string;
  readonly phase: 'input' | 'output';
  readonly code: 'CONNECTOR_INPUT_INVALID' | 'CONNECTOR_OUTPUT_INVALID';
  readonly policyKind: 'validation';
  readonly retryable: false;

  constructor(connector: string, phase: 'input' | 'output') {
    super('connector invocation failed validation');
    if (phase !== 'input' && phase !== 'output') {
      throw new TypeError('invalid connector validation phase');
    }
    this.name = 'ConnectorValidationError';
    this.connector = connector;
    this.phase = phase;
    this.code =
      phase === 'input'
        ? 'CONNECTOR_INPUT_INVALID'
        : 'CONNECTOR_OUTPUT_INVALID';
    this.policyKind = CONNECTOR_DECISIONS[this.code].policyKind;
    this.retryable = CONNECTOR_DECISIONS[this.code].retryable;
    registerDecision(this, this.kind, this.code, this.message);
  }
}

/** A failure to enter the registered connector invocation boundary. */
export type ConnectorInvocationCode = {
  [Code in ConnectorDecisionCode]: (typeof CONNECTOR_DECISIONS)[Code]['policyKind'] extends 'invocation'
    ? Code
    : never;
}[ConnectorDecisionCode];

/** Invalid direct invocation, retaining native TypeError compatibility. */
export class ConnectorInvocationError extends TypeError {
  readonly kind = 'connector-invocation';
  readonly connector: string | undefined;
  readonly code: ConnectorInvocationCode;
  readonly policyKind: 'invocation';
  readonly retryable: false;

  constructor(
    connector: string | undefined,
    code: ConnectorInvocationCode,
    message: string,
  ) {
    super(message);
    if (
      !isConnectorDecisionCode(code) ||
      CONNECTOR_DECISIONS[code].policyKind !== 'invocation'
    ) {
      throw new TypeError('invalid connector invocation code');
    }
    this.name = 'ConnectorInvocationError';
    this.connector = connector;
    this.code = code;
    this.policyKind = CONNECTOR_DECISIONS[code].policyKind;
    this.retryable = CONNECTOR_DECISIONS[code].retryable;
    registerDecision(
      this,
      this.kind,
      this.code,
      'connector invocation boundary failed',
    );
  }
}
