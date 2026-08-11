// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';

import { AuditLogger } from '../audit/index.js';
import type {
  NetworkEgressOptions,
  ToolPolicyEvaluator,
  WritePermissionsPolicy,
} from '../policy-engine/tool-policy.js';
import {
  backgroundExecution,
  egressDomainAllowed,
  isBackgroundExecutionEvaluator,
  isTenantIsolationEvaluator,
  networkEgress,
} from '../policy-engine/tool-policy.js';
import { D1IdempotencyStore } from './d1-idempotency-store.js';
import { D1RateLimitStore } from './d1-rate-limit-store.js';
import type { EgressFetchBase } from './egress-fetch.js';
import type {
  AtomicIdempotencyStore,
  ConnectorPolicies,
  PermissionManifest,
  RateLimitStore,
} from './index.js';

const auditRecordMethod = AuditLogger.prototype.record;
const auditHasExternalSinkMethod = AuditLogger.prototype.hasExternalSink;
const d1GetMethod = D1IdempotencyStore.prototype.get;
const d1PutMethod = D1IdempotencyStore.prototype.put;
const d1ReserveMethod = D1IdempotencyStore.prototype.reserve;
const d1ReleaseMethod = D1IdempotencyStore.prototype.release;
const d1IncrementMethod = D1RateLimitStore.prototype.increment;

/** Durable stores available to the single-deployment connector preset. */
export interface SingleTenantDurableStores {
  /** D1-backed replay protection for connectors declaring idempotency keys. */
  idempotency?: D1IdempotencyStore;
  /** D1-backed shared budgets for connectors declaring rate limits. */
  rateLimit?: D1RateLimitStore;
}

/** Audit behavior required by the single-deployment connector preset. */
export type SingleTenantAuditPosture =
  | {
      /** Production posture requires a configured audit logger. */
      mode: 'production';
      /** Logger receiving every connector decision and gate failure. */
      logger: AuditLogger;
    }
  | {
      /** Development is the only posture that may deliberately omit audit. */
      mode: 'development';
      /** Explicit acknowledgement that this preset has no audit trail. */
      allowUnaudited: true;
    };

/** Host permission-projection posture for connectors using the preset. */
export interface SingleTenantPermissionPosture {
  /** Whether the host mints `breakwater.principalPermissions` on every leg. */
  principalPermissions: 'configured' | 'not-configured';
}

/** Validated inputs for {@link singleTenantConnectorPolicies}. */
export interface SingleTenantConnectorPoliciesOptions {
  /** D1 stores used when a connector manifest enables the matching control. */
  durableStores?: SingleTenantDurableStores;
  /** Production audit logger or the explicit development-only opt-out. */
  audit: SingleTenantAuditPosture;
  /** Organization allowlist applied to every connector declaration. */
  egress: NetworkEgressOptions;
  /** Trusted host permission-projection wiring. */
  permissions: SingleTenantPermissionPosture;
  /** Optional organization approval rules. Destructive approval stays on. */
  writePermissions?: Omit<
    WritePermissionsPolicy,
    'destructiveRequiresApproval'
  > & {
    destructiveRequiresApproval?: true;
  };
  /** Additional deterministic tool-boundary evaluators. */
  evaluators?: readonly ToolPolicyEvaluator[];
  /** Optional base fetch used by the connector runtime guard. */
  fetch?: EgressFetchBase;
}

const singleTenantPreset = Symbol('breakwater.singleTenantConnectorPolicies');
const issuedPresetMetadata = new WeakSet<object>();

interface SingleTenantPolicySnapshot {
  readonly policies: Readonly<ConnectorPolicies>;
  readonly auditMember?: AuditLogger['record'];
  readonly auditRecord?: AuditLogger['record'];
}

interface SingleTenantPresetMetadata {
  readonly kind: 'single-tenant';
  readonly auditMode: SingleTenantAuditPosture['mode'];
  readonly principalPermissions: SingleTenantPermissionPosture['principalPermissions'];
  readonly snapshot: SingleTenantPolicySnapshot;
}

/** Connector policies carrying the single-deployment construction contract. */
export type SingleTenantConnectorPolicies = Readonly<ConnectorPolicies> & {
  readonly [singleTenantPreset]: unknown;
};

interface ValidatedConnectorPolicies {
  readonly policies: ConnectorPolicies;
  readonly auditRecord?: AuditLogger['record'];
}

const evaluatorSchema = z.custom<ToolPolicyEvaluator>(
  (value) =>
    typeof value === 'object' &&
    value !== null &&
    typeof (value as ToolPolicyEvaluator).name === 'string' &&
    (value as ToolPolicyEvaluator).name.length > 0 &&
    typeof (value as ToolPolicyEvaluator).evaluate === 'function',
  'must be a tool policy evaluator',
);

const writePermissionsSchema = z.strictObject({
  requireApproval: z.array(z.string().min(1)).optional(),
  destructiveRequiresApproval: z.literal(true).optional(),
});

const optionsSchema = z.strictObject({
  durableStores: z
    .strictObject({
      idempotency: z.instanceof(D1IdempotencyStore).optional(),
      rateLimit: z.instanceof(D1RateLimitStore).optional(),
    })
    .optional(),
  audit: z.discriminatedUnion('mode', [
    z.strictObject({
      mode: z.literal('production'),
      logger: z.instanceof(AuditLogger),
    }),
    z.strictObject({
      mode: z.literal('development'),
      allowUnaudited: z.literal(true),
    }),
  ]),
  egress: z.strictObject({
    allowedDomains: z.array(z.string()),
    name: z.string().min(1).optional(),
  }),
  permissions: z.strictObject({
    principalPermissions: z.enum(['configured', 'not-configured']),
  }),
  writePermissions: writePermissionsSchema.optional(),
  evaluators: z.array(evaluatorSchema).optional(),
  fetch: z
    .custom<EgressFetchBase>(
      (value) => typeof value === 'function',
      'must be a fetch function',
    )
    .optional(),
});

function parseOptions(options: SingleTenantConnectorPoliciesOptions) {
  const result = optionsSchema.safeParse(options);
  if (result.success) return result.data;
  const issue = result.error.issues[0];
  const path = issue?.path.length ? issue.path.join('.') : 'options';
  throw new TypeError(
    `singleTenantConnectorPolicies: invalid ${path}: ${issue?.message ?? 'configuration'}`,
  );
}

function presetMetadata(
  policies: ConnectorPolicies,
): SingleTenantPresetMetadata | undefined {
  const metadata = (policies as Partial<SingleTenantConnectorPolicies>)[
    singleTenantPreset
  ];
  if (metadata === undefined) return undefined;
  if (
    typeof metadata !== 'object' ||
    metadata === null ||
    !issuedPresetMetadata.has(metadata)
  ) {
    throw new TypeError(
      'single-tenant preset metadata was replaced after validation',
    );
  }
  return metadata as SingleTenantPresetMetadata;
}

function snapshotEvaluator(
  evaluator: ToolPolicyEvaluator,
): ToolPolicyEvaluator {
  return Object.freeze({
    name: evaluator.name,
    evaluate: Object.freeze(evaluator.evaluate.bind(evaluator)),
  });
}

function snapshotIdempotencyStore(
  store: D1IdempotencyStore,
): AtomicIdempotencyStore & { readonly pendingTtlMs: number } {
  return Object.freeze({
    pendingTtlMs: store.pendingTtlMs,
    get: Object.freeze(d1GetMethod.bind(store)),
    put: Object.freeze(d1PutMethod.bind(store)),
    reserve: Object.freeze(d1ReserveMethod.bind(store)),
    release: Object.freeze(d1ReleaseMethod.bind(store)),
  });
}

function snapshotRateLimitStore(store: D1RateLimitStore): RateLimitStore {
  return Object.freeze({
    increment: Object.freeze(d1IncrementMethod.bind(store)),
  });
}

function assertUnchangedSurface(
  connectorId: string,
  name: keyof ConnectorPolicies,
  current: unknown,
  baseline: unknown,
): void {
  if (current !== baseline) {
    throw new TypeError(
      `connector ${connectorId}: single-tenant preset ${name} was replaced, removed, or added after validation`,
    );
  }
}

/**
 * Build a frozen connector-policy set for one physically isolated deployment.
 * Manifest-dependent requirements are enforced later by `createConnector()`.
 */
export function singleTenantConnectorPolicies(
  options: SingleTenantConnectorPoliciesOptions,
): SingleTenantConnectorPolicies {
  const parsed = parseOptions(options);
  const egress: NetworkEgressOptions = Object.freeze({
    allowedDomains: Object.freeze([...parsed.egress.allowedDomains]),
    ...(parsed.egress.name === undefined ? {} : { name: parsed.egress.name }),
  });
  networkEgress(egress);

  const customEvaluators = parsed.evaluators ?? [];
  if (customEvaluators.some(isTenantIsolationEvaluator)) {
    throw new TypeError(
      'singleTenantConnectorPolicies: tenantIsolation() contradicts deployment-wide connector keys',
    );
  }
  if (customEvaluators.some(isBackgroundExecutionEvaluator)) {
    throw new TypeError(
      'singleTenantConnectorPolicies: backgroundExecution() is installed by the preset and cannot be supplied as a custom evaluator',
    );
  }
  if (
    parsed.audit.mode === 'production' &&
    (!auditHasExternalSinkMethod.call(parsed.audit.logger) ||
      parsed.audit.logger.record !== auditRecordMethod ||
      parsed.audit.logger.hasExternalSink !== auditHasExternalSinkMethod)
  ) {
    throw new TypeError(
      'singleTenantConnectorPolicies: production audit logger requires an external sink',
    );
  }

  const writePermissions: WritePermissionsPolicy | undefined =
    parsed.writePermissions === undefined
      ? undefined
      : Object.freeze({
          ...(parsed.writePermissions.requireApproval === undefined
            ? {}
            : {
                requireApproval: Object.freeze([
                  ...parsed.writePermissions.requireApproval,
                ]),
              }),
          ...(parsed.writePermissions.destructiveRequiresApproval === undefined
            ? {}
            : { destructiveRequiresApproval: true as const }),
        });
  const evaluators = Object.freeze([
    ...customEvaluators.map(snapshotEvaluator),
    snapshotEvaluator(backgroundExecution()),
  ]);
  const idempotencyStore = parsed.durableStores?.idempotency
    ? snapshotIdempotencyStore(parsed.durableStores.idempotency)
    : undefined;
  const rateLimitStore = parsed.durableStores?.rateLimit
    ? snapshotRateLimitStore(parsed.durableStores.rateLimit)
    : undefined;
  const audit =
    parsed.audit.mode === 'production' ? parsed.audit.logger : undefined;
  const auditMember = audit === undefined ? undefined : auditRecordMethod;
  const auditRecord = audit
    ? Object.freeze(auditRecordMethod.bind(audit))
    : undefined;
  const enforcedPolicies: Readonly<ConnectorPolicies> = Object.freeze({
    networkEgress: egress,
    ...(writePermissions === undefined ? {} : { writePermissions }),
    evaluators,
    ...(idempotencyStore === undefined ? {} : { idempotencyStore }),
    ...(rateLimitStore === undefined ? {} : { rateLimitStore }),
    ...(audit === undefined ? {} : { audit }),
    ...(parsed.fetch === undefined ? {} : { fetch: parsed.fetch }),
  });
  const snapshot: SingleTenantPolicySnapshot = Object.freeze({
    policies: enforcedPolicies,
    ...(auditMember === undefined ? {} : { auditMember }),
    ...(auditRecord === undefined ? {} : { auditRecord }),
  });
  const metadata: SingleTenantPresetMetadata = Object.freeze({
    kind: 'single-tenant',
    auditMode: parsed.audit.mode,
    principalPermissions: parsed.permissions.principalPermissions,
    snapshot,
  });
  issuedPresetMetadata.add(metadata);
  const policies: SingleTenantConnectorPolicies = Object.freeze({
    ...enforcedPolicies,
    [singleTenantPreset]: metadata,
  });
  return policies;
}

/** @internal Apply the preset's manifest-dependent construction checks. */
export function assertSingleTenantConnectorPolicies(
  connectorId: string,
  manifest: PermissionManifest,
  policies: ConnectorPolicies,
): ValidatedConnectorPolicies {
  const metadata = presetMetadata(policies);
  if (!metadata) {
    const audit = policies.audit;
    return {
      policies,
      ...(audit === undefined ? {} : { auditRecord: audit.record.bind(audit) }),
    };
  }

  const currentNetworkEgress = policies.networkEgress;
  const currentWritePermissions = policies.writePermissions;
  const currentEvaluators = policies.evaluators;
  const currentIdempotencyStore = policies.idempotencyStore;
  const currentRateLimitStore = policies.rateLimitStore;
  const currentAudit = policies.audit;
  const currentFetch = policies.fetch;
  const baseline = metadata.snapshot.policies;
  assertUnchangedSurface(
    connectorId,
    'networkEgress',
    currentNetworkEgress,
    baseline.networkEgress,
  );
  assertUnchangedSurface(
    connectorId,
    'writePermissions',
    currentWritePermissions,
    baseline.writePermissions,
  );
  assertUnchangedSurface(
    connectorId,
    'evaluators',
    currentEvaluators,
    baseline.evaluators,
  );
  assertUnchangedSurface(
    connectorId,
    'idempotencyStore',
    currentIdempotencyStore,
    baseline.idempotencyStore,
  );
  assertUnchangedSurface(
    connectorId,
    'rateLimitStore',
    currentRateLimitStore,
    baseline.rateLimitStore,
  );
  assertUnchangedSurface(connectorId, 'audit', currentAudit, baseline.audit);
  assertUnchangedSurface(connectorId, 'fetch', currentFetch, baseline.fetch);
  if (
    currentAudit !== undefined &&
    currentAudit.record !== metadata.snapshot.auditMember
  ) {
    throw new TypeError(
      `connector ${connectorId}: single-tenant preset audit.record changed after validation`,
    );
  }

  const networkEgressPolicy = baseline.networkEgress;
  if (!networkEgressPolicy) {
    throw new TypeError(
      `connector ${connectorId}: single-tenant preset requires an organization egress policy`,
    );
  }
  for (const declared of manifest.egress ?? []) {
    if (!egressDomainAllowed(declared, networkEgressPolicy.allowedDomains)) {
      throw new TypeError(
        `connector ${connectorId}: declared egress '${declared}' is outside the single-tenant preset organization allowlist`,
      );
    }
  }

  if (
    metadata.auditMode === 'production' &&
    (baseline.audit === undefined ||
      metadata.snapshot.auditRecord === undefined)
  ) {
    throw new TypeError(
      `connector ${connectorId}: single-tenant production preset requires an audit logger with an external sink`,
    );
  }
  if (manifest.idempotencyKey && !baseline.idempotencyStore) {
    throw new TypeError(
      `connector ${connectorId}: single-tenant preset requires D1IdempotencyStore when permissions.idempotencyKey is enabled`,
    );
  }
  if (manifest.rateLimit !== undefined && !baseline.rateLimitStore) {
    throw new TypeError(
      `connector ${connectorId}: single-tenant preset requires D1RateLimitStore when permissions.rateLimit is declared`,
    );
  }
  if (
    manifest.requiredPermissions !== undefined &&
    metadata.principalPermissions !== 'configured'
  ) {
    throw new TypeError(
      `connector ${connectorId}: permissions.requiredPermissions requires single-tenant principal-permissions wiring`,
    );
  }

  return Object.freeze({
    policies: baseline,
    ...(metadata.snapshot.auditRecord === undefined
      ? {}
      : { auditRecord: metadata.snapshot.auditRecord }),
  });
}
