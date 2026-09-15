// SPDX-License-Identifier: Apache-2.0

import type {
  ActiveRouteAttestation,
  DeploymentSpec,
  FleetRecord,
  ObservedActiveRoute,
  ProvisioningBackend,
} from './types.js';

/**
 * The default 60-second convergence budget stays well inside the default
 * five-minute fleet lease renewal interval, so a promote path that attests
 * while holding the lease cannot spend its lease waiting on the provider. The
 * default schedule caps a single convergence at ten attempts. Each attempt
 * spends two provider reads on the plain-Worker backend or three on Workers for
 * Platforms — twenty to thirty reads at worst against the default shared
 * 1,100-request-per-five-minute window the rate coordinator fences.
 */
const DEFAULT_CONVERGENCE_BUDGET_MS = 60_000;
const DEFAULT_INITIAL_RETRY_DELAY_MS = 1_000;
const DEFAULT_MAX_RETRY_DELAY_MS = 8_000;

/**
 * The artifact version a record carries before its first release settles. It is
 * a sentinel, not a version: nothing the provider can report will ever equal
 * it, so an expectation carrying it means "no artifact expectation yet" and is
 * matched on the specification digest alone.
 *
 * Exported because settlement resolves the same sentinel when it builds the
 * key a host deduplicates on. Two copies of this string that drifted apart
 * would let one module waive the version check while the other kept `pending`
 * in the key — defeating exactly the deduplication both exist to protect.
 */
export const PENDING_ARTIFACT_VERSION = 'pending';

export class ActiveRouteAttestationError extends Error {
  /**
   * Whatever the provider did report. Always present, including when the
   * refusal is "it reported nothing", where it is empty.
   */
  readonly observed: ObservedActiveRoute;
  /**
   * The last complete attestation, present only when the provider answered
   * fully and the answer was rejected for disagreeing with the expectation.
   * A refusal raised because no attestation could be formed leaves it unset.
   */
  readonly attestation: ActiveRouteAttestation | undefined;

  constructor(
    message: string,
    observed: ObservedActiveRoute,
    options: {
      readonly cause?: unknown;
      readonly attestation?: ActiveRouteAttestation;
    } = {},
  ) {
    super(
      message,
      options.cause === undefined ? undefined : { cause: options.cause },
    );
    this.name = 'ActiveRouteAttestationError';
    this.observed = observed;
    this.attestation = options.attestation;
  }
}

export type ProviderDeployment =
  | Readonly<{
      versions?: readonly Readonly<{
        percentage?: unknown;
        version_id?: unknown;
      }>[];
    }>
  | undefined;

export function exactActiveVersionId(
  deployment: ProviderDeployment,
  context: string,
): string {
  if (!deployment || !Array.isArray(deployment.versions)) {
    throw new Error(`${context} has no current deployment`);
  }
  for (const version of deployment.versions) {
    if (
      typeof version.percentage !== 'number' ||
      !Number.isFinite(version.percentage) ||
      version.percentage < 0 ||
      version.percentage > 100 ||
      typeof version.version_id !== 'string' ||
      version.version_id.length === 0
    ) {
      throw new Error(`${context} has malformed version traffic metadata`);
    }
  }
  const onlyVersion = deployment.versions[0];
  if (
    deployment.versions.length !== 1 ||
    onlyVersion?.percentage !== 100 ||
    typeof onlyVersion.version_id !== 'string'
  ) {
    throw new Error(
      `${context} must have exactly one current version receiving 100% of traffic`,
    );
  }
  return onlyVersion.version_id;
}

/**
 * The traffic split as the provider reported it, for a refusal to carry. Only
 * well-formed entries survive: a malformed one is exactly what
 * `exactActiveVersionId` already refused over, and inventing a shape for it
 * would put a fabricated percentage into an operator-facing error.
 */
function observedTrafficSplit(
  deployment: ProviderDeployment,
): readonly Readonly<{ artifactVersion: string; percentage: number }>[] {
  return (deployment?.versions ?? []).flatMap((version) =>
    typeof version.version_id === 'string' &&
    typeof version.percentage === 'number' &&
    Number.isFinite(version.percentage)
      ? [
          {
            artifactVersion: version.version_id,
            percentage: version.percentage,
          },
        ]
      : [],
  );
}

/**
 * `exactActiveVersionId` with its refusal restated as an attestation refusal
 * carrying the split. The rule is unchanged and deliberately not relaxed: one
 * version at 100% or nothing, never the version with the largest share.
 */
export function attestedActiveVersionId(
  deployment: ProviderDeployment,
  scriptName: string,
): string {
  try {
    return exactActiveVersionId(deployment, `ordinary Worker '${scriptName}'`);
  } catch (cause) {
    throw new ActiveRouteAttestationError(
      cause instanceof Error ? cause.message : String(cause),
      {
        routedScriptName: scriptName,
        trafficSplit: observedTrafficSplit(deployment),
      },
      { cause },
    );
  }
}

/** The release identity a caller believes should be serving traffic. */
export interface ActiveRouteExpectation {
  readonly specDigest: string;
  /**
   * The provider artifact expected to be routed, or `'pending'` when the caller
   * has no artifact expectation yet and the digest alone decides the match.
   */
  readonly artifactVersion: string;
}

export interface AttestConvergedActiveRouteOptions {
  readonly clock?: () => number;
  /** Injected so a test drives the backoff without spending wall-clock time. */
  readonly sleep?: (delayMs: number) => Promise<void>;
  readonly convergenceBudgetMs?: number;
  readonly initialRetryDelayMs?: number;
  readonly maxRetryDelayMs?: number;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function attestationMatches(
  attestation: ActiveRouteAttestation,
  expected: ActiveRouteExpectation,
): boolean {
  return (
    attestation.specDigest === expected.specDigest &&
    (expected.artifactVersion === PENDING_ARTIFACT_VERSION ||
      attestation.artifactVersion === expected.artifactVersion)
  );
}

function observedFrom(
  attestation: ActiveRouteAttestation,
): ObservedActiveRoute {
  return {
    routedScriptName: attestation.physicalScriptName,
    artifactVersion: attestation.artifactVersion,
    specDigest: attestation.specDigest,
  };
}

/**
 * The retry delays this budget affords, computed up front rather than derived
 * from the clock inside the loop. The clock bound below is the honest one, but
 * it is injectable, and an injected clock that never advances would otherwise
 * turn a permanent mismatch into an unbounded loop. Two independent bounds mean
 * neither can be the only thing standing between a stuck route and a spin.
 */
function retryDelaySchedule(
  budgetMs: number,
  initialDelayMs: number,
  maxDelayMs: number,
): readonly number[] {
  const delays: number[] = [];
  let delayMs = initialDelayMs;
  let scheduledMs = 0;
  while (scheduledMs + delayMs <= budgetMs) {
    delays.push(delayMs);
    scheduledMs += delayMs;
    delayMs = Math.min(delayMs * 2, maxDelayMs);
  }
  return delays;
}

/**
 * Attest what is routed, waiting out the provider's own convergence, and fail
 * closed when it does not converge.
 *
 * Every promote path needs this same wait for the same reason: a hostname-to-
 * script mapping is eventually consistent, so the first read after a promote
 * can legitimately still answer with the release that promote just replaced.
 * Attesting once would make that ordinary lag look like drift. The wait lives
 * here, once, rather than in each caller, so no promote path can be written
 * without it.
 *
 * It is a wait, not a retry loop around a broken provider: an unconverged read
 * — a stale routed release, a route mapping that has not landed yet, a routed
 * artifact whose digest binding is not visible — is retried inside the budget,
 * and anything else propagates immediately. When the budget runs out the last
 * observation is raised as `ActiveRouteAttestationError`, never swallowed and
 * never accepted as a match.
 *
 * A route that already matches costs exactly one attestation; the backoff only
 * ever pays for a route that has not settled.
 */
export async function attestConvergedActiveRoute(
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  expected: ActiveRouteExpectation,
  options: AttestConvergedActiveRouteOptions = {},
): Promise<ActiveRouteAttestation> {
  const clock = options.clock ?? Date.now;
  const sleep =
    options.sleep ??
    ((delayMs: number) =>
      new Promise<void>((resolve) => {
        setTimeout(resolve, delayMs);
      }));
  const budgetMs = positiveInteger(
    options.convergenceBudgetMs ?? DEFAULT_CONVERGENCE_BUDGET_MS,
    'convergenceBudgetMs',
  );
  const initialDelayMs = positiveInteger(
    options.initialRetryDelayMs ?? DEFAULT_INITIAL_RETRY_DELAY_MS,
    'initialRetryDelayMs',
  );
  const maxDelayMs = positiveInteger(
    options.maxRetryDelayMs ?? DEFAULT_MAX_RETRY_DELAY_MS,
    'maxRetryDelayMs',
  );
  if (maxDelayMs < initialDelayMs) {
    throw new Error('maxRetryDelayMs must not be below initialRetryDelayMs');
  }
  const delays = retryDelaySchedule(budgetMs, initialDelayMs, maxDelayMs);
  const startedAt = clock();
  let lastAttestation: ActiveRouteAttestation | undefined;
  let lastRefusal: ActiveRouteAttestationError | undefined;
  for (let attempt = 0; ; attempt += 1) {
    lastAttestation = undefined;
    lastRefusal = undefined;
    try {
      const attestation = await backend.attestActiveRoute(spec);
      if (attestationMatches(attestation, expected)) return attestation;
      lastAttestation = attestation;
    } catch (error) {
      if (!(error instanceof ActiveRouteAttestationError)) throw error;
      lastRefusal = error;
    }
    const delayMs = delays[attempt];
    if (delayMs === undefined) break;
    if (clock() - startedAt + delayMs > budgetMs) break;
    await sleep(delayMs);
  }
  const unconverged = `active route for '${spec.tenantTag}:${spec.environment}' did not converge to specification digest '${expected.specDigest}' within ${budgetMs}ms`;
  if (lastAttestation) {
    throw new ActiveRouteAttestationError(
      `${unconverged}: '${spec.routeHostname}' still routes artifact '${lastAttestation.artifactVersion}' of '${lastAttestation.physicalScriptName}'`,
      observedFrom(lastAttestation),
      { attestation: lastAttestation },
    );
  }
  // The ?? fallbacks are defensive: the loop always attempts once, so one of
  // lastAttestation / lastRefusal is always set.
  throw new ActiveRouteAttestationError(
    `${unconverged}: ${lastRefusal?.message ?? 'no attestation was produced'}`,
    lastRefusal?.observed ?? {},
    { cause: lastRefusal },
  );
}

/**
 * Read what a persisted deployment is actually serving and say whether it is
 * what the record desires.
 *
 * Single-shot on purpose: convergence backoff exists to wait out a promote this
 * process just performed, and a host reading state performed none. Waiting here
 * would only spend the provider budget re-asking a question whose answer has
 * already settled.
 *
 * Reports rather than enforces. A disagreement comes back as
 * `matchesDesired: false` with the attestation that proves it, because a host
 * rendering fleet state wants to show drift, not fail on it. Only a provider
 * that cannot be attested at all throws.
 *
 * NOT for per-request status rendering. Each single-shot call spends two
 * provider reads on the plain-Worker backend or three on Workers for Platforms,
 * against a default 1,100-request-per-five-minute account-wide budget shared by
 * every provisioning operation. A status surface with any traffic at all must
 * cache these results rather than attest per request.
 */
export async function attestFleetRecordActiveRoute(input: {
  readonly record: FleetRecord;
  readonly backend: ProvisioningBackend;
  readonly spec: DeploymentSpec;
}): Promise<
  Readonly<{
    attestation: ActiveRouteAttestation;
    matchesDesired: boolean;
  }>
> {
  const attestation = await input.backend.attestActiveRoute(input.spec);
  return {
    attestation,
    matchesDesired: attestationMatches(attestation, {
      specDigest: input.record.desiredSpecDigest,
      artifactVersion: input.record.artifactVersion,
    }),
  };
}
