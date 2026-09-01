// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { domainToASCII } from 'node:url';
import {
  CLOUDFLARE_INVENTORY_BOUND,
  inventoryBoundExceeded,
} from './cloudflare-client-config.js';
import { MAX_DATABASE_INVENTORY } from './cloudflare-ordinary-worker-operations.js';
import {
  type CloudflareWorkerAttachmentScanContext,
  listDispatchScriptPage,
} from './cloudflare-worker-attachment-scan.js';
// The 9..1,000 provider-request contract and its refusal bytes are shared with
// the attachment scanner; duplicating the message would let the two drift.
import { assertWorkerAttachmentProviderRequestBudget } from './cloudflare-worker-attachment-scan-state.js';
import {
  assertInventoryFindingValue,
  assertNoCredentialInInventoryText,
  type FleetInventoryDeploymentFactKind,
  type FleetInventoryRowKind,
  type FleetInventoryRunOptions,
  type FleetInventoryStage,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  type FleetInventoryStageInput,
  type FleetInventoryStageResult,
  type FleetInventoryStageStep,
  fleetInventoryStageFromUnknown,
  fleetInventoryStageKey,
  isInventoryKeyNameShape,
  nextStage,
} from './fleet-inventory-state.js';
import {
  type HostRoutingTarget,
  parseHostRoutingTarget,
} from './host-routing.js';
import { canonicalDeploymentEgressPolicy } from './platform-resources.js';
import type { FleetInventoryFinding, WorkerZoneRoute } from './types.js';

/**
 * The registry key prefix and fleet tag are private to the Cloudflare client
 * today. The engine must not import the client (the one-way layer rule), so it
 * carries the same two literals; a byte change in either place is a behavior
 * change that the golden drain baseline catches.
 */
const SCRIPT_INVENTORY_PREFIX = '__anchorage_script__:';
const FLEET_SCRIPT_TAG = 'fleet:anchorage';
const DISPATCH_PAGE_SIZE = 1_000;
const DISPATCH_PAGE_BOUND = 100;
const R2_PAGE_SIZE = 1_000;
const R2_JURISDICTIONS = Object.freeze(['default', 'eu', 'fedramp'] as const);
const NON_ASCII = /[^\p{ASCII}]/u;

/** The frozen finding vocabulary every staged finding row must name. */
type FleetInventoryFindingKind = FleetInventoryFinding['kind'];

/** One R2 jurisdiction, in today's fixed encounter order. */
export type FleetInventoryR2Jurisdiction = (typeof R2_JURISDICTIONS)[number];

/** A provider binding as the account API returns it. */
export interface FleetInventoryProviderBinding {
  readonly type?: string;
  readonly name?: string;
  readonly text?: string;
  readonly database_id?: string;
  readonly namespace_id?: string;
  readonly class_name?: string;
  readonly script_name?: string;
  readonly dispatch_namespace?: string;
  readonly service?: string;
  readonly entrypoint?: string;
  readonly queue_name?: string;
  readonly bucket_name?: string;
}

/** One page of host-routing KV key names. */
export interface FleetInventoryKeyPage {
  readonly keys: readonly Readonly<{ name?: string }>[];
  readonly cursor?: string;
}

/** One page of Worker custom domains. */
export interface FleetInventoryDomainPage {
  readonly domains: readonly Readonly<{ hostname: string; service: string }>[];
  readonly cursor?: string;
}

/** One page of Worker zone routes for a single zone. */
export interface FleetInventoryZoneRoutePage {
  readonly routes: readonly Readonly<{
    id?: string;
    pattern?: string;
    script?: string;
  }>[];
  readonly cursor?: string;
}

/** One page of ordinary Worker scripts. */
export interface FleetInventoryScriptPage {
  readonly scripts: readonly Readonly<{ id?: string }>[];
  readonly cursor?: string;
}

/** One page of D1 databases. */
export interface FleetInventoryDatabasePage {
  readonly databases: readonly Readonly<{ uuid?: string; name?: string }>[];
  readonly cursor?: string;
}

/** One page of Durable Object namespaces. */
export interface FleetInventoryNamespacePage {
  readonly namespaces: readonly Readonly<{ id?: string; script?: string }>[];
  readonly cursor?: string;
}

/** One page of R2 buckets inside one jurisdiction. */
export interface FleetInventoryBucketPage {
  readonly buckets: readonly Readonly<{
    name?: string;
    jurisdiction?: string;
    creation_date?: string;
  }>[];
}

/** Dispatch namespace attestation fields, as the provider returns them. */
export interface FleetInventoryDispatchNamespace {
  readonly namespace_name?: string;
  readonly namespace_id?: string;
  readonly trusted_workers?: boolean;
  readonly script_count?: number;
}

/** Live dispatch Worker inspection, mirroring `inspectDispatchWorker`. */
export interface FleetInventoryDispatchWorker {
  readonly artifactVersion: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly schemaVersion: number;
  readonly desiredSpecDigest: string;
  readonly databaseIds: readonly string[];
  readonly durableObjectBindings: readonly Readonly<{
    name: string;
    className: string;
    namespaceId: string;
    scriptName?: string;
    dispatchNamespace?: string;
  }>[];
  readonly serviceBindings: readonly Readonly<{
    name: string;
    service: string;
    entrypoint?: string;
  }>[];
  readonly queueProducerBindings: readonly Readonly<{
    name: string;
    queueName: string;
  }>[];
  readonly r2BucketBindings: readonly Readonly<{
    name: string;
    bucketName: string;
    jurisdiction: string;
  }>[];
  readonly secretNames: readonly string[];
  readonly plainTextBindings: Readonly<Record<string, string>>;
}

/**
 * One ordinary Worker's active artifact. The dependency resolves the exact
 * active version and refuses unsupported bindings, exactly as the single-pass
 * drain does inside its per-script `try`, so a refusal becomes the
 * `incomplete-deployment` finding rather than aborting the run.
 */
export interface FleetInventoryOrdinaryScriptDetail {
  readonly artifactVersion: string;
  readonly bindings: readonly FleetInventoryProviderBinding[];
  readonly subdomainEnabled: boolean;
  readonly previewsEnabled: boolean;
  readonly secretNames: readonly string[];
}

/**
 * The narrow provider seam the bounded inventory engine drives. Each member is
 * one provider operation and is charged one request against
 * `maxProviderRequests`; composite members issue their own inner calls exactly
 * as the single-pass drain does.
 *
 * A bounded chunk receives no staged rows, so a stage that needs an earlier
 * stage's provider data re-reads it through these members. Implementations MAY
 * memoize a listing for the lifetime of one in-memory drain, which is how the
 * drain keeps today's request sequence.
 */
export interface CloudflareFleetInventoryDeps {
  /**
   * Context for the reused `listDispatchScriptPage`; the engine never
   * duplicates dispatch pagination. Stage: `dispatch-pages` (and the
   * re-reads in `registration-checks`/`registration-postprocess`).
   */
  readonly attachmentScan: CloudflareWorkerAttachmentScanContext;
  /**
   * Configured dispatch namespace, refusing with today's plane-capability
   * error when absent. Stages: `dispatch-pages`,
   * `registration-postprocess`.
   */
  dispatchNamespace(): string;
  /**
   * True for the plane-capability refusal, which
   * `registration-checks` rethrows instead of recording a finding.
   */
  isDispatchCapabilityError(error: unknown): boolean;
  /** Stages: `host-kv-keys`, `host-kv-values` (key re-read). */
  listHostRoutingKeys(
    input: Readonly<{
      namespaceId: string;
      cursor?: string;
      signal?: AbortSignal;
    }>,
  ): Promise<FleetInventoryKeyPage>;
  /** Stage: `host-kv-values`. The dependency applies today's key casing. */
  readHostRoutingValue(
    input: Readonly<{
      namespaceId: string;
      keyName: string;
      signal?: AbortSignal;
    }>,
  ): Promise<string | undefined>;
  /** Stage: `registration-checks`. */
  inspectDispatchWorker(
    input: Readonly<{ scriptName: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryDispatchWorker | undefined>;
  /** Stage: `registration-postprocess`. */
  getDispatchNamespace(
    input: Readonly<{ namespace: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryDispatchNamespace>;
  /**
   * Stages: `custom-domains`, `ordinary-script-detail` (route hostnames),
   * `route-claims`.
   */
  listCustomDomains(
    input: Readonly<{ cursor?: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryDomainPage>;
  /**
   * Token verification plus account-wide zone discovery. Stages:
   * `zone-authority`, `zone-routes`, `ordinary-script-detail`,
   * `route-claims`.
   */
  listWorkerRouteZoneIds(
    input: Readonly<{ signal?: AbortSignal }>,
  ): Promise<readonly string[]>;
  /**
   * Stages: `zone-routes`, `ordinary-script-detail`, `route-claims`.
   */
  listZoneRoutes(
    input: Readonly<{ zoneId: string; cursor?: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryZoneRoutePage>;
  /** Stages: `ordinary-scripts`, `ordinary-script-detail`, `route-claims`. */
  listOrdinaryScripts(
    input: Readonly<{ cursor?: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryScriptPage>;
  /** Stages: `ordinary-script-detail`, `route-claims` (plain identities). */
  readOrdinaryScriptDetail(
    input: Readonly<{ scriptName: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryOrdinaryScriptDetail>;
  /** Stage: `d1-databases`. */
  listDatabases(
    input: Readonly<{ cursor?: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryDatabasePage>;
  /** Stage: `do-namespaces`. */
  listDurableObjectNamespaces(
    input: Readonly<{ cursor?: string; signal?: AbortSignal }>,
  ): Promise<FleetInventoryNamespacePage>;
  /** Stage: `r2-buckets`. */
  listR2Buckets(
    input: Readonly<{
      jurisdiction: FleetInventoryR2Jurisdiction;
      namePrefix: string;
      startAfter?: string;
      signal?: AbortSignal;
    }>,
  ): Promise<FleetInventoryBucketPage>;
}

/** Fixed refusal when a same-stage page re-read no longer matches its digest. */
export class CloudflareFleetInventoryCursorDriftError extends Error {
  constructor(readonly step: FleetInventoryStageStep) {
    super(
      `fleet inventory stage '${step}' page changed between bounded chunks`,
    );
    this.name = 'CloudflareFleetInventoryCursorDriftError';
  }
}

/**
 * Fixed refusal when one chunk of a stage cannot complete inside
 * `maxProviderRequests`. Stages without a resumption ordinal or cursor must
 * finish in one chunk, and a chunk that could make no progress fails closed
 * rather than looping forever.
 */
export class CloudflareFleetInventoryBudgetError extends Error {
  constructor(readonly step: FleetInventoryStageStep) {
    super(
      `fleet inventory stage '${step}' cannot complete one chunk within its provider request budget`,
    );
    this.name = 'CloudflareFleetInventoryBudgetError';
  }
}

/** Fixed refusal when a provider listing repeats its resumption cursor. */
export class CloudflareFleetInventoryCursorError extends Error {
  constructor(readonly step: FleetInventoryStageStep) {
    super(`fleet inventory stage '${step}' repeated a provider cursor`);
    this.name = 'CloudflareFleetInventoryCursorError';
  }
}

interface HostRoutingRegistration {
  readonly scriptName: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly databaseId: string;
  readonly routeHostname: string;
  readonly keyOwned: boolean;
}

interface HostRegistryRoute {
  readonly hostname: string;
  readonly scriptName: string;
  readonly tenantTag: string;
  readonly environment: string;
}

interface DispatchScript {
  readonly id: string;
  readonly tags: readonly string[];
}

interface MatchedZoneRoute extends WorkerZoneRoute {
  readonly scriptName: string;
}

interface PlainIdentity {
  readonly tenantTag: string;
  readonly environment: string;
}

function sha256Hex(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function tagValue(tags: readonly string[], prefix: string): string | undefined {
  return tags.find((tag) => tag.startsWith(prefix))?.slice(prefix.length);
}

/**
 * Hostnames reach the durable controls as ASCII, while the finding detail keeps
 * today's exact bytes: an ASCII value is never rewritten, so only a genuine IDN
 * value is punycoded for validation.
 */
function asciiHost(value: string): string {
  if (!NON_ASCII.test(value)) return value;
  const ascii = domainToASCII(value);
  return ascii === '' ? value : ascii;
}

function detailValue(value: string, field: string): string {
  assertInventoryFindingValue(value, field);
  return value;
}

function detailHost(value: string, field: string): string {
  assertInventoryFindingValue(asciiHost(value), field);
  return value;
}

class RequestBudget {
  #used = 0;

  constructor(
    private readonly maximum: number,
    private readonly step: FleetInventoryStageStep,
  ) {}

  get used(): number {
    return this.#used;
  }

  get available(): boolean {
    return this.#used < this.maximum;
  }

  /** Charges one provider request, failing closed at the caller's budget. */
  spend(): void {
    if (!this.available) {
      throw new CloudflareFleetInventoryBudgetError(this.step);
    }
    this.#used += 1;
  }
}

/**
 * Page identity for a resumed chunk: the digest of the first page this chunk
 * consumed must match the digest the previous chunk of the SAME stage position
 * persisted, otherwise the offset must not advance.
 */
class PageIdentity {
  #digest: string | undefined;

  constructor(
    private readonly expected: string | undefined,
    private readonly step: FleetInventoryStageStep,
  ) {}

  get digest(): string | undefined {
    return this.#digest;
  }

  observe(parts: readonly unknown[]): void {
    if (this.#digest !== undefined) return;
    const digest = sha256Hex(JSON.stringify(parts));
    this.#digest = digest;
    if (this.expected !== undefined && this.expected !== digest) {
      throw new CloudflareFleetInventoryCursorDriftError(this.step);
    }
  }
}

class StagedRowSink {
  readonly rows: FleetInventoryStagedRow[] = [];
  readonly facts: FleetInventoryStagedFact[] = [];
  readonly #counts: Record<FleetInventoryRowKind, number>;
  readonly #factOrdinals = new Map<string, number>();

  constructor(counts: Readonly<Record<FleetInventoryRowKind, number>>) {
    this.#counts = { ...counts };
  }

  count(kind: FleetInventoryRowKind): number {
    return this.#counts[kind];
  }

  get counts(): Readonly<Record<FleetInventoryRowKind, number>> {
    return { ...this.#counts };
  }

  add(
    kind: FleetInventoryRowKind,
    payload: Readonly<Record<string, unknown>>,
  ): number {
    const ordinal = this.#counts[kind];
    this.#counts[kind] = ordinal + 1;
    this.rows.push({ kind, ordinal, payload });
    return ordinal;
  }

  fact(
    deploymentOrdinal: number,
    factKind: FleetInventoryDeploymentFactKind,
    payload: Readonly<Record<string, unknown>>,
  ): void {
    const key = `${deploymentOrdinal}:${factKind}`;
    const factOrdinal = this.#factOrdinals.get(key) ?? 0;
    this.#factOrdinals.set(key, factOrdinal + 1);
    this.facts.push({ deploymentOrdinal, factKind, factOrdinal, payload });
  }

  finding(
    kind: FleetInventoryFindingKind,
    tenantTag: string,
    environment: string,
    detail: string,
  ): void {
    this.add('finding', {
      record: 'finding',
      tenantTag,
      environment,
      kind,
      detail,
    });
  }
}

interface StageContext {
  readonly deps: CloudflareFleetInventoryDeps;
  readonly options: FleetInventoryRunOptions;
  readonly budget: RequestBudget;
  readonly identity: PageIdentity;
  readonly sink: StagedRowSink;
  readonly diagnostics: string[];
  readonly signal?: AbortSignal;
}

function checkSignal(signal: AbortSignal | undefined): void {
  signal?.throwIfAborted();
}

/** Sanitized, call-local provider diagnostic; never durable. */
function diagnostic(
  context: StageContext,
  label: string,
  error: unknown,
): void {
  context.diagnostics.push(`${label}: ${String(error)}`);
}

async function hostRoutingKeyNames(
  context: StageContext,
): Promise<readonly (string | undefined)[]> {
  const namespaceId = context.options.hostRoutingKvId;
  if (namespaceId === undefined) return [];
  const names: (string | undefined)[] = [];
  let cursor: string | undefined;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listHostRoutingKeys({
      namespaceId,
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    for (const key of page.keys) {
      names.push(
        key.name === undefined || key.name === '' ? undefined : key.name,
      );
      if (names.length > CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'host-routing KV key inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
    }
    if (!page.cursor) return names;
    if (seen.has(page.cursor)) {
      throw new CloudflareFleetInventoryCursorError('host-kv-keys');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
  }
}

interface ParsedHostRoutingKey {
  readonly registration?: HostRoutingRegistration;
  readonly route?: HostRegistryRoute & {
    readonly policy: ReturnType<typeof canonicalDeploymentEgressPolicy>;
    readonly stateEgress?: HostRoutingTarget['stateEgress'];
  };
  readonly finding?: Readonly<{
    kind: FleetInventoryFindingKind;
    tenantTag: string;
    environment: string;
    detail: string;
  }>;
}

/**
 * Reproduces the drain's per-key classification for one host-routing KV key,
 * including its finding vocabulary and the order of its refusals. The raw key
 * name is untrusted input, so it faces the credential control first and then
 * the shape predicate, whose failure takes the ordinal fallback rather than
 * aborting the account inventory.
 */
async function classifyHostRoutingKey(
  context: StageContext,
  keyOrdinal: number,
  keyName: string,
): Promise<ParsedHostRoutingKey> {
  const namespaceId = context.options.hostRoutingKvId;
  if (namespaceId === undefined) return {};
  const isRegistration = keyName.startsWith(SCRIPT_INVENTORY_PREFIX);
  const registeredName = isRegistration
    ? keyName.slice(SCRIPT_INVENTORY_PREFIX.length)
    : undefined;
  assertNoCredentialInInventoryText(keyName, 'hostRoutingKey');
  const safeKeyName = isInventoryKeyNameShape(keyName);
  const unsafeName = (): ParsedHostRoutingKey => ({
    finding: {
      kind: 'malformed-script-registration',
      tenantTag: 'unknown',
      environment: 'unknown',
      detail: `script inventory key at ordinal ${keyOrdinal} has an unsafe name`,
    },
  });
  checkSignal(context.signal);
  context.budget.spend();
  const serialized = await context.deps.readHostRoutingValue({
    namespaceId,
    keyName,
    ...(context.signal ? { signal: context.signal } : {}),
  });
  if (!safeKeyName) return unsafeName();
  const key = detailHost(keyName, 'hostRoutingKey');
  if (serialized === undefined) {
    return {
      finding: {
        kind: isRegistration ? 'stale-script-registration' : 'stale-route',
        tenantTag: 'unknown',
        environment: 'unknown',
        detail: `fleet inventory key '${key}' disappeared while it was being read`,
      },
    };
  }
  let value: unknown;
  try {
    value = JSON.parse(serialized);
  } catch {
    return {
      finding: {
        kind: isRegistration
          ? 'malformed-script-registration'
          : 'malformed-route',
        tenantTag: 'unknown',
        environment: 'unknown',
        detail: `fleet inventory key '${key}' is not valid JSON`,
      },
    };
  }
  if (!value || typeof value !== 'object') {
    return {
      finding: {
        kind: isRegistration
          ? 'malformed-script-registration'
          : 'malformed-route',
        tenantTag: 'unknown',
        environment: 'unknown',
        detail: `fleet inventory key '${key}' is not an object`,
      },
    };
  }
  const candidate = value as Record<string, unknown>;
  const claimedTenant =
    typeof candidate.tenantTag === 'string' ? candidate.tenantTag : 'unknown';
  const claimedEnvironment =
    typeof candidate.environment === 'string'
      ? candidate.environment
      : 'unknown';
  if (isRegistration) {
    if (
      typeof candidate.scriptName !== 'string' ||
      typeof candidate.tenantTag !== 'string' ||
      typeof candidate.environment !== 'string' ||
      typeof candidate.databaseId !== 'string' ||
      typeof candidate.routeHostname !== 'string'
    ) {
      return {
        finding: {
          kind: 'malformed-script-registration',
          tenantTag: claimedTenant,
          environment: claimedEnvironment,
          detail: `script inventory key '${key}' has incomplete ownership metadata`,
        },
      };
    }
    if (
      !candidate.scriptName.startsWith(context.options.scriptNamePrefix) &&
      !registeredName?.startsWith(context.options.scriptNamePrefix)
    ) {
      return {};
    }
    const keyOwned = registeredName === candidate.scriptName;
    const registration: HostRoutingRegistration = {
      scriptName: candidate.scriptName,
      tenantTag: candidate.tenantTag,
      environment: candidate.environment,
      databaseId: candidate.databaseId,
      routeHostname: candidate.routeHostname,
      keyOwned,
    };
    if (keyOwned) return { registration };
    return {
      registration,
      finding: {
        kind: 'stale-script-registration',
        tenantTag: candidate.tenantTag,
        environment: candidate.environment,
        detail: `script inventory key '${key}' claims '${detailValue(
          candidate.scriptName,
          'scriptName',
        )}'`,
      },
    };
  }
  if (
    typeof candidate.scriptName !== 'string' ||
    typeof candidate.tenantTag !== 'string' ||
    typeof candidate.environment !== 'string' ||
    typeof candidate.policyId !== 'string' ||
    typeof candidate.policyDigest !== 'string' ||
    !Array.isArray(candidate.policyHosts) ||
    candidate.policyHosts.some((host) => typeof host !== 'string')
  ) {
    return {
      finding: {
        kind: 'malformed-route',
        tenantTag: claimedTenant,
        environment: claimedEnvironment,
        detail: `host route '${key}' has incomplete ownership metadata`,
      },
    };
  }
  let policy: ReturnType<typeof canonicalDeploymentEgressPolicy>;
  try {
    policy = canonicalDeploymentEgressPolicy({
      policyId: candidate.policyId,
      tenantTag: candidate.tenantTag,
      environment: candidate.environment,
      allowedHosts: candidate.policyHosts as string[],
    });
  } catch {
    return {
      finding: {
        kind: 'malformed-route',
        tenantTag: candidate.tenantTag,
        environment: candidate.environment,
        detail: `host route '${key}' has invalid policy metadata`,
      },
    };
  }
  if (
    candidate.policyDigest !== policy.policyDigest ||
    JSON.stringify(candidate.policyHosts) !== JSON.stringify(policy.policyHosts)
  ) {
    return {
      finding: {
        kind: 'malformed-route',
        tenantTag: candidate.tenantTag,
        environment: candidate.environment,
        detail: `host route '${key}' has inconsistent policy metadata`,
      },
    };
  }
  let stateEgress: HostRoutingTarget['stateEgress'];
  try {
    stateEgress = (await parseHostRoutingTarget(serialized)).stateEgress;
  } catch {
    return {
      finding: {
        kind: 'malformed-route',
        tenantTag: candidate.tenantTag,
        environment: candidate.environment,
        detail: `host route '${key}' has invalid state-egress metadata`,
      },
    };
  }
  if (!candidate.scriptName.startsWith(context.options.scriptNamePrefix)) {
    return {};
  }
  return {
    route: {
      hostname: keyName,
      scriptName: candidate.scriptName,
      tenantTag: candidate.tenantTag,
      environment: candidate.environment,
      policy,
      ...(stateEgress ? { stateEgress } : {}),
    },
  };
}

interface HostRoutingSnapshot {
  readonly registrations: readonly HostRoutingRegistration[];
  readonly routes: readonly HostRegistryRoute[];
}

/**
 * Re-reads the host-routing registry a later stage depends on. Findings are
 * intentionally discarded: `host-kv-values` already staged them, and a second
 * copy would change the durable finding order.
 */
async function hostRoutingSnapshot(
  context: StageContext,
): Promise<HostRoutingSnapshot> {
  const registrations: HostRoutingRegistration[] = [];
  const routes: HostRegistryRoute[] = [];
  const names = await hostRoutingKeyNames(context);
  for (const [keyOrdinal, keyName] of names.entries()) {
    if (keyName === undefined) continue;
    const parsed = await classifyHostRoutingKey(context, keyOrdinal, keyName);
    if (parsed.registration) registrations.push(parsed.registration);
    if (parsed.route) {
      routes.push({
        hostname: parsed.route.hostname,
        scriptName: parsed.route.scriptName,
        tenantTag: parsed.route.tenantTag,
        environment: parsed.route.environment,
      });
    }
  }
  return { registrations, routes };
}

async function dispatchScripts(
  context: StageContext,
): Promise<readonly DispatchScript[]> {
  if (!context.options.includeDispatchNamespace) return [];
  const namespace = context.deps.dispatchNamespace();
  const scripts: DispatchScript[] = [];
  let cursor: string | undefined;
  let pageNumber = 0;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await listDispatchScriptPage(context.deps.attachmentScan, {
      namespace,
      ...(cursor === undefined ? {} : { cursor }),
      perPage: DISPATCH_PAGE_SIZE,
      ...(context.signal ? { signal: context.signal } : {}),
    });
    scripts.push(...page.scripts);
    if (scripts.length > CLOUDFLARE_INVENTORY_BOUND) {
      throw inventoryBoundExceeded(
        'dispatch script inventory',
        CLOUDFLARE_INVENTORY_BOUND,
      );
    }
    if (!page.nextCursor) return scripts;
    pageNumber += 1;
    if (pageNumber >= DISPATCH_PAGE_BOUND) {
      throw new Error('Cloudflare dispatch script listing exceeded 100 pages');
    }
    if (seen.has(page.nextCursor)) {
      throw new Error('Cloudflare dispatch script listing repeated a cursor');
    }
    seen.add(page.nextCursor);
    cursor = page.nextCursor;
  }
}

async function customDomains(
  context: StageContext,
): Promise<readonly Readonly<{ hostname: string; service: string }>[]> {
  const matched: Readonly<{ hostname: string; service: string }>[] = [];
  let cursor: string | undefined;
  let encountered = 0;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listCustomDomains({
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    for (const domain of page.domains) {
      encountered += 1;
      if (encountered > CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'custom domain inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
      if (domain.service.startsWith(context.options.scriptNamePrefix)) {
        matched.push({ hostname: domain.hostname, service: domain.service });
      }
    }
    if (!page.cursor) return matched;
    if (seen.has(page.cursor)) {
      throw new CloudflareFleetInventoryCursorError('custom-domains');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
  }
}

async function zoneRoutesForZone(
  context: StageContext,
  zoneId: string,
): Promise<readonly MatchedZoneRoute[]> {
  const matched: MatchedZoneRoute[] = [];
  let cursor: string | undefined;
  let encountered = 0;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listZoneRoutes({
      zoneId,
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    for (const route of page.routes) {
      encountered += 1;
      if (encountered > CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'Worker zone-route inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
      if (
        route.script?.startsWith(context.options.scriptNamePrefix) &&
        route.id &&
        route.pattern
      ) {
        matched.push({
          zoneId,
          routeId: route.id,
          pattern: route.pattern,
          scriptName: route.script,
        });
      }
    }
    if (!page.cursor) return matched;
    if (seen.has(page.cursor)) {
      throw new CloudflareFleetInventoryCursorError('zone-routes');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
  }
}

async function allZoneRoutes(
  context: StageContext,
): Promise<readonly MatchedZoneRoute[]> {
  checkSignal(context.signal);
  context.budget.spend();
  const zoneIds = await context.deps.listWorkerRouteZoneIds(
    context.signal ? { signal: context.signal } : {},
  );
  const routes: MatchedZoneRoute[] = [];
  for (const zoneId of zoneIds) {
    routes.push(...(await zoneRoutesForZone(context, zoneId)));
  }
  return routes;
}

async function ordinaryScriptNames(
  context: StageContext,
): Promise<readonly string[]> {
  const matched: string[] = [];
  let cursor: string | undefined;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listOrdinaryScripts({
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    for (const script of page.scripts) {
      if (!script.id?.startsWith(context.options.scriptNamePrefix)) continue;
      matched.push(script.id);
      if (matched.length > CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'ordinary Worker script inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
    }
    if (!page.cursor) return matched;
    if (seen.has(page.cursor)) {
      throw new CloudflareFleetInventoryCursorError('ordinary-scripts');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
  }
}

interface OrdinaryDeployment {
  readonly scriptName: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly resourceRole?: 'platform-state' | 'deployment-egress';
  readonly resourceGroupId?: string;
  readonly artifactVersion: string;
  readonly desiredSpecDigest?: string;
  readonly schemaVersion: number;
  readonly databaseIds: readonly string[];
  readonly durableObjectBindings: readonly Readonly<Record<string, unknown>>[];
  readonly serviceBindings: readonly Readonly<Record<string, unknown>>[];
  readonly queueProducerBindings: readonly Readonly<Record<string, unknown>>[];
  readonly kvNamespaceBindings: readonly Readonly<Record<string, unknown>>[];
  readonly r2BucketBindings: readonly Readonly<Record<string, unknown>>[];
  readonly secretNames: readonly string[];
  readonly plainTextBindings: Readonly<Record<string, string>>;
  readonly publiclyReachable: boolean;
}

/**
 * Rebuilds one ordinary Worker's deployment from its active artifact, applying
 * the drain's identity requirement. Every refusal inside this function is the
 * refusal the drain reports as `plain Worker '<name>' could not be
 * inventoried`.
 */
function ordinaryDeployment(
  scriptName: string,
  detail: FleetInventoryOrdinaryScriptDetail,
  scriptZoneRoutes: readonly MatchedZoneRoute[],
): OrdinaryDeployment {
  const bindings = detail.bindings;
  const databaseIds = bindings.flatMap((binding) =>
    binding.type === 'd1' && binding.database_id ? [binding.database_id] : [],
  );
  const durableObjectBindings = bindings.flatMap((binding) => {
    if (
      binding.type !== 'durable_object_namespace' ||
      !binding.namespace_id ||
      !binding.name ||
      !binding.class_name
    ) {
      return [];
    }
    return [
      {
        name: binding.name,
        className: binding.class_name,
        namespaceId: binding.namespace_id,
        ...(binding.script_name ? { scriptName: binding.script_name } : {}),
        ...(binding.dispatch_namespace
          ? { dispatchNamespace: binding.dispatch_namespace }
          : {}),
      },
    ];
  });
  const serviceBindings = bindings.flatMap((binding) =>
    binding.type === 'service' && binding.name && binding.service
      ? [
          {
            name: binding.name,
            service: binding.service,
            ...(binding.entrypoint ? { entrypoint: binding.entrypoint } : {}),
          },
        ]
      : [],
  );
  const queueProducerBindings = bindings.flatMap((binding) =>
    binding.type === 'queue' && binding.name && binding.queue_name
      ? [{ name: binding.name, queueName: binding.queue_name }]
      : [],
  );
  const kvNamespaceBindings = bindings.flatMap((binding) =>
    binding.type === 'kv_namespace' && binding.name && binding.namespace_id
      ? [{ name: binding.name, namespaceId: binding.namespace_id }]
      : [],
  );
  const r2BucketBindings = bindings.flatMap((binding) =>
    binding.type === 'r2_bucket' && binding.name && binding.bucket_name
      ? [
          {
            name: binding.name,
            bucketName: binding.bucket_name,
            jurisdiction: 'default' as const,
          },
        ]
      : [],
  );
  const plainText = new Map(
    bindings.flatMap((binding) =>
      binding.type === 'plain_text'
        ? [[binding.name ?? '', binding.text ?? ''] as const]
        : [],
    ),
  );
  const tenantTag = plainText.get('DEPLOYMENT_TENANT');
  const environment = plainText.get('FLEET_ENVIRONMENT');
  const resourceRole = plainText.get('FLEET_RESOURCE_ROLE');
  const resourceGroupId = plainText.get('FLEET_RESOURCE_GROUP');
  const schemaVersion = Number(plainText.get('FLEET_SCHEMA_VERSION'));
  if (!tenantTag || !environment || !Number.isSafeInteger(schemaVersion)) {
    throw new Error('active Worker identity settings are missing');
  }
  const trusted =
    resourceRole === 'platform-state' || resourceRole === 'deployment-egress';
  const specDigest = plainText.get('FLEET_SPEC_DIGEST');
  return {
    scriptName,
    tenantTag,
    environment,
    ...(trusted ? { resourceRole, resourceGroupId } : {}),
    artifactVersion: detail.artifactVersion,
    ...(specDigest ? { desiredSpecDigest: specDigest } : {}),
    schemaVersion,
    databaseIds,
    durableObjectBindings,
    serviceBindings,
    queueProducerBindings,
    kvNamespaceBindings,
    r2BucketBindings,
    secretNames: detail.secretNames,
    plainTextBindings: Object.fromEntries(plainText),
    publiclyReachable:
      trusted &&
      (detail.subdomainEnabled ||
        detail.previewsEnabled ||
        scriptZoneRoutes.length > 0),
  };
}

async function plainIdentities(
  context: StageContext,
  names: readonly string[],
): Promise<ReadonlyMap<string, PlainIdentity>> {
  const identities = new Map<string, PlainIdentity>();
  for (const scriptName of names) {
    checkSignal(context.signal);
    context.budget.spend();
    try {
      const detail = await context.deps.readOrdinaryScriptDetail({
        scriptName,
        ...(context.signal ? { signal: context.signal } : {}),
      });
      const deployment = ordinaryDeployment(scriptName, detail, []);
      identities.set(scriptName, {
        tenantTag: deployment.tenantTag,
        environment: deployment.environment,
      });
    } catch (error) {
      // The drain records no identity for a script it could not inventory; the
      // finding for that script belongs to `ordinary-script-detail`.
      diagnostic(context, `plain Worker '${scriptName}'`, error);
    }
  }
  return identities;
}

function stageDeploymentFacts(
  sink: StagedRowSink,
  deploymentOrdinal: number,
  facts: Readonly<{
    databaseIds: readonly string[];
    durableObjectBindings: readonly Readonly<Record<string, unknown>>[];
    serviceBindings: readonly Readonly<Record<string, unknown>>[];
    queueProducerBindings: readonly Readonly<Record<string, unknown>>[];
    kvNamespaceBindings?: readonly Readonly<Record<string, unknown>>[];
    r2BucketBindings: readonly Readonly<Record<string, unknown>>[];
    secretNames: readonly string[];
    plainTextBindings: Readonly<Record<string, string>>;
    routeHostnames: readonly string[];
    zoneRoutes: readonly WorkerZoneRoute[];
  }>,
): void {
  for (const databaseId of facts.databaseIds) {
    sink.fact(deploymentOrdinal, 'database-id', { databaseId });
  }
  for (const binding of facts.durableObjectBindings) {
    sink.fact(deploymentOrdinal, 'durable-object-binding', binding);
  }
  for (const binding of facts.serviceBindings) {
    sink.fact(deploymentOrdinal, 'service-binding', binding);
  }
  for (const binding of facts.queueProducerBindings) {
    sink.fact(deploymentOrdinal, 'queue-producer-binding', binding);
  }
  for (const binding of facts.kvNamespaceBindings ?? []) {
    sink.fact(deploymentOrdinal, 'kv-binding', binding);
  }
  for (const binding of facts.r2BucketBindings) {
    sink.fact(deploymentOrdinal, 'r2-binding', binding);
  }
  for (const secretName of facts.secretNames) {
    sink.fact(deploymentOrdinal, 'secret-name', { secretName });
  }
  for (const [name, text] of Object.entries(facts.plainTextBindings)) {
    sink.fact(deploymentOrdinal, 'plain-text-binding', { name, text });
  }
  for (const hostname of facts.routeHostnames) {
    sink.fact(deploymentOrdinal, 'route-hostname', { hostname });
  }
  for (const route of facts.zoneRoutes) {
    sink.fact(deploymentOrdinal, 'zone-route', { ...route });
  }
}

async function advanceHostKvKeys(
  context: StageContext,
  stage: Readonly<{ step: 'host-kv-keys'; cursor?: string }>,
): Promise<FleetInventoryStage> {
  const namespaceId = context.options.hostRoutingKvId;
  if (namespaceId === undefined) {
    return nextStage(stage, context.options, context.sink.counts);
  }
  let cursor = stage.cursor;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listHostRoutingKeys({
      namespaceId,
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    context.identity.observe([
      'host-kv-keys',
      cursor ?? null,
      page.keys.map((key) => key.name ?? null),
      page.cursor ?? null,
    ]);
    for (const key of page.keys) {
      if (context.sink.count('registration') >= CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'host-routing KV key inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
      // The key list is the durable work list for `host-kv-values`. The raw
      // name is deliberately absent: it is untrusted input that the value
      // stage re-reads from the provider, so no hostile byte is persisted.
      context.sink.add('registration', {
        record: 'kv-key',
        named: key.name !== undefined && key.name !== '',
      });
    }
    if (!page.cursor) {
      return nextStage(stage, context.options, context.sink.counts);
    }
    if (seen.has(page.cursor) || page.cursor === cursor) {
      throw new CloudflareFleetInventoryCursorError('host-kv-keys');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
    if (!context.budget.available) return { step: 'host-kv-keys', cursor };
  }
}

async function advanceHostKvValues(
  context: StageContext,
  stage: Readonly<{ step: 'host-kv-values'; keyOrdinal: number }>,
): Promise<FleetInventoryStage> {
  const names = await hostRoutingKeyNames(context);
  context.identity.observe([
    'host-kv-values',
    stage.keyOrdinal,
    names.map((name) => name ?? null),
  ]);
  let keyOrdinal = stage.keyOrdinal;
  while (keyOrdinal < names.length) {
    if (!context.budget.available) {
      if (keyOrdinal === stage.keyOrdinal) {
        throw new CloudflareFleetInventoryBudgetError('host-kv-values');
      }
      return { step: 'host-kv-values', keyOrdinal };
    }
    const keyName = names[keyOrdinal];
    if (keyName !== undefined) {
      const parsed = await classifyHostRoutingKey(context, keyOrdinal, keyName);
      if (parsed.registration) {
        context.sink.add('registration', {
          record: 'registration',
          ...parsed.registration,
        });
      }
      if (parsed.finding) {
        context.sink.finding(
          parsed.finding.kind,
          parsed.finding.tenantTag,
          parsed.finding.environment,
          parsed.finding.detail,
        );
      }
      if (parsed.route) {
        context.sink.add('route', {
          record: 'route',
          backend: 'workers-for-platforms',
          surface: 'host-registry',
          hostname: parsed.route.hostname,
          scriptName: parsed.route.scriptName,
          tenantTag: parsed.route.tenantTag,
          environment: parsed.route.environment,
          ...parsed.route.policy,
          ...(parsed.route.stateEgress
            ? { stateEgress: parsed.route.stateEgress }
            : {}),
        });
      }
    }
    keyOrdinal += 1;
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceDispatchPages(
  context: StageContext,
  stage: Readonly<{
    step: 'dispatch-pages';
    cursor?: string;
    pageOrdinal: number;
  }>,
): Promise<FleetInventoryStage> {
  if (!context.options.includeDispatchNamespace) {
    return nextStage(stage, context.options, context.sink.counts);
  }
  const namespace = context.deps.dispatchNamespace();
  let cursor = stage.cursor;
  let pageOrdinal = stage.pageOrdinal;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await listDispatchScriptPage(context.deps.attachmentScan, {
      namespace,
      ...(cursor === undefined ? {} : { cursor }),
      perPage: DISPATCH_PAGE_SIZE,
      ...(context.signal ? { signal: context.signal } : {}),
    });
    context.identity.observe([
      'dispatch-pages',
      cursor ?? null,
      page.scripts.map((script) => [script.id, script.tags]),
      page.nextCursor ?? null,
    ]);
    for (const script of page.scripts) {
      if (context.sink.count('dispatch-script') >= CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'dispatch script inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
      context.sink.add('dispatch-script', {
        record: 'dispatch-script',
        scriptId: script.id,
        tags: [...script.tags],
      });
    }
    if (!page.nextCursor) {
      return nextStage(stage, context.options, context.sink.counts);
    }
    pageOrdinal += 1;
    if (pageOrdinal >= DISPATCH_PAGE_BOUND) {
      throw new Error('Cloudflare dispatch script listing exceeded 100 pages');
    }
    if (seen.has(page.nextCursor) || page.nextCursor === cursor) {
      throw new Error('Cloudflare dispatch script listing repeated a cursor');
    }
    seen.add(page.nextCursor);
    cursor = page.nextCursor;
    if (!context.budget.available) {
      return { step: 'dispatch-pages', cursor, pageOrdinal };
    }
  }
}

async function advanceRegistrationChecks(
  context: StageContext,
  stage: Readonly<{
    step: 'registration-checks';
    registrationOrdinal: number;
  }>,
): Promise<FleetInventoryStage> {
  const { registrations, routes } = await hostRoutingSnapshot(context);
  const scripts = await dispatchScripts(context);
  const listedByName = new Map(scripts.map((script) => [script.id, script]));
  context.identity.observe([
    'registration-checks',
    stage.registrationOrdinal,
    registrations.map((registration) => registration.scriptName),
  ]);
  const includeDispatchNamespace = context.options.includeDispatchNamespace;
  let registrationOrdinal = stage.registrationOrdinal;
  while (registrationOrdinal < registrations.length) {
    if (!context.budget.available) {
      if (registrationOrdinal === stage.registrationOrdinal) {
        throw new CloudflareFleetInventoryBudgetError('registration-checks');
      }
      return { step: 'registration-checks', registrationOrdinal };
    }
    const registration = registrations[registrationOrdinal];
    if (!registration) break;
    const scriptName = detailValue(registration.scriptName, 'scriptName');
    const listed = listedByName.get(registration.scriptName);
    if (includeDispatchNamespace && !listed) {
      context.sink.finding(
        'stale-script-registration',
        registration.tenantTag,
        registration.environment,
        `registered script '${scriptName}' is absent from the dispatch namespace listing`,
      );
    }
    if (
      includeDispatchNamespace &&
      listed &&
      (!listed.tags.includes(FLEET_SCRIPT_TAG) ||
        tagValue(listed.tags, 'tenant:') !== registration.tenantTag ||
        tagValue(listed.tags, 'environment:') !== registration.environment)
    ) {
      context.sink.finding(
        'stale-script-registration',
        registration.tenantTag,
        registration.environment,
        `registered script '${scriptName}' does not match its live fleet tags`,
      );
    }
    checkSignal(context.signal);
    context.budget.spend();
    let live: FleetInventoryDispatchWorker | undefined;
    try {
      live = await context.deps.inspectDispatchWorker({
        scriptName: registration.scriptName,
        ...(context.signal ? { signal: context.signal } : {}),
      });
    } catch (error) {
      if (context.deps.isDispatchCapabilityError(error)) throw error;
      // The durable finding carries the fixed template only; the transient
      // provider text stays in the call-local diagnostics.
      diagnostic(context, `registered script '${scriptName}'`, error);
      context.sink.finding(
        'stale-script-registration',
        registration.tenantTag,
        registration.environment,
        `registered script '${scriptName}' could not be inspected`,
      );
      registrationOrdinal += 1;
      continue;
    }
    if (!live) {
      context.sink.finding(
        'stale-script-registration',
        registration.tenantTag,
        registration.environment,
        `registered script '${scriptName}' is missing`,
      );
      registrationOrdinal += 1;
      continue;
    }
    const deploymentOrdinal = context.sink.add('deployment', {
      record: 'deployment',
      backend: 'workers-for-platforms',
      scriptName: registration.scriptName,
      tenantTag: live.tenantTag,
      environment: live.environment,
      artifactVersion: live.artifactVersion,
      desiredSpecDigest: live.desiredSpecDigest,
      schemaVersion: live.schemaVersion,
    });
    stageDeploymentFacts(context.sink, deploymentOrdinal, {
      databaseIds: live.databaseIds,
      durableObjectBindings: live.durableObjectBindings,
      serviceBindings: live.serviceBindings,
      queueProducerBindings: live.queueProducerBindings,
      r2BucketBindings: live.r2BucketBindings,
      secretNames: live.secretNames,
      plainTextBindings: live.plainTextBindings,
      routeHostnames: routes
        .filter((route) => route.scriptName === registration.scriptName)
        .map((route) => route.hostname),
      zoneRoutes: [],
    });
    const ownerMatches =
      registration.keyOwned &&
      live.tenantTag === registration.tenantTag &&
      live.environment === registration.environment &&
      live.databaseIds.length === 1 &&
      live.databaseIds[0] === registration.databaseId;
    if (!ownerMatches && registration.keyOwned) {
      context.sink.finding(
        'stale-script-registration',
        registration.tenantTag,
        registration.environment,
        `registered script '${scriptName}' does not match its live tenant, environment, or database ownership`,
      );
    }
    registrationOrdinal += 1;
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceRegistrationPostprocess(
  context: StageContext,
  stage: Readonly<{ step: 'registration-postprocess' }>,
): Promise<FleetInventoryStage> {
  const { registrations, routes } = await hostRoutingSnapshot(context);
  const scripts = await dispatchScripts(context);
  const registrationByScript = new Map(
    registrations.map((registration) => [
      registration.scriptName,
      registration,
    ]),
  );
  for (const script of scripts) {
    const registration = registrationByScript.get(script.id);
    if (registration?.keyOwned) continue;
    context.sink.finding(
      'unknown-dispatch-scripts',
      tagValue(script.tags, 'tenant:') ?? 'unknown',
      tagValue(script.tags, 'environment:') ?? 'unknown',
      `dispatch script '${detailValue(script.id, 'dispatchScript')}' has no valid owner-checked registry entry`,
    );
  }
  for (const route of routes) {
    const registration = registrationByScript.get(route.scriptName);
    if (
      !registration?.keyOwned ||
      registration.tenantTag !== route.tenantTag ||
      registration.environment !== route.environment ||
      registration.routeHostname !== route.hostname
    ) {
      context.sink.finding(
        'stale-route',
        route.tenantTag,
        route.environment,
        `host route '${detailHost(route.hostname, 'routeHostname')}' does not match its script registration owner`,
      );
    }
  }
  if (context.options.includeDispatchNamespace) {
    const namespace = context.deps.dispatchNamespace();
    const namespaceName = detailValue(namespace, 'dispatchNamespace');
    checkSignal(context.signal);
    context.budget.spend();
    const inventory = await context.deps.getDispatchNamespace({
      namespace,
      ...(context.signal ? { signal: context.signal } : {}),
    });
    const dispatchScriptCount = inventory.script_count;
    if (
      typeof dispatchScriptCount !== 'number' ||
      !Number.isSafeInteger(dispatchScriptCount) ||
      dispatchScriptCount < 0
    ) {
      throw new Error(
        `dispatch namespace '${namespaceName}' returned no valid script_count`,
      );
    }
    context.sink.add('meta', {
      record: 'dispatch-inventory',
      dispatchScriptCount,
      name: inventory.namespace_name ?? namespace,
      ...(inventory.namespace_id
        ? { namespaceId: inventory.namespace_id }
        : {}),
      trustedWorkers: inventory.trusted_workers,
      scriptCount: dispatchScriptCount,
    });
    if (
      inventory.namespace_name !== namespace ||
      inventory.trusted_workers !== false
    ) {
      context.sink.finding(
        'trusted-dispatch-namespace',
        'unknown',
        'unknown',
        `dispatch namespace '${namespaceName}' does not attest trusted_workers=false`,
      );
    }
    if (dispatchScriptCount > scripts.length) {
      context.sink.finding(
        'unknown-dispatch-scripts',
        'unknown',
        'unknown',
        `dispatch namespace '${namespaceName}' reports ${dispatchScriptCount - scripts.length} script(s) missing from the paginated listing`,
      );
    }
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceCustomDomains(
  context: StageContext,
  stage: Readonly<{ step: 'custom-domains' }>,
): Promise<FleetInventoryStage> {
  for (const domain of await customDomains(context)) {
    context.sink.add('meta', {
      record: 'custom-domain',
      hostname: domain.hostname,
      service: domain.service,
    });
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceZoneAuthority(
  context: StageContext,
  stage: Readonly<{ step: 'zone-authority' }>,
): Promise<FleetInventoryStage> {
  checkSignal(context.signal);
  context.budget.spend();
  const zoneIds = await context.deps.listWorkerRouteZoneIds(
    context.signal ? { signal: context.signal } : {},
  );
  for (const zoneId of zoneIds) {
    context.sink.add('meta', { record: 'zone', zoneId });
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceZoneRoutes(
  context: StageContext,
  stage: Readonly<{ step: 'zone-routes'; zoneOrdinal: number }>,
): Promise<FleetInventoryStage> {
  checkSignal(context.signal);
  context.budget.spend();
  const zoneIds = await context.deps.listWorkerRouteZoneIds(
    context.signal ? { signal: context.signal } : {},
  );
  context.identity.observe(['zone-routes', stage.zoneOrdinal, [...zoneIds]]);
  let zoneOrdinal = stage.zoneOrdinal;
  while (zoneOrdinal < zoneIds.length) {
    if (!context.budget.available) {
      if (zoneOrdinal === stage.zoneOrdinal) {
        throw new CloudflareFleetInventoryBudgetError('zone-routes');
      }
      return { step: 'zone-routes', zoneOrdinal };
    }
    const zoneId = zoneIds[zoneOrdinal];
    if (zoneId === undefined) break;
    for (const route of await zoneRoutesForZone(context, zoneId)) {
      context.sink.add('meta', {
        record: 'zone-route',
        zoneId: route.zoneId,
        routeId: route.routeId,
        pattern: route.pattern,
        scriptName: route.scriptName,
      });
    }
    zoneOrdinal += 1;
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceOrdinaryScripts(
  context: StageContext,
  stage: Readonly<{ step: 'ordinary-scripts'; cursor?: string }>,
): Promise<FleetInventoryStage> {
  let cursor = stage.cursor;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listOrdinaryScripts({
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    context.identity.observe([
      'ordinary-scripts',
      cursor ?? null,
      page.scripts.map((script) => script.id ?? null),
      page.cursor ?? null,
    ]);
    for (const script of page.scripts) {
      if (!script.id?.startsWith(context.options.scriptNamePrefix)) continue;
      if (context.sink.count('deployment') >= CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'ordinary Worker script inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
      // The candidate list is the durable work list for the detail stage; the
      // deployment itself is staged there, under a later ordinal.
      context.sink.add('deployment', {
        record: 'candidate-script',
        scriptName: script.id,
      });
    }
    if (!page.cursor) {
      return nextStage(stage, context.options, context.sink.counts);
    }
    if (seen.has(page.cursor) || page.cursor === cursor) {
      throw new CloudflareFleetInventoryCursorError('ordinary-scripts');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
    if (!context.budget.available) return { step: 'ordinary-scripts', cursor };
  }
}

async function advanceOrdinaryScriptDetail(
  context: StageContext,
  stage: Readonly<{ step: 'ordinary-script-detail'; scriptOrdinal: number }>,
): Promise<FleetInventoryStage> {
  const names = await ordinaryScriptNames(context);
  context.identity.observe([
    'ordinary-script-detail',
    stage.scriptOrdinal,
    [...names],
  ]);
  const domains = await customDomains(context);
  const zoneRoutes = await allZoneRoutes(context);
  let scriptOrdinal = stage.scriptOrdinal;
  while (scriptOrdinal < names.length) {
    if (!context.budget.available) {
      if (scriptOrdinal === stage.scriptOrdinal) {
        throw new CloudflareFleetInventoryBudgetError('ordinary-script-detail');
      }
      return { step: 'ordinary-script-detail', scriptOrdinal };
    }
    const scriptName = names[scriptOrdinal];
    if (scriptName === undefined) break;
    const safeScriptName = detailValue(scriptName, 'scriptName');
    const scriptZoneRoutes = zoneRoutes.filter(
      (route) => route.scriptName === scriptName,
    );
    checkSignal(context.signal);
    context.budget.spend();
    try {
      const detail = await context.deps.readOrdinaryScriptDetail({
        scriptName,
        ...(context.signal ? { signal: context.signal } : {}),
      });
      const deployment = ordinaryDeployment(
        scriptName,
        detail,
        scriptZoneRoutes,
      );
      if (deployment.publiclyReachable) {
        context.sink.finding(
          'incomplete-deployment',
          deployment.tenantTag,
          deployment.environment,
          `trusted Worker '${safeScriptName}' is publicly reachable on workers.dev, a preview URL, or a zone route`,
        );
      }
      const deploymentOrdinal = context.sink.add('deployment', {
        record: 'deployment',
        backend: 'plain-worker',
        ...(deployment.resourceRole
          ? {
              resourceRole: deployment.resourceRole,
              resourceGroupId: deployment.resourceGroupId,
            }
          : {}),
        scriptName: deployment.scriptName,
        tenantTag: deployment.tenantTag,
        environment: deployment.environment,
        artifactVersion: deployment.artifactVersion,
        ...(deployment.desiredSpecDigest
          ? { desiredSpecDigest: deployment.desiredSpecDigest }
          : {}),
        schemaVersion: deployment.schemaVersion,
      });
      stageDeploymentFacts(context.sink, deploymentOrdinal, {
        databaseIds: deployment.databaseIds,
        durableObjectBindings: deployment.durableObjectBindings,
        serviceBindings: deployment.serviceBindings,
        queueProducerBindings: deployment.queueProducerBindings,
        kvNamespaceBindings: deployment.kvNamespaceBindings,
        r2BucketBindings: deployment.r2BucketBindings,
        secretNames: deployment.secretNames,
        plainTextBindings: deployment.plainTextBindings,
        routeHostnames: domains
          .filter((domain) => domain.service === scriptName)
          .map((domain) => domain.hostname),
        zoneRoutes: scriptZoneRoutes.map(
          ({ scriptName: _scriptName, ...route }) => route,
        ),
      });
    } catch (error) {
      // Cross-stage provider drift (a script deleted after the listing) lands
      // here, exactly as the single-pass drain surfaces it.
      diagnostic(context, `plain Worker '${safeScriptName}'`, error);
      context.sink.finding(
        'incomplete-deployment',
        'unknown',
        'unknown',
        `plain Worker '${safeScriptName}' could not be inventoried`,
      );
    }
    scriptOrdinal += 1;
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceRouteClaims(
  context: StageContext,
  stage: Readonly<{ step: 'route-claims' }>,
): Promise<FleetInventoryStage> {
  const domains = await customDomains(context);
  const zoneRoutes = await allZoneRoutes(context);
  const identities = await plainIdentities(
    context,
    await ordinaryScriptNames(context),
  );
  for (const domain of domains) {
    const identity = identities.get(domain.service);
    context.sink.add('route', {
      record: 'route',
      backend: 'plain-worker',
      surface: 'custom-domain',
      hostname: domain.hostname,
      scriptName: domain.service,
      tenantTag: identity?.tenantTag ?? 'unknown',
      environment: identity?.environment ?? 'unknown',
    });
    if (!identity) {
      context.sink.finding(
        'stale-route',
        'unknown',
        'unknown',
        `custom domain '${detailHost(domain.hostname, 'customDomain')}' points to a missing or incomplete plain Worker '${detailValue(domain.service, 'scriptName')}'`,
      );
    }
  }
  for (const route of zoneRoutes) {
    const identity = identities.get(route.scriptName);
    context.sink.add('route', {
      record: 'route',
      backend: 'plain-worker',
      surface: 'zone-route',
      zoneId: route.zoneId,
      routeId: route.routeId,
      hostname: route.pattern,
      scriptName: route.scriptName,
      tenantTag: identity?.tenantTag ?? 'unknown',
      environment: identity?.environment ?? 'unknown',
    });
    context.sink.finding(
      'stale-route',
      identity?.tenantTag ?? 'unknown',
      identity?.environment ?? 'unknown',
      `zone route '${detailHost(route.pattern, 'zoneRoutePattern')}' exposes plain Worker '${detailValue(route.scriptName, 'scriptName')}'`,
    );
  }
  return nextStage(stage, context.options, context.sink.counts);
}

async function advanceDatabases(
  context: StageContext,
  stage: Readonly<{ step: 'd1-databases' }>,
): Promise<FleetInventoryStage> {
  let cursor: string | undefined;
  let encountered = 0;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listDatabases({
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    for (const database of page.databases) {
      encountered += 1;
      if (encountered > MAX_DATABASE_INVENTORY) {
        throw inventoryBoundExceeded(
          'D1 database inventory',
          MAX_DATABASE_INVENTORY,
        );
      }
      if (
        database.uuid &&
        database.name?.startsWith(context.options.databaseNamePrefix)
      ) {
        context.sink.add('database-id', {
          record: 'database-id',
          databaseId: database.uuid,
        });
      }
    }
    if (!page.cursor) {
      return nextStage(stage, context.options, context.sink.counts);
    }
    if (seen.has(page.cursor) || page.cursor === cursor) {
      throw new CloudflareFleetInventoryCursorError('d1-databases');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
  }
}

async function advanceDurableObjectNamespaces(
  context: StageContext,
  stage: Readonly<{ step: 'do-namespaces' }>,
): Promise<FleetInventoryStage> {
  const { registrations } = await hostRoutingSnapshot(context);
  const registeredScriptNames = new Set(
    registrations.map((registration) => registration.scriptName),
  );
  let cursor: string | undefined;
  let encountered = 0;
  const seen = new Set<string>();
  for (;;) {
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listDurableObjectNamespaces({
      ...(cursor === undefined ? {} : { cursor }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    for (const namespace of page.namespaces) {
      encountered += 1;
      if (encountered > CLOUDFLARE_INVENTORY_BOUND) {
        throw inventoryBoundExceeded(
          'Durable Object namespace inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
      if (
        namespace.id &&
        namespace.script &&
        (registeredScriptNames.has(namespace.script) ||
          namespace.script.startsWith(context.options.scriptNamePrefix))
      ) {
        context.sink.add('namespace-id', {
          record: 'namespace-id',
          namespaceId: namespace.id,
        });
      }
    }
    if (!page.cursor) {
      return nextStage(stage, context.options, context.sink.counts);
    }
    if (seen.has(page.cursor) || page.cursor === cursor) {
      throw new CloudflareFleetInventoryCursorError('do-namespaces');
    }
    seen.add(page.cursor);
    cursor = page.cursor;
  }
}

async function advanceR2Buckets(
  context: StageContext,
  stage: Readonly<{
    step: 'r2-buckets';
    jurisdictionOrdinal: 0 | 1 | 2;
    startAfter?: string;
  }>,
): Promise<FleetInventoryStage> {
  if (!context.options.includeR2Buckets) {
    return nextStage(stage, context.options, context.sink.counts);
  }
  let jurisdictionOrdinal = stage.jurisdictionOrdinal;
  let startAfter = stage.startAfter;
  for (;;) {
    const jurisdiction = R2_JURISDICTIONS[jurisdictionOrdinal];
    if (jurisdiction === undefined) {
      return nextStage(stage, context.options, context.sink.counts);
    }
    checkSignal(context.signal);
    context.budget.spend();
    const page = await context.deps.listR2Buckets({
      jurisdiction,
      namePrefix: context.options.scriptNamePrefix,
      ...(startAfter === undefined ? {} : { startAfter }),
      ...(context.signal ? { signal: context.signal } : {}),
    });
    const buckets = page.buckets;
    context.identity.observe([
      'r2-buckets',
      jurisdictionOrdinal,
      startAfter ?? null,
      buckets.map((bucket) => bucket.name ?? null),
    ]);
    for (const bucket of buckets) {
      if (!bucket.name?.startsWith(context.options.scriptNamePrefix)) continue;
      if (
        bucket.jurisdiction !== undefined &&
        bucket.jurisdiction !== jurisdiction
      ) {
        throw new Error(`R2 bucket '${bucket.name}' changed jurisdiction`);
      }
      if (
        !bucket.creation_date ||
        !Number.isFinite(Date.parse(bucket.creation_date))
      ) {
        throw new Error(
          `R2 bucket '${bucket.name}' has no valid creation date`,
        );
      }
      if (context.sink.count('r2-bucket') >= CLOUDFLARE_INVENTORY_BOUND) {
        // The bound counts only accepted fleet-owned buckets, not every
        // provider item scanned while filtering by prefix.
        throw inventoryBoundExceeded(
          'R2 bucket inventory',
          CLOUDFLARE_INVENTORY_BOUND,
        );
      }
      context.sink.add('r2-bucket', {
        record: 'r2-bucket',
        bucketName: bucket.name,
        jurisdiction,
        creationDate: new Date(bucket.creation_date).toISOString(),
      });
    }
    if (buckets.length < R2_PAGE_SIZE) {
      const nextOrdinal = jurisdictionOrdinal + 1;
      if (nextOrdinal > 2) {
        return nextStage(stage, context.options, context.sink.counts);
      }
      jurisdictionOrdinal = nextOrdinal as 0 | 1 | 2;
      startAfter = undefined;
    } else {
      const last = buckets.at(-1)?.name;
      if (!last || last === startAfter) {
        throw new Error('R2 bucket inventory pagination did not advance');
      }
      startAfter = last;
    }
    if (!context.budget.available) {
      return {
        step: 'r2-buckets',
        jurisdictionOrdinal,
        ...(startAfter === undefined ? {} : { startAfter }),
      };
    }
  }
}

/**
 * Executes ONE bounded chunk of the stage named by `input.stage`, reproducing
 * the single-pass drain's provider encounter order, finding vocabulary, finding
 * order, and inventory bounds. Every provider read goes through `deps`; the
 * engine holds no credential, no D1 knowledge, and no state between calls.
 *
 * Cross-stage provider drift is NOT an error: a resource that changes between
 * stages is recorded exactly as the drain would surface it, so a generation is
 * a point-in-time-per-stage snapshot rather than a globally consistent one.
 */
export async function advanceCloudflareFleetInventoryStage(
  deps: CloudflareFleetInventoryDeps,
  input: FleetInventoryStageInput,
): Promise<FleetInventoryStageResult> {
  assertWorkerAttachmentProviderRequestBudget(input.maxProviderRequests);
  const stage = fleetInventoryStageFromUnknown(input.stage);
  const budget = new RequestBudget(input.maxProviderRequests, stage.step);
  const resumed =
    fleetInventoryStageKey(stage) ===
    fleetInventoryStageKey(input.progress.stage);
  const identity = new PageIdentity(
    resumed ? input.progress.lastPageDigest : undefined,
    stage.step,
  );
  const sink = new StagedRowSink(input.progress.stagedCounts);
  const context: StageContext = {
    deps,
    options: input.options,
    budget,
    identity,
    sink,
    diagnostics: [],
    ...(input.signal ? { signal: input.signal } : {}),
  };
  checkSignal(input.signal);
  const next = await advanceStage(context, stage);
  return {
    rows: sink.rows,
    facts: sink.facts,
    nextStage: next,
    ...(identity.digest === undefined ? {} : { pageDigest: identity.digest }),
    providerRequests: budget.used,
    diagnostics: context.diagnostics,
  };
}

async function advanceStage(
  context: StageContext,
  stage: FleetInventoryStage,
): Promise<FleetInventoryStage> {
  switch (stage.step) {
    case 'host-kv-keys':
      return advanceHostKvKeys(context, stage);
    case 'host-kv-values':
      return advanceHostKvValues(context, stage);
    case 'dispatch-pages':
      return advanceDispatchPages(context, stage);
    case 'registration-checks':
      return advanceRegistrationChecks(context, stage);
    case 'registration-postprocess':
      return advanceRegistrationPostprocess(context, stage);
    case 'custom-domains':
      return advanceCustomDomains(context, stage);
    case 'zone-authority':
      return advanceZoneAuthority(context, stage);
    case 'zone-routes':
      return advanceZoneRoutes(context, stage);
    case 'ordinary-scripts':
      return advanceOrdinaryScripts(context, stage);
    case 'ordinary-script-detail':
      return advanceOrdinaryScriptDetail(context, stage);
    case 'route-claims':
      return advanceRouteClaims(context, stage);
    case 'd1-databases':
      return advanceDatabases(context, stage);
    case 'do-namespaces':
      return advanceDurableObjectNamespaces(context, stage);
    case 'r2-buckets':
      return advanceR2Buckets(context, stage);
    default:
      // `finalize` performs no provider work; the coordinator finalizes.
      return { step: 'finalize' };
  }
}
