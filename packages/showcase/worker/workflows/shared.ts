// Shared surface for the six showcase workflow modules: the dependency bag the
// host resolves once and threads into every module, the binding-gated egress
// seam, and the one non-obvious primitive — withContextKeys — that lets a step
// inject a per-call context key (dry-run flag, idempotency key) into a connector
// call WITHOUT mutating the runtime-minted requestContext the rest of the run
// relies on.

import { RequestContext } from '@mastra/core/request-context';
import type { ToolExecutionContext } from '@mastra/core/tools';
import type {
  AtomicIdempotencyStore,
  AuditLogger,
  InspectableIdempotencyStore,
  RateLimitStore,
} from '@proofoftech/breakwater';
import type {
  ArtifactBucket,
  R2ArtifactStore,
} from '@proofoftech/flowsafe/artifacts';
import type {
  InitSource,
  RequestContextProvider,
} from '@proofoftech/flowsafe/do-runner';
import { z } from 'zod';

/**
 * The resume payload every showcase gate accepts — matches approval-api's
 * defaultResumeData ({ approved, comment?, decidedBy? }). Declared once here so
 * the six modules share one contract shape instead of hand-copying it; a change
 * to defaultResumeData's shape is then reconciled in a single place.
 */
export const showcaseResumeSchema = z.object({
  approved: z.boolean(),
  comment: z.string().optional(),
  decidedBy: z.string().optional(),
});

/**
 * Cloudflare Email Service binding (structural subset — the repo's "no
 * workers-types dep for optional bindings" convention). Declared in
 * wrangler.jsonc as `send_email`, called as `env.EMAIL.send({...})`.
 * @see https://developers.cloudflare.com/email-service/
 */
export interface EmailServiceBinding {
  send(message: {
    to: string | string[];
    from: { email: string; name?: string };
    subject: string;
    html?: string;
    text?: string;
  }): Promise<unknown>;
}

/**
 * Structural fetch (the Workers/Node global `fetch` satisfies it) so egress
 * connectors stay host-independent and Node-testable. Only the fields the
 * connectors use are modelled.
 */
export type FetchLike = (
  url: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
  },
) => Promise<{ ok: boolean; status: number; text(): Promise<string> }>;

/**
 * A binding-gated HTTP egress target. Absent on a connector's deps => that
 * connector simulates the call (records the envelope, no network). Present =>
 * the connector POSTs to `endpoint` for real. `endpoint`'s host must be within
 * the connector's declared egress allowlist or the egress gate denies.
 */
export interface EgressBinding {
  fetch: FetchLike;
  /** Absolute URL the connector POSTs to (host must match the egress allowlist). */
  endpoint: string;
  /** Optional bearer token sent as the Authorization header. */
  token?: string;
}

/** Public input to buildShowcaseRuntime — all binding-gated infra is optional. */
export interface ShowcaseDeps {
  /** `init` input: a Cloudflare env (D1 from `DB`) or `{ storage }`. */
  initInput: InitSource;
  /**
   * The grant-minting seam, built by the host from the deployment-wide
   * approval store. The provider is consulted on every start and resume leg.
   */
  grantProvider: RequestContextProvider;
  /** Connector audit sink; defaults to a buffering AuditLogger when omitted. */
  audit?: AuditLogger;
  // gtm-outbound
  email?: EmailServiceBinding;
  fromAddress?: string;
  fromName?: string;
  // content-pipeline (default: InMemoryArtifactBucket, offline-real)
  artifactBucket?: ArtifactBucket;
  // content-pipeline + product-launch (default: InMemoryIdempotencyStore)
  idempotencyStore?: AtomicIdempotencyStore & InspectableIdempotencyStore;
  /**
   * Set only after every legacy Breakwater writer sharing an injected store
   * has stopped and drained. A fresh default in-memory store is acknowledged
   * automatically because no older writer can reach it.
   */
  idempotencyKeyMigration?: 'legacy-writers-drained';
  // lead-generation (default: InMemoryRateLimitStore)
  rateLimitStore?: RateLimitStore;
  // lead-generation CRM assign egress; absent => simulated
  crm?: EgressBinding;
  // product-launch deploy egress; absent => simulated
  deploy?: EgressBinding;
}

/**
 * The RESOLVED deps a module's register() receives (ctx.deps): every store is a
 * concrete instance so modules never branch on "is it configured". The stores
 * are shared across modules — the connector SDK scopes idempotency/rate-limit
 * keys by connector id, so sharing is safe.
 */
export interface ShowcaseModuleDeps {
  email?: EmailServiceBinding;
  fromAddress: string;
  fromName: string;
  artifactStore: R2ArtifactStore;
  idempotency: AtomicIdempotencyStore & InspectableIdempotencyStore;
  idempotencyKeyMigration?: 'legacy-writers-drained';
  rateLimit: RateLimitStore;
  crm?: EgressBinding;
  deploy?: EgressBinding;
}

/**
 * Clone the run's RequestContext and set per-call keys on the copy. Cloning
 * (not a fresh RequestContext) preserves the runtime-minted grant + workflow
 * scope so the write and egress gates still see them; NOT mutating the
 * original scopes the injected key (dry-run flag, idempotency key) to THIS
 * connector call so it can never leak to a later step — a leaked dry-run flag
 * would silently turn a later real side effect into a simulation. Object-spread
 * cannot do this: RequestContext is a class instance whose `.get()`/`.set()`
 * live on the prototype, so a spread yields a plain object with no `.get()` and
 * the wrapper throws.
 */
function contextWith(
  base: unknown,
  extra: Record<string, unknown>,
): RequestContext {
  const source = base as
    | { entries?: () => Iterable<readonly [string, unknown]> }
    | undefined;
  // Merge into a plain object, then build the RequestContext from its entries —
  // the same construction the runtime uses. `extra` wins over `base`'s keys.
  const merged: Record<string, unknown> = {};
  if (source?.entries) {
    for (const [key, value] of source.entries()) merged[key] = value;
  }
  for (const [key, value] of Object.entries(extra)) merged[key] = value;
  return new RequestContext(Object.entries(merged));
}

/**
 * A createConnector() result narrowed to the one method a step invokes. The
 * Tool's `execute` returns `Promise<void | ValidationError | Out>`, so this stays
 * loose (`Promise<unknown>`) and callConnector casts the validated result.
 */
export interface ConnectorLike<In> {
  execute?: (input: In, context: ToolExecutionContext) => Promise<unknown>;
}

/**
 * Invoke a connector from a workflow step: forward the runtime-supplied
 * requestContext (carrying the minted grant) and, optionally, inject per-call
 * context keys (dry-run, idempotency) onto a non-mutating clone. Never sets the
 * grant key by hand — the provider mints it and the wrapper enforces it; a
 * forged resume reaches here with no grant and the wrapper throws (fail closed).
 */
export async function callConnector<In, Out>(
  connector: ConnectorLike<In>,
  input: In,
  requestContext: unknown,
  extraKeys?: Record<string, unknown>,
): Promise<Out> {
  if (!connector.execute) throw new Error('connector has no execute');
  const context =
    extraKeys !== undefined
      ? contextWith(requestContext, extraKeys)
      : requestContext;
  return (await connector.execute(input, {
    requestContext: context,
  } as unknown as ToolExecutionContext)) as Out;
}
