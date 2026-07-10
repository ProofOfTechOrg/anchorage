// The showcase's explanatory copy, one module: zone descriptions, concept
// tooltips, role notes, and the per-workflow guides (steps, gates, capability
// badges, simulated-visibility notes). Copy is bound by the truthfulness
// rules: never claim a delivery happened (say logged/simulated/skipped),
// grants are derived server-side (never "sent"), numbers are the verified
// constants (3s/5s polls, 30 min refresh, ~24h sandbox, 15 min sweep, 4h SLA,
// 20/500 run caps, 5/min rate limit).

import type { NarrationZone } from '@/narration';

/** The one-line product statement (header + signed-out landing). */
export const TAGLINE =
  'Approval gates and capability grants for AI agent workflows. Runs suspend durably on Cloudflare, wait for a decision, and resume.';

export const ZONES: Record<NarrationZone, { label: string; blurb: string }> = {
  browser: {
    label: 'browser',
    blurb:
      'Happened in this tab: rendering, in-memory tokens, role switching, polling loops. Nothing here is trusted by the server.',
  },
  worker: {
    label: 'Worker',
    blurb:
      'Cloudflare Worker: verifies JWTs, resolves your tenant, enforces RBAC and SoD, mints run ids, derives grants, orchestrates resumes, owns the crons.',
  },
  do: {
    label: 'Durable Object',
    blurb:
      'A single-threaded Cloudflare compute instance, one per run. It executes steps under a per-run lock and owns the suspend/resume lifecycle.',
  },
  d1: {
    label: 'D1',
    blurb:
      'The database: run snapshots, approval records, tenant registry, demo budgets.',
  },
  cron: {
    label: 'cron',
    blurb:
      'Scheduled Worker invocations: the SLA sweep every 15 minutes escalates overdue approvals; purge crons reap expired sandboxes and old snapshots.',
  },
};

export const GLOSSARY = {
  approvalGate:
    'A workflow step that suspends the run and auto-queues an approval request. Its gated side effect cannot run until a decision mints a grant.',
  suspended:
    'The run is parked durably in its Durable Object with no compute waiting. It survives isolate restarts and moves only when a decision resumes it server-side.',
  grantDerivation:
    'On approve, the Worker recomputes approved connectors from stored records bound to exactly this suspension. A forged resume finds no grant and fails closed.',
  fingerprint:
    'suspendedAt (epoch ms) plus resumeCount (per-step resume ordinal) name this exact suspension. Approvals bind to it, so a re-suspension needs a fresh decision.',
  resumeCount:
    'Runtime-owned resume counter per step: absent on a first suspension, 1 after the first resume. The collision-free tie-breaker for grants.',
  sod: 'Whoever advanced a run into a gate cannot decide that request: the server answers 403, admins included. Each demo role is a distinct actor id, so switching works.',
  runId:
    'Minted server-side as {tenantId}_{uuid}; a client-sent runId is rejected (400). The prefix makes snapshots, DOs, and grants tenant-disjoint by construction.',
  tenantIsolation:
    "Every id, store, and budget is scoped to your tenant. Another tenant's run answers 404 rather than 403, so the API is not an existence oracle.",
  sandboxTenant:
    "Your ephemeral tenant: 'dm' + 18 hex chars, invisible to other visitors. Everything in it is purged after expiry; nothing you do here persists.",
  expiry:
    "The tenant's end of life (~24 h from first sign-in). A cron reaper purges its runs, approvals, and budget rows after a grace window.",
  actorEcho:
    "Your identity renders from the server's authenticated echo on API responses. The browser holds tokens but never decides who you are.",
  runCaps:
    'Demo budget: 20 runs per sandbox lifetime and 500 across all visitors per UTC day. Exceeding either returns 429.',
  reset:
    "Deletes ALL of your sandbox's runs and approval records server-side (admin role required). You stay signed in; the run budget is NOT refilled.",
  simulated:
    "The connector's real code path runs (grant check, audit, idempotency, limits), but no binding is configured, so the external call is skipped and its envelope logged.",
  dryRun:
    'A real pass through the deploy connector with side effects off. Needs no grant, changes nothing, and returns the preview shown at the first gate.',
  idempotency:
    'Every write carries an idempotency key. A retry with the same key returns the recorded result (replayed: true) instead of executing again.',
  rateLimit:
    'Per-tenant call budget on a connector: crm-assign allows 5/min. Your sandbox exhausting it cannot throttle any other visitor.',
  egressAllowlist:
    'The only host a connector may call (crm.example.com, deploy.example.com). Any other destination is refused before a request leaves.',
  fourGates:
    'Order on every connector call: egress allowlist → write-approval grant → idempotent replay → rate limit. All four are scoped to your tenant.',
  crossWorkflowIsolation:
    "The grant-access connector refuses a request naming another workflow's scope. It fails closed even with a valid approval.",
  destructiveClass:
    'Marked as a connector whose effect cannot be undone. Each action (deploy vs promote) carries its own idempotency key.',
  polling:
    'This UI polls run status every 3s and the queue/metrics every 5s. There is no event API; the activity feed is reconstructed from these responses.',
  artifactStore:
    'Real writes to an in-memory bucket standing in for R2. Keys look like production (workflowId/runId/name); contents vanish with the sandbox.',
} as const;

export type GlossaryKey = keyof typeof GLOSSARY;

export const ROLE_NOTES: Record<string, string> = {
  admin:
    'Starts any workflow, including access-request, and decides approvals. Still blocked by separation of duties on requests it advanced itself.',
  operator:
    'Starts 4 of the 5 workflows and edits run inputs. Cannot claim or decide approvals; reviewing is reviewer/admin work.',
  reviewer:
    'Cannot start runs. Claims, delegates, and decides approvals: the second pair of eyes this whole system exists for.',
  viewer:
    'Read-only. Sees runs, the queue, and metrics; every write it attempts returns 403.',
  builder:
    'A deploy-capable role allowed to start this workflow. The public demo mints no builder token, so only admin can start it here.',
};

/** A capability badge on the launcher card: a Token label + its hover tip. */
export interface CapabilityBadge {
  label: string;
  tip: string;
}

export interface WorkflowGuide {
  /** Step ids in definition order — MUST match the Mastra step ids, because
   * the run card highlights the step named by RunSummary.suspended. */
  steps: readonly string[];
  gateSteps: readonly string[];
  /**
   * Steps behind a `.branch()` predicate — the input decides which of them
   * actually run, so narration must never claim them unconditionally.
   */
  conditionalSteps?: readonly string[];
  connector?: string;
  capabilities: readonly CapabilityBadge[];
  /** Simulated-visibility card note (what is real vs logged here). */
  note: string;
  /** Variant note when the gate short-circuits (run never suspends). */
  shortCircuitNote?: string;
}

/**
 * The steps a start narration may TRUTHFULLY claim executed: definition-order
 * steps up to the suspended gate (or all of them when the run finished),
 * minus any branch-conditional steps — the summary can't tell which branch an
 * input matched, and over-claiming a skipped branch would be a false line.
 * Under-claiming is honest; over-claiming never is.
 */
export function claimableSteps(
  guide: WorkflowGuide | undefined,
  suspendedStep: string | undefined,
): readonly string[] | undefined {
  if (!guide) return undefined;
  let upToGate: readonly string[];
  if (suspendedStep === undefined) {
    // Terminal without a suspension: every listed (unconditional) step ran.
    upToGate = guide.steps;
  } else {
    const gateIndex = guide.steps.indexOf(suspendedStep);
    // Unknown step (guide drift): claim nothing rather than guess.
    if (gateIndex < 0) return undefined;
    upToGate = guide.steps.slice(0, gateIndex);
  }
  const unconditional = upToGate.filter(
    (step) => !guide.conditionalSteps?.includes(step),
  );
  return unconditional.length > 0 ? unconditional : undefined;
}

export const WORKFLOW_GUIDES: Record<string, WorkflowGuide> = {
  'gtm-outbound': {
    steps: [
      'researchAccounts',
      'enrichContacts',
      'generateOutreach',
      'reviewAndApprove',
      'sendOutreach',
    ],
    gateSteps: ['reviewAndApprove'],
    connector: 'outreach-email',
    capabilities: [
      { label: '1 gate', tip: GLOSSARY.approvalGate },
      { label: 'outreach-email', tip: GLOSSARY.simulated },
      { label: 'idempotent', tip: GLOSSARY.idempotency },
    ],
    note: 'The send is a Cloudflare Email Service call with no binding here, so the envelope is logged and nothing is delivered.',
  },
  'content-pipeline': {
    steps: [
      'researchTopic',
      'writeIntro',
      'writeBody',
      'writeConclusion',
      'reviewContent',
      'publishContent',
    ],
    gateSteps: ['reviewContent'],
    connector: 'publish-article',
    capabilities: [
      { label: '1 gate', tip: GLOSSARY.approvalGate },
      { label: 'publish-article', tip: GLOSSARY.artifactStore },
      { label: 'idempotent: runId:contentHash', tip: GLOSSARY.idempotency },
    ],
    note: 'Publish writes to a real artifact store keyed {workflowId}/{runId}/… (in-memory here, R2 in production). Idempotency key: runId:contentHash.',
  },
  'lead-generation': {
    steps: [
      'scoreLeads',
      'fastTrack',
      'nurture',
      'reviewHotLeads',
      'assignLeads',
    ],
    gateSteps: ['reviewHotLeads'],
    // .branch([hot, cold]) — the input decides which of these actually run.
    conditionalSteps: ['fastTrack', 'nurture'],
    connector: 'crm-assign',
    capabilities: [
      { label: '1 gate (only if hot leads)', tip: GLOSSARY.approvalGate },
      { label: 'crm-assign', tip: GLOSSARY.simulated },
      { label: 'egress: crm.example.com', tip: GLOSSARY.egressAllowlist },
      { label: 'rate 5/min', tip: GLOSSARY.rateLimit },
    ],
    note: "crm-assign may only reach crm.example.com (allowlist) at 5/min, and it's offline here, so assignments are logged, not sent.",
    shortCircuitNote:
      'No hot leads, so the gate never suspended: the run queued no approval and never called the CRM.',
  },
  'product-launch': {
    steps: [
      'validateReadiness',
      'approveLaunch',
      'executeLaunch',
      'confirmRollout',
      'completeLaunch',
    ],
    gateSteps: ['approveLaunch', 'confirmRollout'],
    connector: 'release-deploy',
    capabilities: [
      { label: '2 gates', tip: GLOSSARY.approvalGate },
      { label: 'release-deploy', tip: GLOSSARY.simulated },
      { label: 'dry-run preview', tip: GLOSSARY.dryRun },
      { label: 'destructive-class', tip: GLOSSARY.destructiveClass },
      { label: 'idempotent per action', tip: GLOSSARY.idempotency },
    ],
    note: 'release-deploy is destructive-class and idempotency-keyed per action (deploy vs promote); offline here, so both actions are logged previews of themselves.',
  },
  'access-request': {
    steps: ['requestAccess', 'approveAccess', 'grantAccess'],
    gateSteps: ['approveAccess'],
    connector: 'grant-access',
    capabilities: [
      { label: '1 gate', tip: GLOSSARY.approvalGate },
      { label: 'grant-access', tip: GLOSSARY.crossWorkflowIsolation },
      {
        label: 'cross-workflow isolation',
        tip: GLOSSARY.crossWorkflowIsolation,
      },
    ],
    note: "The access grant exists only in this run's result; no real system is touched. Cross-workflow isolation on the connector is enforced for real.",
  },
};

/**
 * Product-launch renders all three flavors in one card footer — the demo's
 * clearest teaching moment about what "permission" means here.
 */
export const DRY_RUN_TRIO_FOOTER =
  'Dry-run runs the real path with no permission and changes nothing. Simulated means the permission was granted for real and the external call was skipped. Declined means the permission was refused and nothing ran.';
