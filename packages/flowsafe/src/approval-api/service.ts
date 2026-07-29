// SPDX-License-Identifier: Apache-2.0
// ApprovalService — the queue's business rules: role authorization, CAS
// transitions, SLA escalation, audit emission, and the post-decision resume.
//
// Grant-minting trust boundary (security-threat-model.md, boundary 6): this
// service — via the store records it writes under RBAC — is the ONLY source
// of approval grants. decide() persists the decision; the grant provider
// (grants.ts) derives requestContext grants from approved records at
// start/resume. Nothing here ever reads capability data from client input.

import {
  PATH_SAFE_ID_PATTERN,
  tenantOwnsSaltedId,
} from '../do-runner/path-safe-id.js';
import type {
  ApprovalActor,
  ApprovalAuditSink,
  ApprovalNotificationEvent,
  ApprovalNotificationSink,
  ApprovalRole,
  ApprovalStreamEvent,
  ApprovalStreamSink,
} from './contract.js';
import { APPROVAL_ROLES, DECIDER_ROLES } from './contract.js';
import {
  type AutomatedExecutionPrincipal,
  canonicalAutomatedPrincipal,
  isExecutionPrincipal,
  isTrustedAutomationPrincipal,
  principalActor,
  principalAuditFields,
  type TrustedAutomationPrincipal,
} from './principal.js';
import { type ApprovalPatch, listAllApprovedForRun } from './store.js';
import type {
  SystemApprovalStore,
  TenantBoundApprovalStore,
} from './tenant-brand.js';
import {
  APPROVAL_PRIORITIES,
  type ApprovalDecision,
  type ApprovalListFilter,
  type ApprovalMetrics,
  type ApprovalRecord,
  type ApprovalResumeTarget,
  type ApprovalStatus,
  approvalCursor,
  type BatchDecideItem,
  type BatchDecideResult,
  type CreateApprovalInput,
  type DecideResult,
  MAX_APPROVAL_BATCH_DECIDE,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  type ResumeOutcome,
} from './types.js';

/** Requester failed role authorization — HTTP 403. */
export class ApprovalAuthzError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ApprovalAuthzError';
  }
}

/** No approval with that id — HTTP 404. */
export class UnknownApprovalError extends Error {
  constructor(id: string) {
    super(`no approval '${id}'`);
    this.name = 'UnknownApprovalError';
  }
}

/** CAS guard failed: the request moved to a state the action no longer applies to — HTTP 409. */
export class ApprovalConflictError extends Error {
  readonly currentStatus: ApprovalStatus;

  constructor(id: string, action: string, currentStatus: ApprovalStatus) {
    super(`cannot ${action} approval '${id}' in status '${currentStatus}'`);
    this.name = 'ApprovalConflictError';
    this.currentStatus = currentStatus;
  }
}

/** A request the caller can fix — HTTP 400. */
export class InvalidApprovalInputError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidApprovalInputError';
  }
}

/**
 * Separation-of-duties exemption. `false`/absent = SoD on (the requester can
 * never decide their own request); `true` = every decider may self-decide;
 * `{ roles }` = only actors whose role is listed (single-operator deployments
 * set e.g. `{ roles: ['admin'] }`).
 */
export type SelfDecisionPolicy = boolean | { roles: readonly ApprovalRole[] };

/**
 * Whether an actor of `role` may decide their OWN request under `policy`. Total
 * and pure — the run-router's catalog echo and the service's decide() gate
 * share this ONE definition so the SPA hint and the server verdict can't drift.
 */
export function selfDecisionExempts(
  policy: SelfDecisionPolicy | undefined,
  role: ApprovalRole,
): boolean {
  if (policy === true) return true;
  if (!policy) return false;
  return policy.roles.includes(role);
}

export interface ApprovalServiceOptions {
  /**
   * Must be tenant-bound: the service asserts every acting
   * principal's tenant against this binding, and the store's predicates are
   * what scope reads/writes. Obtain via a store factory's forTenant().
   */
  store: TenantBoundApprovalStore;
  /**
   * Structural match for breakwater AuditLogger.record — wire
   * `(event) => auditLogger.record(event)`. Must not throw; failures are
   * contained anyway (availability over export reliability, matching
   * AuditLogger's own sink policy).
   */
  audit?: ApprovalAuditSink;
  /**
   * Notification transport seam — fired once per record actually CREATED
   * (`created: true`; the idempotent re-observation of an already-open step
   * never re-notifies). Fire-and-forget: a throwing or rejecting sink is
   * contained and recorded to the audit sink as `approval.notify`/'error',
   * never failing the create. See ApprovalNotificationSink (contract.ts) for
   * the ctx.waitUntil convention on Workers hosts.
   */
  notify?: ApprovalNotificationSink;
  /**
   * Live-stream fan-out seam — fired once per SUCCESSFUL mutation
   * (create with `created: true`, claim, decide, delegate, supersede) with the
   * POST-transition record. Distinct from `notify` (reviewer transport,
   * created/escalated only): this is the same-trust intra-tenant feed a
   * per-tenant hub relays to open dashboards. Fire-and-forget with the same
   * containment as `notify`: a throwing or rejecting sink is audited as
   * `approval.stream`/'error' and never fails the mutation. See
   * ApprovalStreamSink (contract.ts) for the ctx.waitUntil convention.
   */
  stream?: ApprovalStreamSink;
  /** Applied when CreateApprovalInput.slaSeconds is absent. */
  defaultSlaSeconds?: number;
  /**
   * Resumes the run after a decision (approve AND reject — the workflow
   * learns the outcome via resumeData). Same-Worker deployments use
   * resumeViaRuntime(runtime); cross-Worker ones fetch the run's DO.
   */
  resumeRun?: (
    record: ApprovalRecord,
    decision: ApprovalDecision,
  ) => Promise<unknown>;
  /**
   * Permit the requester to decide their own request. Off by default —
   * separation of duties is the safe enterprise default. `true` exempts every
   * decider; `{ roles }` exempts only those roles (a single-operator
   * deployment sets e.g. `{ roles: ['admin'] }`). A permitted self-decision is
   * audited with `detail.selfDecision: true`.
   */
  allowSelfDecision?: SelfDecisionPolicy;
  /** Injectable clock (tests, deterministic SLA math). */
  now?: () => Date;
}

// Role policy from security-threat-model.md: reviewers decide, operators run
// the system, admins do both; every authenticated role may read. (The SLA
// sweep has no role: it is cron-owned TCB code — see sweepSLA below.)
// CAN_REVIEW IS the exported DECIDER_ROLES — one source, so the run-router's
// canSelfDecide echo and this gate can never disagree on who may decide.
const CAN_REVIEW: readonly ApprovalRole[] = DECIDER_ROLES;
const CAN_CREATE: readonly ApprovalRole[] = ['operator', 'builder', 'admin'];

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

export class ApprovalService {
  readonly #store: TenantBoundApprovalStore;
  readonly #audit?: ApprovalAuditSink;
  readonly #notify?: ApprovalNotificationSink;
  readonly #stream?: ApprovalStreamSink;
  readonly #defaultSlaSeconds?: number;
  readonly #resumeRun?: (
    record: ApprovalRecord,
    decision: ApprovalDecision,
  ) => Promise<unknown>;
  readonly #allowSelfDecision?: SelfDecisionPolicy;
  readonly #now: () => Date;

  /**
   * The tenant this service is bound to. Exposed so the host-kit bridges can
   * mint their own bookkeeping principal against it instead of making every
   * host construct one and vouch for it.
   */
  get tenantId(): string {
    return this.#store.tenantId;
  }

  constructor(options: ApprovalServiceOptions) {
    this.#store = options.store;
    this.#audit = options.audit;
    this.#notify = options.notify;
    this.#stream = options.stream;
    this.#defaultSlaSeconds = options.defaultSlaSeconds;
    this.#resumeRun = options.resumeRun;
    this.#allowSelfDecision = options.allowSelfDecision;
    this.#now = options.now ?? (() => new Date());
  }

  async create(
    input: CreateApprovalInput,
    actor: ApprovalActor,
    resumeTarget?: ApprovalResumeTarget,
  ): Promise<{ record: ApprovalRecord; created: boolean }> {
    this.#authorize(actor, CAN_CREATE, 'approval.create', 'approval');
    return this.#createAuthorized(input, actor, resumeTarget);
  }

  /**
   * File an approval on behalf of an AUTOMATED principal — the trusted entry
   * for platform bridges (suspension reconcile, agent host) that have no person
   * behind them.
   *
   * These callers used to fabricate `role: 'operator'` to satisfy CAN_CREATE.
   * They cannot simply project onto a role instead: automated principals
   * project to the least-privileged role precisely so they can never decide,
   * and `viewer` is not in CAN_CREATE. So the role gate is replaced here — not
   * widened — by a kind-and-tenant check.
   *
   * There is deliberately NO principal-taking claim/decide/delegate. Filing a
   * request is trusted platform work; deciding one is a human judgement, and an
   * automated principal approving its own request is the separation-of-duties
   * hole this whole model exists to close.
   */
  async createAsPrincipal(
    input: CreateApprovalInput,
    principal: TrustedAutomationPrincipal,
    resumeTarget?: ApprovalResumeTarget,
  ): Promise<{ record: ApprovalRecord; created: boolean }> {
    this.#authorizeAutomated(principal, 'approval.create', 'approval');
    return this.#createAuthorized(
      input,
      principalActor(principal),
      resumeTarget,
      principalAuditFields(principal),
    );
  }

  async #createAuthorized(
    input: CreateApprovalInput,
    actor: ApprovalActor,
    resumeTarget?: ApprovalResumeTarget,
    provenance?: Record<string, unknown>,
  ): Promise<{ record: ApprovalRecord; created: boolean }> {
    this.#validateCreate(input);
    const now = this.#now();
    const slaSeconds = input.slaSeconds ?? this.#defaultSlaSeconds;
    const connectors = [...(input.connectors ?? [])];
    const grantScope =
      connectors.length === 0
        ? undefined
        : input.runScoped === true
          ? ('run' as const)
          : input.toolCallId !== undefined
            ? ('tool-call' as const)
            : ('suspension' as const);
    const record: ApprovalRecord = {
      id: crypto.randomUUID(),
      // The bound store re-stamps this from its own field either way; setting
      // it here keeps the literal honest for the type and the audit trail.
      tenantId: this.#store.tenantId,
      workflowId: input.workflowId,
      runId: input.runId,
      title: input.title,
      connectors,
      priority: input.priority ?? 'normal',
      status: 'pending',
      createdAt: now.toISOString(),
      updatedAt: now.toISOString(),
    };
    if (input.stepPath !== undefined) record.stepPath = [...input.stepPath];
    if (grantScope !== undefined) record.grantScope = grantScope;
    if (input.toolCallId !== undefined) record.toolCallId = input.toolCallId;
    if (input.suspendedAt !== undefined) {
      record.suspendedAt = input.suspendedAt;
    }
    if (input.resumedAt !== undefined) {
      record.resumedAt = input.resumedAt;
    }
    if (input.resumeCount !== undefined) {
      record.resumeCount = input.resumeCount;
    }
    if (input.runScoped !== undefined) record.runScoped = input.runScoped;
    if (input.summary !== undefined) record.summary = input.summary;
    if (input.payload !== undefined) record.payload = input.payload;
    if (resumeTarget !== undefined) {
      this.#validateResumeTarget(resumeTarget);
      record.resumeTarget = structuredClone(resumeTarget);
    }
    // Attribution powers the self-approval check: default to the creating
    // actor. An explicit override stays possible for system bridges — the
    // caller already holds a CAN_CREATE role either way.
    record.requestedBy = input.requestedBy ?? actor.id;
    if (slaSeconds !== undefined) {
      record.slaDeadlineAt = new Date(
        now.getTime() + slaSeconds * 1000,
      ).toISOString();
    }
    const result = await this.#store.create(record);
    this.#record(
      actor,
      'approval.create',
      `approval:${result.record.id}`,
      'allowed',
      {
        detail: {
          tenantId: result.record.tenantId,
          workflowId: result.record.workflowId,
          runId: result.record.runId,
          grantScope: result.record.grantScope,
          created: result.created,
          ...provenance,
        },
      },
    );
    // Only an actual insert notifies — the idempotent re-observation path
    // (created: false) returns the EXISTING open record, which already
    // notified when it entered the queue.
    if (result.created) {
      // `provenance` rides the subordinate events too. An automated operation
      // whose primary event carries principalKind/principalId/purpose but whose
      // notify and stream failures do not leaves exactly the gaps that make an
      // incident unreconstructable: the failures are the rows an operator reads.
      fireNotification(
        this.#notify,
        { type: 'created', record: result.record },
        (reason) =>
          this.#record(
            actor,
            'approval.notify',
            `approval:${result.record.id}`,
            'error',
            {
              reason,
              detail: { tenantId: result.record.tenantId, ...provenance },
            },
          ),
      );
      fireStreamEvent(
        this.#stream,
        { type: 'created', record: result.record },
        (reason) =>
          this.#record(
            actor,
            'approval.stream',
            `approval:${result.record.id}`,
            'error',
            {
              reason,
              detail: { tenantId: result.record.tenantId, ...provenance },
            },
          ),
      );
    }
    return result;
  }

  async get(id: string, actor: ApprovalActor): Promise<ApprovalRecord> {
    this.#authorize(actor, APPROVAL_ROLES, 'approval.read', `approval:${id}`);
    const record = await this.#store.get(id);
    if (!record) throw new UnknownApprovalError(id);
    return record;
  }

  async list(
    filter: ApprovalListFilter,
    actor: ApprovalActor,
  ): Promise<ApprovalRecord[]> {
    this.#authorize(actor, APPROVAL_ROLES, 'approval.read', 'approval');
    return this.#store.list(filter);
  }

  async claim(id: string, actor: ApprovalActor): Promise<ApprovalRecord> {
    this.#authorize(actor, CAN_REVIEW, 'approval.claim', `approval:${id}`);
    const now = this.#now().toISOString();
    // 'claimed' is deliberately not claimable — reassignment is delegate()'s
    // job, so a claim can never silently steal another reviewer's item.
    const updated = await this.#transitionOrExplain(id, 'claim', actor, {
      from: ['pending', 'escalated'],
      patch: {
        status: 'claimed',
        claimedBy: actor.id,
        claimedAt: now,
        updatedAt: now,
      },
    });
    this.#record(actor, 'approval.claim', `approval:${id}`, 'allowed');
    fireStreamEvent(
      this.#stream,
      { type: 'claimed', record: updated },
      (reason) =>
        this.#record(actor, 'approval.stream', `approval:${id}`, 'error', {
          reason,
          detail: { tenantId: updated.tenantId },
        }),
    );
    return updated;
  }

  async decide(
    id: string,
    input: { decision: ApprovalDecision; comment?: string },
    actor: ApprovalActor,
  ): Promise<DecideResult> {
    this.#authorize(actor, CAN_REVIEW, 'approval.decide', `approval:${id}`);
    this.#assertDecisionInput(input);
    // Role-scoped SoD: an exempt decider (allowSelfDecision: true, or a role
    // named in { roles }) skips the pre-read entirely; everyone else keeps
    // today's read-then-CAS self-request denial.
    if (!selfDecisionExempts(this.#allowSelfDecision, actor.role)) {
      const existing = await this.#store.get(id);
      if (!existing) throw new UnknownApprovalError(id);
      // requestedBy is immutable after create, so read-then-CAS carries no
      // TOCTOU hazard here. An ABSENT requestedBy passes this check by
      // design: create() always attributes it, so only records injected
      // straight into the store (TCB code) can lack it — a bulk-import path
      // added later must attribute requesters or accept self-decidability.
      if (existing.requestedBy === actor.id) {
        this.#record(actor, 'approval.decide', `approval:${id}`, 'denied', {
          reason: 'self-approval: decider is the requester',
        });
        throw new ApprovalAuthzError(
          'the requester cannot decide their own approval (separation of duties; set allowSelfDecision to permit)',
        );
      }
      // Cross-gate causal SoD: a reviewer who APPROVED an earlier gate of THIS
      // run advanced it to the gate now before them, so they must not also
      // decide this one. requestedBy attribution cannot carry this on its own —
      // reconcileApprovalsForSummary files the next gate as the SYSTEM actor,
      // which the requestedBy check above never blocks — so the bar is derived
      // from the run's own APPROVED history instead.
      //
      // Read the COMPLETE approved history via the shared complete-reader (the
      // same drain-all-pages helper connectorGrantsForLeg uses, so the SoD
      // bar and grant derivation can never drift): a many-gate run's
      // causally-prior approval can sit past MAX_APPROVAL_LIST_LIMIT, and a
      // single default-bounded first page would drop the newest under FIFO
      // 'created' order and fail the bar OPEN — violating the fail-closed SoD
      // MUST.
      const priorApproved = await listAllApprovedForRun(
        this.#store,
        existing.workflowId,
        existing.runId,
      );
      // APPROVED-ONLY scope: a human REJECTION also resumes the run and re-files
      // the next gate (reject -> revise -> re-review), so barring a prior
      // same-actor rejection would break the normal review cycle; querying
      // 'approved' also excludes system supersede-rejections. The causal anchor
      // priorAt <= gateAt fires for a SEQUENTIAL gate (filed after the earlier
      // approval) but never for PARALLEL gates filed together before any
      // decision, so independent same-actor gates are not over-blocked.
      // existing.createdAt is mandatory so the anchor is always present;
      // requestedBy/decidedBy/decidedAt/createdAt are immutable post-decision,
      // so this read-then-CAS carries no TOCTOU.
      //
      // gateAt is constant per call — compute it once. Fail CLOSED on an
      // unparseable stamp: `NaN <= x` and `x <= NaN` are both false, so a bare
      // `priorAt <= gateAt` would silently DROP the bar (fail OPEN) on garbage,
      // the opposite of the SoD MUST. A NaN priorAt OR a NaN gateAt therefore
      // BARS (a NaN gate bars every prior-by-this-actor — the correct
      // fail-closed direction). Unreachable today (createdAt/decidedAt are
      // server-stamped ISO), but the security bar must not rest on that.
      const gateAt = Date.parse(existing.createdAt);
      const barred = priorApproved.some((prior) => {
        if (prior.id === existing.id) return false;
        if (prior.decidedBy !== actor.id) return false;
        if (typeof prior.decidedAt !== 'string') return false;
        const priorAt = Date.parse(prior.decidedAt);
        return (
          Number.isNaN(priorAt) || Number.isNaN(gateAt) || priorAt <= gateAt
        );
      });
      if (barred) {
        this.#record(actor, 'approval.decide', `approval:${id}`, 'denied', {
          reason:
            'cross-gate separation of duties: this actor approved an earlier gate of the run',
        });
        throw new ApprovalAuthzError(
          'the reviewer who advanced this run at an earlier gate cannot also decide this gate (separation of duties; set allowSelfDecision to permit)',
        );
      }
    }
    const now = this.#now().toISOString();
    const patch: ApprovalPatch = {
      status: input.decision === 'approve' ? 'approved' : 'rejected',
      decidedBy: actor.id,
      decision: input.decision,
      decidedAt: now,
      updatedAt: now,
    };
    if (input.comment !== undefined) patch.comment = input.comment;
    const updated = await this.#transitionOrExplain(id, 'decide', actor, {
      from: OPEN_STATUSES,
      patch,
    });
    // Annotate a PERMITTED self-decision from the post-transition record (no
    // extra read): an exercised SoD exemption must leave an audit trail. Fires
    // for both a global `true` and a role-scoped exemption.
    const selfDecision = updated.requestedBy === actor.id;
    this.#record(actor, 'approval.decide', `approval:${id}`, 'allowed', {
      detail: {
        decision: input.decision,
        // Queue dwell time (created -> decided), in seconds. Feeds the
        // generic durationSeconds histogram convention breakwater's
        // metricsAuditSink observes — any emitter may adopt the field.
        durationSeconds:
          (Date.parse(now) - Date.parse(updated.createdAt)) / 1000,
        tenantId: updated.tenantId,
        workflowId: updated.workflowId,
        runId: updated.runId,
        grantScope: updated.grantScope,
        connectorCount: updated.connectors.length,
        ...(updated.stepPath === undefined
          ? {}
          : { stepPath: updated.stepPath }),
        ...(updated.suspendedAt === undefined
          ? {}
          : { suspendedAt: updated.suspendedAt }),
        ...(updated.resumeCount === undefined
          ? {}
          : { resumeCount: updated.resumeCount }),
        ...(updated.toolCallId === undefined
          ? {}
          : { toolCallId: updated.toolCallId }),
        ...(selfDecision ? { selfDecision: true } : {}),
      },
    });
    // Fire the live-stream event AFTER the transition + audit and BEFORE
    // #resume: the queue fan-out reflects the durable decision immediately,
    // independent of whether the subsequent run resume succeeds.
    fireStreamEvent(
      this.#stream,
      { type: 'decided', record: updated },
      (reason) =>
        this.#record(actor, 'approval.stream', `approval:${id}`, 'error', {
          reason,
          detail: { tenantId: updated.tenantId },
        }),
    );
    return { record: updated, resume: await this.#resume(updated, actor) };
  }

  /**
   * Triage fan-out: apply ONE decision to up to MAX_APPROVAL_BATCH_DECIDE
   * records, each through the EXISTING decide() — per-record CAS, SoD,
   * audit, and resume semantics untouched; the one-decision-per-suspension
   * model is not widened. Partial success is the contract: each record's
   * outcome (or typed failure) is reported in the envelope, never as a
   * thrown error. Only record-INDEPENDENT failures reject the whole batch:
   * the caller's role, malformed ids, the cap, and a malformed decision.
   *
   * Sequential, not Promise.all — preserves audit ordering and avoids
   * hammering the store's CAS with concurrent writes (D1/DO contention);
   * a batch is a reviewer clicking once, not a throughput path.
   */
  async decideBatch(
    ids: readonly string[],
    input: { decision: ApprovalDecision; comment?: string },
    actor: ApprovalActor,
  ): Promise<BatchDecideResult> {
    this.#authorize(actor, CAN_REVIEW, 'approval.decide', 'approval');
    if (!Array.isArray(ids) || ids.length === 0) {
      throw new InvalidApprovalInputError(
        'ids must be a non-empty array of approval ids',
      );
    }
    if (!ids.every((id) => isNonEmptyString(id))) {
      throw new InvalidApprovalInputError(
        'every id must be a non-empty string',
      );
    }
    // Order-preserving dedupe: deciding an id twice in one batch would
    // guarantee a spurious per-record conflict for the duplicate.
    const unique = [...new Set(ids)];
    if (unique.length > MAX_APPROVAL_BATCH_DECIDE) {
      throw new InvalidApprovalInputError(
        `a batch may decide at most ${MAX_APPROVAL_BATCH_DECIDE} approvals (got ${unique.length} unique ids)`,
      );
    }
    // Same validation decide() applies — hoisted so a malformed decision is
    // one 400, not N identical per-record failures.
    this.#assertDecisionInput(input);
    const results: BatchDecideItem[] = [];
    for (const id of unique) {
      try {
        const { record, resume } = await this.decide(id, input, actor);
        results.push({ id, ok: true, record, resume });
      } catch (error) {
        results.push({
          id,
          ok: false,
          error: errorMessage(error),
          code: batchDecideErrorCode(error),
        });
      }
    }
    const decided = results.filter((item) => item.ok).length;
    const failed = results.length - decided;
    // Summary only — each decide() above already left its own per-record
    // trail (allowed/denied), so this is the batch's one-line correlation.
    this.#record(actor, 'approval.decide.batch', 'approval', 'allowed', {
      detail: { requested: unique.length, decided, failed },
    });
    return { results, decided, failed };
  }

  /**
   * CAS-transitions a stale open record (pending/claimed/escalated, bound to
   * a suspension its step has since moved past) straight to 'rejected'.
   * host-kit's reconcileApprovalsForSummary calls this before filing a fresh
   * record for a step whose only open record no longer matches the run's
   * current (suspendedAt, resumeCount) fingerprint. Without this transition,
   * every poll would re-list the stale record through the open-step
   * uniqueness index and file nothing.
   *
   * Deliberately bypasses decide(): a rejection decision resumes the run
   * with declined semantics via #resume(), and a stale record must die
   * WITHOUT touching a run that is already suspended at a DIFFERENT
   * (current) fingerprint — this method never calls #resume(). 'rejected'
   * (not 'approved') is the deliberate terminal choice: connectorGrantsForLeg
   * (grants.ts) reads ONLY status: 'approved' records when deriving a leg's
   * grants, so a superseded record is excluded by its STATUS alone, not
   * merely by its stale fingerprint — it can never mint even if a future
   * change loosened the fingerprint check.
   *
   * Never routed by router.ts (an ApprovalService method the HTTP surface
   * never wires up) — reachable only from host-kit's
   * reconcileApprovalsForSummary, itself only invoked from createRunRouter's
   * optional reconcileApprovals hook on a status() read, never from a
   * request body. Authorized like create() (CAN_CREATE, not CAN_REVIEW):
   * the only actor that ever calls this is the same system actor create()
   * already accepts for reconcile-filed records — superseding is the
   * symmetric "un-file" half of that same self-healing operation, not a
   * reviewer decision.
   *
   * Returns null — mirroring the store's own CAS contract, rather than
   * throwing — when the record is unknown or already left the OPEN set (a
   * real decision won the race): the caller backs off instead of treating a
   * lost race as an error, the same "null = lost a race, skip quietly"
   * contract sweepSLA uses for its own store-level CAS.
   */
  async supersedeStale(
    id: string,
    actor: ApprovalActor,
    reason: string,
  ): Promise<ApprovalRecord | null> {
    this.#authorize(actor, CAN_CREATE, 'approval.supersede', `approval:${id}`);
    return this.#supersedeAuthorized(id, actor, reason);
  }

  /**
   * Supersede on behalf of an AUTOMATED principal — the reconcile bridge's
   * half of `createAsPrincipal`, and authorized the same way.
   *
   * Superseding is bookkeeping, not a decision: the record's suspension
   * fingerprint no longer matches the run, so it can never be resumed and is
   * closed to stop it shadowing the fresh filing. It is deliberately not
   * reachable through `decide`, so this does not give automation a decision.
   */
  async supersedeStaleAsPrincipal(
    id: string,
    principal: TrustedAutomationPrincipal,
    reason: string,
  ): Promise<ApprovalRecord | null> {
    this.#authorizeAutomated(principal, 'approval.supersede', `approval:${id}`);
    return this.#supersedeAuthorized(
      id,
      principalActor(principal),
      reason,
      principalAuditFields(principal),
    );
  }

  async #supersedeAuthorized(
    id: string,
    actor: ApprovalActor,
    reason: string,
    provenance?: Record<string, unknown>,
  ): Promise<ApprovalRecord | null> {
    const now = this.#now().toISOString();
    const updated = await this.#store.transition(id, OPEN_STATUSES, {
      status: 'rejected',
      decidedBy: actor.id,
      decision: 'reject',
      comment: reason,
      decidedAt: now,
      updatedAt: now,
    });
    if (!updated) return null;
    this.#record(actor, 'approval.supersede', `approval:${id}`, 'allowed', {
      reason,
      detail: {
        tenantId: updated.tenantId,
        workflowId: updated.workflowId,
        runId: updated.runId,
        ...provenance,
      },
    });
    // `reason` here is the supersede reason (method arg); the reportError arg
    // is renamed so it does not shadow it.
    fireStreamEvent(
      this.#stream,
      { type: 'superseded', record: updated },
      (streamError) =>
        this.#record(actor, 'approval.stream', `approval:${id}`, 'error', {
          reason: streamError,
          detail: { tenantId: updated.tenantId, ...provenance },
        }),
    );
    return updated;
  }

  async delegate(
    id: string,
    input: { to: string },
    actor: ApprovalActor,
  ): Promise<ApprovalRecord> {
    this.#authorize(actor, CAN_REVIEW, 'approval.delegate', `approval:${id}`);
    if (!isNonEmptyString(input.to)) {
      throw new InvalidApprovalInputError(
        "delegate requires a non-empty 'to' reviewer id",
      );
    }
    const now = this.#now().toISOString();
    // Deliberately last-writer-wins under concurrency: 'claimed' is itself in
    // the guard set, so racing delegations both succeed and the final
    // assignee is the last write. Delegation is reassignment — unlike decide
    // (mutual exclusion) or claim (no-steal), there is no side effect to
    // protect, only a pointer to move.
    const updated = await this.#transitionOrExplain(id, 'delegate', actor, {
      from: OPEN_STATUSES,
      patch: {
        status: 'claimed',
        claimedBy: input.to,
        delegatedTo: input.to,
        claimedAt: now,
        updatedAt: now,
      },
    });
    this.#record(actor, 'approval.delegate', `approval:${id}`, 'allowed', {
      detail: { to: input.to },
    });
    fireStreamEvent(
      this.#stream,
      { type: 'delegated', record: updated },
      (reason) =>
        this.#record(actor, 'approval.stream', `approval:${id}`, 'error', {
          reason,
          detail: { tenantId: updated.tenantId },
        }),
    );
    return updated;
  }

  async metrics(actor: ApprovalActor): Promise<ApprovalMetrics> {
    this.#authorize(actor, APPROVAL_ROLES, 'approval.read', 'approval');
    // D3: computation moved into the store contract (SQL aggregate on D1,
    // JS reduction on in-memory) instead of loading every record here.
    return this.#store.metrics(this.#now().getTime());
  }

  // CAS wrapper: on guard failure, disambiguate unknown id (404) from a
  // concurrent state change (409) and leave an audit trail either way.
  async #transitionOrExplain(
    id: string,
    action: string,
    actor: ApprovalActor,
    args: { from: readonly ApprovalStatus[]; patch: ApprovalPatch },
  ): Promise<ApprovalRecord> {
    const updated = await this.#store.transition(id, args.from, args.patch);
    if (updated) return updated;
    const current = await this.#store.get(id);
    if (!current) {
      this.#record(actor, `approval.${action}`, `approval:${id}`, 'denied', {
        reason: 'unknown approval',
      });
      throw new UnknownApprovalError(id);
    }
    this.#record(actor, `approval.${action}`, `approval:${id}`, 'denied', {
      reason: `status is '${current.status}'`,
    });
    throw new ApprovalConflictError(id, action, current.status);
  }

  async #resume(
    record: ApprovalRecord,
    actor: ApprovalActor,
  ): Promise<ResumeOutcome> {
    if (!this.#resumeRun) return { attempted: false };
    // record.decision is set by decide()'s patch before this runs.
    const decision = record.decision as ApprovalDecision;
    try {
      const summary = await this.#resumeRun(record, decision);
      this.#record(
        actor,
        'approval.resume',
        `approval:${record.id}`,
        'allowed',
        {
          detail: {
            tenantId: record.tenantId,
            workflowId: record.workflowId,
            runId: record.runId,
          },
        },
      );
      return { attempted: true, ok: true, summary };
    } catch (error) {
      // The decision is already durable; the run stays suspended and a later
      // resume derives the same grants from the store. Report, don't unwind.
      const message = errorMessage(error);
      this.#record(actor, 'approval.resume', `approval:${record.id}`, 'error', {
        reason: message,
        detail: {
          tenantId: record.tenantId,
          workflowId: record.workflowId,
          runId: record.runId,
        },
      });
      return { attempted: true, ok: false, error: message };
    }
  }

  #authorize(
    actor: ApprovalActor,
    roles: readonly ApprovalRole[],
    action: string,
    resource: string,
  ): void {
    if (
      isNonEmptyString(actor?.id) &&
      (APPROVAL_ROLES as readonly string[]).includes(actor.role) &&
      roles.includes(actor.role) &&
      // INV-2 belt at the service layer: the acting principal's tenant must
      // BE the store's binding. The resolver constructs the service from the
      // actor's own tenant so this can only fire on a wiring bug — which is
      // exactly when it must fail closed rather than act cross-tenant.
      isNonEmptyString(actor.tenantId) &&
      actor.tenantId === this.#store.tenantId
    ) {
      return;
    }
    const roleOk =
      actor !== null &&
      actor !== undefined &&
      (APPROVAL_ROLES as readonly string[]).includes(actor.role) &&
      roles.includes(actor.role);
    this.#record(actor ?? null, action, resource, 'denied', {
      reason: !actor
        ? 'no actor'
        : !roleOk
          ? `role '${actor.role}' is not in [${roles.join(', ')}]`
          : `actor tenant '${actor.tenantId}' does not match the store binding '${this.#store.tenantId}'`,
    });
    throw new ApprovalAuthzError(
      roleOk && actor
        ? `${action}: actor tenant does not match this service's tenant binding`
        : `${action} requires one of roles [${roles.join(', ')}]`,
    );
  }

  /**
   * Authorize an automated caller on kind, brand, and tenant.
   *
   * The parameter type is NOT the enforcement. `trustAutomationPrincipal` is
   * the only minter, but TypeScript is erased at runtime, so a cast or a value
   * rebuilt from storage reaches here typed correctly and shaped however the
   * caller left it. Re-reading the brand and the shape is what turns "only the
   * minter produces this" from a convention into a check — and it is what stops
   * a principal validated as `system` from being read back as a human `admin`.
   *
   * The tenant is checked for the same reason `#authorize` checks it: the
   * resolver builds the service from the principal's own tenant, so a mismatch
   * can only be a wiring bug — exactly when it must fail closed.
   */
  #authorizeAutomated(
    principal: TrustedAutomationPrincipal,
    action: string,
    resource: string,
  ): void {
    if (!isTrustedAutomationPrincipal(principal)) {
      // No actor: an unvouched value has no attribution worth recording, and
      // projecting one through principalActor would read the very fields the
      // check just refused to trust.
      this.#record(null, action, resource, 'denied', {
        reason:
          'principal is not a vouched automated principal (missing trust brand or invalid automated shape)',
      });
      throw new ApprovalAuthzError(
        `${action}: principal is not a vouched automated principal`,
      );
    }
    if (principal.tenantId === this.#store.tenantId) return;
    this.#record(principalActor(principal), action, resource, 'denied', {
      reason: `principal tenant '${principal.tenantId}' does not match the store binding '${this.#store.tenantId}'`,
      // A denial is an automated event too: without these the only automated
      // audit rows carrying provenance would be the ones that succeeded.
      detail: principalAuditFields(principal),
    });
    throw new ApprovalAuthzError(
      `${action}: principal tenant does not match this service's tenant binding`,
    );
  }

  // Callers pass the final resource string ('approval' for collection-level
  // actions, 'approval:<id>' for record-level) — no format inference here.
  #record(
    actor: ApprovalActor | null,
    action: string,
    resource: string,
    decision: 'allowed' | 'denied' | 'error',
    extra: { reason?: string; detail?: Record<string, unknown> } = {},
  ): void {
    if (!this.#audit) return;
    try {
      const outcome = this.#audit({
        actor,
        action,
        resource,
        decision,
        reason: extra.reason,
        detail: extra.detail,
      });
      // The sink may return a promise (a composed breakwater sink with an
      // async member); a rejection must be contained here too or it becomes
      // an unhandled rejection after the action already returned.
      if (outcome instanceof Promise) {
        outcome.catch(() => {
          // Availability over export reliability — same policy as below.
        });
      }
    } catch {
      // Availability over export reliability (AuditLogger's own policy): a
      // crashing sink must not fail the approval action it records.
    }
  }

  // Shared by decide() and decideBatch() — one home for the decision-shape
  // rules, so the two surfaces can never drift on wording or strictness.
  #assertDecisionInput(input: {
    decision: ApprovalDecision;
    comment?: string;
  }): void {
    if (input.decision !== 'approve' && input.decision !== 'reject') {
      throw new InvalidApprovalInputError(
        "decision must be 'approve' or 'reject'",
      );
    }
    if (input.comment !== undefined && typeof input.comment !== 'string') {
      throw new InvalidApprovalInputError('comment must be a string');
    }
  }

  #validateCreate(input: CreateApprovalInput): void {
    if (!isNonEmptyString(input.workflowId)) {
      throw new InvalidApprovalInputError('workflowId is required');
    }
    if (!isNonEmptyString(input.runId)) {
      throw new InvalidApprovalInputError('runId is required');
    }
    // Cheap INV-1 belt: every read path filters on the tenant_id COLUMN (not
    // by parsing run_id), so a foreign-prefixed record would not leak — but it
    // would be an orphan row no tenant's queue ever shows. Turn it into a
    // loud error at the only write path.
    if (!tenantOwnsSaltedId(this.#store.tenantId, input.runId)) {
      throw new InvalidApprovalInputError(
        `runId '${input.runId}' does not carry this tenant's prefix '${this.#store.tenantId}_' — approvals bind to tenant-salted runs (INV-1)`,
      );
    }
    if (!isNonEmptyString(input.title)) {
      throw new InvalidApprovalInputError('title is required');
    }
    if (
      input.stepPath !== undefined &&
      (!Array.isArray(input.stepPath) ||
        !input.stepPath.every((segment) => isNonEmptyString(segment)))
    ) {
      throw new InvalidApprovalInputError(
        'stepPath must be an array of non-empty strings',
      );
    }
    if (
      input.connectors !== undefined &&
      (!Array.isArray(input.connectors) ||
        !input.connectors.every((connector) => isNonEmptyString(connector)))
    ) {
      throw new InvalidApprovalInputError(
        'connectors must be an array of non-empty strings',
      );
    }
    const connectorCount = input.connectors?.length ?? 0;
    if (
      input.toolCallId !== undefined &&
      (!isNonEmptyString(input.toolCallId) || connectorCount !== 1)
    ) {
      throw new InvalidApprovalInputError(
        'toolCallId requires exactly one connector and must be a non-empty string',
      );
    }
    // Attribution is what the separation-of-duties check compares, so an
    // explicit one must be a usable identity — an empty string would silently
    // match no actor and, worse, read as "attributed" to any later reader.
    if (
      input.requestedBy !== undefined &&
      !isNonEmptyString(input.requestedBy)
    ) {
      throw new InvalidApprovalInputError(
        'requestedBy must be a non-empty string',
      );
    }
    // runScoped is a capability switch (mints on every leg); only a real
    // boolean opts in, never a truthy string from a lax caller.
    if (input.runScoped !== undefined && typeof input.runScoped !== 'boolean') {
      throw new InvalidApprovalInputError('runScoped must be a boolean');
    }
    if (input.runScoped === true) {
      if (
        connectorCount === 0 ||
        input.stepPath !== undefined ||
        input.suspendedAt !== undefined ||
        input.resumeCount !== undefined ||
        input.toolCallId !== undefined
      ) {
        throw new InvalidApprovalInputError(
          'runScoped grants require connectors and may not carry step, suspension, resume-count, or tool-call identity',
        );
      }
    } else if (connectorCount > 0) {
      if (input.stepPath === undefined || input.suspendedAt === undefined) {
        throw new InvalidApprovalInputError(
          'step-scoped connector grants require stepPath and suspendedAt',
        );
      }
    }
    if (
      input.runScoped !== true &&
      input.stepPath === undefined &&
      (input.suspendedAt !== undefined ||
        input.resumeCount !== undefined ||
        input.toolCallId !== undefined)
    ) {
      throw new InvalidApprovalInputError(
        'suspension and tool-call identity require a stepPath',
      );
    }
    if (input.resumeCount !== undefined && input.suspendedAt === undefined) {
      throw new InvalidApprovalInputError('resumeCount requires suspendedAt');
    }
    if (
      input.priority !== undefined &&
      !APPROVAL_PRIORITIES.includes(input.priority)
    ) {
      throw new InvalidApprovalInputError(
        `priority must be one of [${APPROVAL_PRIORITIES.join(', ')}]`,
      );
    }
    if (
      input.slaSeconds !== undefined &&
      (typeof input.slaSeconds !== 'number' ||
        !Number.isFinite(input.slaSeconds) ||
        input.slaSeconds <= 0)
    ) {
      throw new InvalidApprovalInputError(
        'slaSeconds must be a positive number',
      );
    }
    if (
      input.suspendedAt !== undefined &&
      (typeof input.suspendedAt !== 'number' ||
        !Number.isFinite(input.suspendedAt) ||
        input.suspendedAt <= 0)
    ) {
      throw new InvalidApprovalInputError(
        'suspendedAt must be a positive epoch-ms number',
      );
    }
    if (
      input.resumedAt !== undefined &&
      (typeof input.resumedAt !== 'number' ||
        !Number.isFinite(input.resumedAt) ||
        input.resumedAt <= 0)
    ) {
      throw new InvalidApprovalInputError(
        'resumedAt must be a positive epoch-ms number',
      );
    }
    // resumeCount is the runtime resume ordinal — a positive integer (1,2,…);
    // undefined means a first suspension. Unlike the timestamps it is a count,
    // so it must be an integer.
    if (
      input.resumeCount !== undefined &&
      (typeof input.resumeCount !== 'number' ||
        !Number.isInteger(input.resumeCount) ||
        input.resumeCount <= 0)
    ) {
      throw new InvalidApprovalInputError(
        'resumeCount must be a positive integer',
      );
    }
    // The record contract is JSON-safe end to end (types.ts); reject what
    // JSON.stringify cannot represent (BigInt, circular refs) here so the
    // two stores never diverge on exotic payloads.
    if (input.payload !== undefined) {
      try {
        JSON.stringify(input.payload);
      } catch (error) {
        throw new InvalidApprovalInputError(
          `payload must be JSON-serializable: ${errorMessage(error)}`,
        );
      }
    }
  }

  #validateResumeTarget(target: ApprovalResumeTarget): void {
    if (target === null || typeof target !== 'object') {
      throw new InvalidApprovalInputError(
        'resumeTarget must be a trusted thread or agent-thread target',
      );
    }
    const ownsPathSafeId = (value: unknown): value is string =>
      typeof value === 'string' &&
      PATH_SAFE_ID_PATTERN.test(value) &&
      tenantOwnsSaltedId(this.#store.tenantId, value);
    if (target.kind === 'thread') {
      if (
        !ownsPathSafeId(target.threadId) ||
        (target.resourceId !== undefined && !ownsPathSafeId(target.resourceId))
      ) {
        throw new InvalidApprovalInputError(
          'resumeTarget must name path-safe thread/resource ids owned by the bound tenant',
        );
      }
      return;
    }
    if (target.kind === 'agent-thread') {
      if (
        typeof target.agentId !== 'string' ||
        !PATH_SAFE_ID_PATTERN.test(target.agentId) ||
        !ownsPathSafeId(target.threadId) ||
        !ownsPathSafeId(target.resourceId) ||
        // Fails closed on the pre-principal `{id, role, tenantId}` form: an
        // ApprovalActor is not an ExecutionPrincipal, and coercing one would
        // resurrect a fabricated operator as a human.
        !isExecutionPrincipal(target.principal) ||
        target.principal.tenantId !== this.#store.tenantId
      ) {
        throw new InvalidApprovalInputError(
          'agent resumeTarget must name path-safe ids and a valid execution principal owned by the bound tenant',
        );
      }
      return;
    }
    throw new InvalidApprovalInputError(
      'resumeTarget must be a trusted thread or agent-thread target',
    );
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

// Maps a per-record decide() failure to BatchDecideItem.code — the same
// classification errorResponse (router.ts) applies to thrown errors, kept as
// data because a batch envelope has one HTTP status for N outcomes.
function batchDecideErrorCode(error: unknown): BatchDecideItem['code'] {
  if (error instanceof UnknownApprovalError) return 'not-found';
  if (error instanceof ApprovalConflictError) return 'conflict';
  if (error instanceof ApprovalAuthzError) return 'forbidden';
  if (error instanceof InvalidApprovalInputError) return 'invalid';
  return 'error';
}

// Containment for the notification seam (shared by create() and sweepSLA):
// the sink is fire-and-forget — a sync throw is caught here, an async
// rejection is caught on the returned promise — and either failure is
// reported through the AUDIT sink (as `approval.notify`/'error') so the
// evidence survives without the approval action ever failing. Deliberately
// not awaited: hosts that must keep a transport alive past the response wrap
// it in ctx.waitUntil themselves (see ApprovalNotificationSink).
function fireNotification(
  notify: ApprovalNotificationSink | undefined,
  event: ApprovalNotificationEvent,
  reportError: (reason: string) => void,
): void {
  if (!notify) return;
  try {
    const outcome = notify(event);
    if (outcome) {
      outcome.catch((error: unknown) =>
        reportError(`notification sink rejected: ${errorMessage(error)}`),
      );
    }
  } catch (error) {
    reportError(`notification sink threw: ${errorMessage(error)}`);
  }
}

// Containment for the live-stream seam — the SAME house fire-and-forget
// dual-guard idiom as fireNotification: a sync throw is caught by the
// try/catch, an async rejection by the returned promise's .catch (a bare
// Promise.resolve(sink(event)).catch() would miss the sync throw, which is
// evaluated before the .catch attaches). Either failure is reported through
// the AUDIT sink (as `approval.stream`/'error') so the mutation that fired it
// never fails. Deliberately not awaited: a host that must keep a hub publish
// alive past the response wraps it in ctx.waitUntil itself (see
// ApprovalStreamSink); the cron sweep collects it into pendingSends instead.
function fireStreamEvent(
  stream: ApprovalStreamSink | undefined,
  event: ApprovalStreamEvent,
  reportError: (reason: string) => void,
): void {
  if (!stream) return;
  try {
    const outcome = stream(event);
    if (outcome) {
      outcome.catch((error: unknown) =>
        reportError(`stream sink rejected: ${errorMessage(error)}`),
      );
    }
  } catch (error) {
    reportError(`stream sink threw: ${errorMessage(error)}`);
  }
}

/** Options for the cron-owned SLA sweep. */
export interface SweepSLAOptions {
  /**
   * Attribution identity for audit events (e.g. the worker's system actor).
   * Attribution only — the sweep runs inside the trusted computing base
   * (cron), so there is no role check: the TYPE of the store argument is the
   * authorization (a SystemApprovalStore is unobtainable from request scope).
   *
   * Automated kinds only, and REFUSED at runtime as well: a human here would
   * stamp `principalKind: 'human'` onto cross-tenant cron escalations, which is
   * the synthetic operator this whole model exists to remove.
   */
  systemPrincipal: AutomatedExecutionPrincipal;
  audit?: ApprovalAuditSink;
  /** Fired for each record escalated. */
  onEscalation?: (record: ApprovalRecord) => void;
  /**
   * Notification transport seam — fired once per escalated record, alongside
   * (not instead of) onEscalation: onEscalation is the hosts' structured-log
   * hook, notify is the reviewer-facing transport. Same containment as
   * ApprovalServiceOptions.notify: failures audit as `approval.notify`/'error'
   * and never abort the sweep.
   */
  notify?: ApprovalNotificationSink;
  /**
   * Live-stream fan-out seam — fired once per escalated record, alongside
   * onEscalation and notify. Same containment as ApprovalServiceOptions.stream:
   * a throwing or rejecting sink is audited as `approval.stream`/'error' and
   * never aborts the sweep. A scheduled handler has no request-scoped
   * waitUntil, so the cron host collects each publish into its pendingSends and
   * awaits it there (see host-kit's runSlaSweepMaintenance) — never
   * fire-and-forget under `scheduled()`.
   */
  stream?: ApprovalStreamSink;
  /** Injectable clock (tests, deterministic SLA math). */
  now?: () => Date;
}

/**
 * Escalate every open request past its SLA deadline, ACROSS TENANTS.
 * Idempotent: already escalated records are not re-escalated (their status
 * left the guard set). Cron-owned TCB code — deliberately NOT a service
 * method and NOT reachable over HTTP: an unfiltered cross-tenant read+write
 * behind a role check was an IDOR-shaped hole (any CAN_SWEEP actor could
 * escalate every tenant's queue). The distinct SystemApprovalStore parameter
 * type makes "cross-tenant reads happen only inside the TCB" a compile-time
 * property, not a convention.
 */
export async function sweepSLA(
  store: SystemApprovalStore,
  options: SweepSLAOptions,
): Promise<ApprovalRecord[]> {
  // The principal is attribution, not authorization — the SystemApprovalStore
  // type is the authorization. The parameter type already excludes a human;
  // this is the erased-type half, because this is the one exported cross-tenant
  // function that CARRIES an attribution principal (purgeExpiredApprovals takes
  // none, so it has no attribution to corrupt), and a bad principal here makes
  // every escalation it emits, for every tenant, unattributable.
  const systemPrincipal = canonicalAutomatedPrincipal(options.systemPrincipal);
  if (systemPrincipal === undefined) {
    throw new Error(
      'sweepSLA: systemPrincipal must be a valid automated execution principal',
    );
  }
  const now = options.now ?? (() => new Date());
  const record = (
    action: string,
    resource: string,
    decision: 'allowed' | 'denied' | 'error',
    extra: { reason?: string; detail?: Record<string, unknown> } = {},
  ): void => {
    if (!options.audit) return;
    try {
      const outcome = options.audit({
        actor: principalActor(systemPrincipal),
        action,
        resource,
        decision,
        reason: extra.reason,
        // Provenance the fabricated maintenance operator never carried: which
        // kind of principal swept, under whose id, and why.
        detail: {
          ...extra.detail,
          ...principalAuditFields(systemPrincipal),
        },
      });
      // Same promise containment as ApprovalService's #record: a composed
      // sink's rejection must never surface as an unhandled rejection.
      if (outcome instanceof Promise) {
        outcome.catch(() => {
          // Availability over export reliability — see below.
        });
      }
    } catch {
      // Availability over export reliability — a crashing sink must not
      // abort the sweep.
    }
  };
  const at = now();
  const nowIso = at.toISOString();
  const escalated: ApprovalRecord[] = [];
  // D3: page the cross-tenant open set with an explicit cursor instead of one
  // unbounded SELECT (the system view is deliberately un-defaulted, so a bare
  // list() here would be the whole table). Keyset paging is on (createdAt, id);
  // escalating a record only drops it from the pending/claimed filter and never
  // changes its cursor position, so every currently-open record is visited
  // exactly once — no skips, no repeats — and every breach as of `at` still
  // escalates regardless of where it sits in FIFO order.
  let after: string | undefined;
  for (;;) {
    const page = await store.list({
      status: ['pending', 'claimed'],
      limit: MAX_APPROVAL_LIST_LIMIT,
      after,
    });
    for (const candidate of page) {
      if (
        candidate.slaDeadlineAt === undefined ||
        Date.parse(candidate.slaDeadlineAt) > at.getTime()
      ) {
        continue;
      }
      const updated = await store.transition(
        candidate.id,
        ['pending', 'claimed'],
        { status: 'escalated', escalatedAt: nowIso, updatedAt: nowIso },
      );
      // null = lost a race (decided or escalated concurrently) — skip quietly.
      if (!updated) continue;
      escalated.push(updated);
      record('approval.escalate', `approval:${updated.id}`, 'allowed', {
        reason: `SLA deadline ${updated.slaDeadlineAt} breached`,
        // tenantId attributes the escalation: a cross-tenant sweep without it
        // emits unattributable audit events.
        detail: {
          tenantId: updated.tenantId,
          workflowId: updated.workflowId,
          runId: updated.runId,
        },
      });
      if (options.onEscalation) {
        try {
          options.onEscalation(updated);
        } catch (error) {
          // The hook is notification-only; a crashing notifier must not
          // abort the sweep. The audit trail keeps the evidence.
          record('approval.escalate', `approval:${updated.id}`, 'error', {
            reason: `onEscalation threw: ${errorMessage(error)}`,
            detail: { tenantId: updated.tenantId },
          });
        }
      }
      fireNotification(
        options.notify,
        { type: 'escalated', record: updated },
        (reason) =>
          record('approval.notify', `approval:${updated.id}`, 'error', {
            reason,
            detail: { tenantId: updated.tenantId },
          }),
      );
      fireStreamEvent(
        options.stream,
        { type: 'escalated', record: updated },
        (reason) =>
          record('approval.stream', `approval:${updated.id}`, 'error', {
            reason,
            detail: { tenantId: updated.tenantId },
          }),
      );
    }
    // Full page ⇒ there may be more; cursor past the last row (FIFO order, so
    // `after` is valid) and continue. Short page ⇒ done.
    const last = page.at(-1);
    if (page.length < MAX_APPROVAL_LIST_LIMIT || !last) break;
    after = approvalCursor(last);
  }
  return escalated;
}
