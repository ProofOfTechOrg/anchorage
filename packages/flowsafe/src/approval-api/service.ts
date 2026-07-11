// SPDX-License-Identifier: Apache-2.0
// ApprovalService — the queue's business rules: role authorization, CAS
// transitions, SLA escalation, audit emission, and the post-decision resume.
//
// Grant-minting trust boundary (security-threat-model.md, boundary 6): this
// service — via the store records it writes under RBAC — is the ONLY source
// of approval grants. decide() persists the decision; the grant provider
// (grants.ts) derives requestContext grants from approved records at
// start/resume. Nothing here ever reads capability data from client input.

import type {
  ApprovalActor,
  ApprovalAuditSink,
  ApprovalNotificationEvent,
  ApprovalNotificationSink,
  ApprovalRole,
} from './contract.js';
import { APPROVAL_ROLES } from './contract.js';
import type { ApprovalPatch } from './store.js';
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

export interface ApprovalServiceOptions {
  /**
   * MUST be tenant-bound (INV-2): the service asserts every acting
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
   * separation of duties is the safe enterprise default; enable only for
   * single-operator deployments.
   */
  allowSelfDecision?: boolean;
  /** Injectable clock (tests, deterministic SLA math). */
  now?: () => Date;
}

// Role policy from security-threat-model.md: reviewers decide, operators run
// the system, admins do both; every authenticated role may read. (The SLA
// sweep has no role: it is cron-owned TCB code — see sweepSLA below.)
const CAN_REVIEW: readonly ApprovalRole[] = ['reviewer', 'admin'];
const CAN_CREATE: readonly ApprovalRole[] = ['operator', 'builder', 'admin'];

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

export class ApprovalService {
  readonly #store: TenantBoundApprovalStore;
  readonly #audit?: ApprovalAuditSink;
  readonly #notify?: ApprovalNotificationSink;
  readonly #defaultSlaSeconds?: number;
  readonly #resumeRun?: (
    record: ApprovalRecord,
    decision: ApprovalDecision,
  ) => Promise<unknown>;
  readonly #allowSelfDecision: boolean;
  readonly #now: () => Date;

  constructor(options: ApprovalServiceOptions) {
    this.#store = options.store;
    this.#audit = options.audit;
    this.#notify = options.notify;
    this.#defaultSlaSeconds = options.defaultSlaSeconds;
    this.#resumeRun = options.resumeRun;
    this.#allowSelfDecision = options.allowSelfDecision ?? false;
    this.#now = options.now ?? (() => new Date());
  }

  async create(
    input: CreateApprovalInput,
    actor: ApprovalActor,
  ): Promise<{ record: ApprovalRecord; created: boolean }> {
    this.#authorize(actor, CAN_CREATE, 'approval.create', 'approval');
    this.#validateCreate(input);
    const now = this.#now();
    const slaSeconds = input.slaSeconds ?? this.#defaultSlaSeconds;
    const record: ApprovalRecord = {
      id: crypto.randomUUID(),
      // The bound store re-stamps this from its own field either way; setting
      // it here keeps the literal honest for the type and the audit trail.
      tenantId: this.#store.tenantId,
      workflowId: input.workflowId,
      runId: input.runId,
      title: input.title,
      connectors: [...(input.connectors ?? [])],
      priority: input.priority ?? 'normal',
      status: 'pending',
      createdAt: now.toISOString(),
      updatedAt: now.toISOString(),
    };
    if (input.stepPath !== undefined) record.stepPath = [...input.stepPath];
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
          created: result.created,
        },
      },
    );
    // Only an actual insert notifies — the idempotent re-observation path
    // (created: false) returns the EXISTING open record, which already
    // notified when it entered the queue.
    if (result.created) {
      fireNotification(
        this.#notify,
        { type: 'created', record: result.record },
        (reason) =>
          this.#record(
            actor,
            'approval.notify',
            `approval:${result.record.id}`,
            'error',
            { reason, detail: { tenantId: result.record.tenantId } },
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
    return updated;
  }

  async decide(
    id: string,
    input: { decision: ApprovalDecision; comment?: string },
    actor: ApprovalActor,
  ): Promise<DecideResult> {
    this.#authorize(actor, CAN_REVIEW, 'approval.decide', `approval:${id}`);
    this.#assertDecisionInput(input);
    if (!this.#allowSelfDecision) {
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
      },
    });
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
   * D4 follow-up (2026-07-11 audit): CAS-transitions a STALE OPEN record
   * (pending/claimed/escalated, bound to a suspension its step has since
   * moved past) straight to 'rejected'. host-kit's reconcileApprovalsForSummary
   * calls this before filing a fresh record for a step whose only open
   * record no longer matches the run's CURRENT (suspendedAt, resumeCount)
   * fingerprint — the loop where a stale-but-open record otherwise never
   * heals (every poll re-lists, finds the same open record via the
   * open-step uniqueness index, and re-files nothing).
   *
   * Deliberately bypasses decide(): a rejection decision resumes the run
   * with declined semantics via #resume(), and a stale record must die
   * WITHOUT touching a run that is already suspended at a DIFFERENT
   * (current) fingerprint — this method never calls #resume(). 'rejected'
   * (not 'approved') is the deliberate terminal choice: approvedConnectorsForLeg
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
      },
    });
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
    if (!input.runId.startsWith(`${this.#store.tenantId}_`)) {
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

/** Options for the cron-owned SLA sweep. */
export interface SweepSLAOptions {
  /**
   * Attribution identity for audit events (e.g. the worker's system actor).
   * Attribution only — the sweep runs inside the trusted computing base
   * (cron), so there is no role check: the TYPE of the store argument is the
   * authorization (a SystemApprovalStore is unobtainable from request scope).
   */
  systemActor: ApprovalActor;
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
        actor: options.systemActor,
        action,
        resource,
        decision,
        reason: extra.reason,
        detail: extra.detail,
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
    }
    // Full page ⇒ there may be more; cursor past the last row (FIFO order, so
    // `after` is valid) and continue. Short page ⇒ done.
    const last = page.at(-1);
    if (page.length < MAX_APPROVAL_LIST_LIMIT || !last) break;
    after = approvalCursor(last);
  }
  return escalated;
}
