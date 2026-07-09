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
  ApprovalRole,
} from './contract.js';
import { APPROVAL_ROLES } from './contract.js';
import type { ApprovalPatch, ApprovalStore } from './store.js';
import {
  APPROVAL_PRIORITIES,
  type ApprovalDecision,
  type ApprovalListFilter,
  type ApprovalMetrics,
  type ApprovalRecord,
  type ApprovalStatus,
  type CreateApprovalInput,
  type DecideResult,
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
  store: ApprovalStore;
  /**
   * Structural match for breakwater AuditLogger.record — wire
   * `(event) => auditLogger.record(event)`. Must not throw; failures are
   * contained anyway (availability over export reliability, matching
   * AuditLogger's own sink policy).
   */
  audit?: ApprovalAuditSink;
  /** Fired for each record sweepSLA() escalates. */
  onEscalation?: (record: ApprovalRecord) => void;
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
// the system, admins do both; every authenticated role may read.
const CAN_REVIEW: readonly ApprovalRole[] = ['reviewer', 'admin'];
const CAN_CREATE: readonly ApprovalRole[] = ['operator', 'builder', 'admin'];
const CAN_SWEEP: readonly ApprovalRole[] = ['operator', 'admin'];

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

export class ApprovalService {
  readonly #store: ApprovalStore;
  readonly #audit?: ApprovalAuditSink;
  readonly #onEscalation?: (record: ApprovalRecord) => void;
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
    this.#onEscalation = options.onEscalation;
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
          workflowId: result.record.workflowId,
          runId: result.record.runId,
          created: result.created,
        },
      },
    );
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
    if (input.decision !== 'approve' && input.decision !== 'reject') {
      throw new InvalidApprovalInputError(
        "decision must be 'approve' or 'reject'",
      );
    }
    if (input.comment !== undefined && typeof input.comment !== 'string') {
      throw new InvalidApprovalInputError('comment must be a string');
    }
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
        workflowId: updated.workflowId,
        runId: updated.runId,
      },
    });
    return { record: updated, resume: await this.#resume(updated, actor) };
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

  /**
   * Escalate every open request past its SLA deadline. Idempotent: already
   * escalated records are not re-escalated (their status left the guard set).
   * Production trigger: a Workers cron hitting POST .../sla/sweep.
   */
  async sweepSLA(actor: ApprovalActor): Promise<ApprovalRecord[]> {
    this.#authorize(actor, CAN_SWEEP, 'approval.escalate', 'approval');
    const now = this.#now();
    const nowIso = now.toISOString();
    const open = await this.#store.list({ status: ['pending', 'claimed'] });
    const escalated: ApprovalRecord[] = [];
    for (const record of open) {
      if (
        record.slaDeadlineAt === undefined ||
        Date.parse(record.slaDeadlineAt) > now.getTime()
      ) {
        continue;
      }
      const updated = await this.#store.transition(
        record.id,
        ['pending', 'claimed'],
        { status: 'escalated', escalatedAt: nowIso, updatedAt: nowIso },
      );
      // null = lost a race (decided or escalated concurrently) — skip quietly.
      if (!updated) continue;
      escalated.push(updated);
      this.#record(
        actor,
        'approval.escalate',
        `approval:${updated.id}`,
        'allowed',
        {
          reason: `SLA deadline ${updated.slaDeadlineAt} breached`,
          detail: { workflowId: updated.workflowId, runId: updated.runId },
        },
      );
      if (this.#onEscalation) {
        try {
          this.#onEscalation(updated);
        } catch (error) {
          // The hook is notification-only; a crashing notifier must not
          // abort the sweep. The audit trail keeps the evidence.
          this.#record(
            actor,
            'approval.escalate',
            `approval:${updated.id}`,
            'error',
            {
              reason: `onEscalation threw: ${errorMessage(error)}`,
            },
          );
        }
      }
    }
    return escalated;
  }

  async metrics(actor: ApprovalActor): Promise<ApprovalMetrics> {
    this.#authorize(actor, APPROVAL_ROLES, 'approval.read', 'approval');
    const all = await this.#store.list();
    const now = this.#now().getTime();
    const open = all.filter((record) => OPEN_STATUSES.includes(record.status));
    const decided = all.filter(
      (record) => record.status === 'approved' || record.status === 'rejected',
    );
    const resolutionsSeconds = decided
      .filter((record) => record.decidedAt !== undefined)
      .map(
        (record) =>
          (Date.parse(record.decidedAt as string) -
            Date.parse(record.createdAt)) /
          1000,
      );
    return {
      openCount: open.length,
      slaBreachedCount: open.filter(
        (record) =>
          record.slaDeadlineAt !== undefined &&
          Date.parse(record.slaDeadlineAt) <= now,
      ).length,
      escalationCount: all.filter((record) => record.escalatedAt !== undefined)
        .length,
      decidedCount: decided.length,
      approvedCount: decided.filter((record) => record.status === 'approved')
        .length,
      rejectedCount: decided.filter((record) => record.status === 'rejected')
        .length,
      avgResolutionSeconds:
        resolutionsSeconds.length > 0
          ? resolutionsSeconds.reduce((sum, value) => sum + value, 0) /
            resolutionsSeconds.length
          : null,
    };
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
          detail: { workflowId: record.workflowId, runId: record.runId },
        },
      );
      return { attempted: true, ok: true, summary };
    } catch (error) {
      // The decision is already durable; the run stays suspended and a later
      // resume derives the same grants from the store. Report, don't unwind.
      const message = errorMessage(error);
      this.#record(actor, 'approval.resume', `approval:${record.id}`, 'error', {
        reason: message,
        detail: { workflowId: record.workflowId, runId: record.runId },
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
      roles.includes(actor.role)
    ) {
      return;
    }
    this.#record(actor ?? null, action, resource, 'denied', {
      reason: actor
        ? `role '${actor.role}' is not in [${roles.join(', ')}]`
        : 'no actor',
    });
    throw new ApprovalAuthzError(
      `${action} requires one of roles [${roles.join(', ')}]`,
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
      this.#audit({
        actor,
        action,
        resource,
        decision,
        reason: extra.reason,
        detail: extra.detail,
      });
    } catch {
      // Availability over export reliability (AuditLogger's own policy): a
      // crashing sink must not fail the approval action it records.
    }
  }

  #validateCreate(input: CreateApprovalInput): void {
    if (!isNonEmptyString(input.workflowId)) {
      throw new InvalidApprovalInputError('workflowId is required');
    }
    if (!isNonEmptyString(input.runId)) {
      throw new InvalidApprovalInputError('runId is required');
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
