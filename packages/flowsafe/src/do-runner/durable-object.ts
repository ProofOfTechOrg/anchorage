// SPDX-License-Identifier: Apache-2.0
// Classic fetch-contract Durable Object (constructor(state, env) + fetch).
// Deliberately NOT `extends DurableObject` from 'cloudflare:workers': the
// classic contract needs no workers-only runtime import, so this module and
// everything under it load in node/vitest. Run state lives in D1, not DO
// storage — that is what lets a run started before a restart resume after
// one. Serialization of execution and lifecycle mutations on a single run is
// enforced by RunnerRuntime's per-run FIFO locks; routing one DO instance per run
// (idFromName(`${workflowId}:${runId}`)) makes those locks authoritative,
// since all traffic for a run lands on one instance. The object's alarm is
// reserved for recovering run-owner claims that outlive an interrupted start.

import {
  decodeExecutionPrincipal,
  type ExecutionPrincipal,
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import type { DurableObjectRunnerState, WebSocketLike } from './cf-types.js';
import { newWebSocketPair, safeSend } from './cf-types.js';
import {
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from './deployment-identity.js';
import { DoStatusError, doErrorResponse } from './do-error-response.js';
import { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
import { isPathSafeId } from './path-safe-id.js';
import {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  type RunLifecycleCas,
  type RunLifecycleTransitionResult,
  type RunnerRuntime,
  type RunSummary,
  UnknownRunError,
} from './runtime.js';
import {
  resolveScheduleStartOwner,
  type ScheduleSourceStore,
  type ScheduleSourceWorkflowTarget,
} from './schedule-source.js';

export interface DurableObjectRunOwner {
  readonly kind: ExecutionPrincipalKind;
  readonly id: string;
}

export interface DurableObjectRunOwnershipStore {
  owner(
    kind: 'run' | 'schedule' | 'thread' | 'resource',
    resourceId: string,
  ): Promise<DurableObjectRunOwner | undefined>;
  reserveAll(
    claims: readonly { kind: 'run'; resourceId: string }[],
    owner: DurableObjectRunOwner,
    token: string,
  ): Promise<boolean>;
  settleReservation(
    token: string,
    release: readonly { kind: 'run'; resourceId: string }[],
  ): Promise<void>;
  release?(
    kind: 'run',
    resourceId: string,
    owner: DurableObjectRunOwner,
  ): Promise<boolean>;
}

export interface DurableObjectRunLifecycleHooks {
  abandonApprovals(
    workflowId: string,
    runId: string,
    status: 'cancelled' | 'timed_out',
  ): Promise<void>;
  discardScheduleDispatch?(
    scheduleId: string,
    dispatchId: string,
    runId: string,
  ): Promise<void>;
}

const RUN_OWNER_RECOVERY_KEY = 'flowsafe:run-owner-recovery:v1';
const RUN_OWNER_RECOVERY_DELAY_MS = 60_000;

interface RunOwnerRecovery {
  version: 1;
  workflowId: string;
  runId: string;
  token: string;
}

interface StartBody {
  workflowId?: string;
  runId?: string;
  inputData?: unknown;
  initialState?: unknown;
  scheduleId?: unknown;
  dispatchId?: unknown;
  deadlineMs?: unknown;
}

interface ResumeBody {
  step?: string | string[];
  resumeData?: unknown;
  requestedBy?: unknown;
  requestedByKind?: unknown;
  deadlineMs?: unknown;
}

interface DeadlineBody {
  expectedRevision?: unknown;
  expectedDeadlineAt?: unknown;
}

class DurableObjectRunIdentityError extends DoStatusError {
  readonly status = 403;
}

export abstract class DurableObjectRunner<TEnv = unknown> {
  protected readonly env: TEnv;
  /** Absent in Node tests; present under workerd and reserved for alarms. */
  protected readonly state?: DurableObjectRunnerState;
  #runtime?: RunnerRuntime;
  #operationTail = Promise.resolve();

  constructor(state: DurableObjectRunnerState | undefined, env: TEnv) {
    this.state = state;
    this.env = env;
  }

  /** Define workflows via init() and return its runtime. Called once, lazily. */
  protected abstract build(env: TEnv): RunnerRuntime;

  /** The deployment-local ownership registry used for recoverable run starts. */
  protected abstract runOwnership(env: TEnv): DurableObjectRunOwnershipStore;

  /** Existing schedules domain used only for target-verifiable schedule fires. */
  protected scheduleSource(_env: TEnv): ScheduleSourceStore | undefined {
    return undefined;
  }

  /** Required when the host exposes terminate or deadline lifecycle routes. */
  protected runLifecycle(
    _env: TEnv,
  ): DurableObjectRunLifecycleHooks | undefined {
    return undefined;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      // Deployment-identity check BEFORE any routing or storage work: under
      // workerd this instance refuses to serve until its env tag matches the
      // database sentinel (fail closed on a mis-provisioned binding); off
      // workerd (node tests, state undefined) it is a no-op. Memoized after
      // the first success, so steady-state requests pay nothing.
      await verifyDurableObjectDeploymentRequest(request, this.state, this.env);
      return await this.#route(request);
    } catch (error) {
      return doErrorResponse(error);
    }
  }

  #ensureRuntime(): RunnerRuntime {
    if (!this.#runtime) {
      this.#runtime = this.build(this.env);
    }
    return this.#runtime;
  }

  // INV-1 enforcement at the DO boundary: this instance was addressed as
  // idFromName(`${workflowId}:${runId}`) by the trusted Worker, and id.name
  // is unforgeable at this boundary. If a request asks the instance to act on
  // a DIFFERENT (workflowId, runId), someone routed around the name join —
  // acting on the request's ids would run outside this instance's identity
  // (and outside its per-run serialization). Refuse loudly. `id.name` is only
  // populated for idFromName-created ids; it is absent under node tests
  // (state undefined / minimal stubs), where the runtime's own validation
  // still applies.
  #assertRunIdentity(workflowId: string, runId: string): void {
    const name = this.state?.id?.name;
    if (name === undefined) return;
    if (name !== `${workflowId}:${runId}`) {
      throw new Error(
        `DO identity mismatch: instance is '${name}' but the request names '${workflowId}:${runId}' — refusing (INV-1)`,
      );
    }
  }

  #requestedBy(value: unknown): string {
    if (!isExecutionPrincipalId(value)) {
      throw new InvalidRunRequestError('run requester is missing or malformed');
    }
    return value;
  }

  #requestedByKind(value: unknown): DurableObjectRunOwner['kind'] {
    if (!isExecutionPrincipalKind(value)) {
      throw new InvalidRunRequestError('run requester kind is malformed');
    }
    return value;
  }

  #trustedExecutionPrincipal(request: Request): ExecutionPrincipal {
    const encoded = request.headers.get(EXECUTION_PRINCIPAL_HEADER);
    const principal = encoded ? decodeExecutionPrincipal(encoded) : undefined;
    if (!principal) {
      throw new DurableObjectRunIdentityError(
        'run request carries no valid trusted execution principal',
      );
    }
    return principal;
  }

  async #finalizeTerminal(
    runtime: RunnerRuntime,
    workflowId: string,
    runId: string,
    owner: DurableObjectRunOwner,
    result: RunLifecycleTransitionResult,
  ): Promise<RunSummary> {
    if (result.cleanup.cleanupCompleted) return result.summary;
    const hooks = this.runLifecycle(this.env);
    if (!hooks) {
      throw new Error('run termination requires lifecycle cleanup hooks');
    }
    await hooks.abandonApprovals(workflowId, runId, result.cleanup.status);
    if (result.cleanup.scheduleDispatch) {
      if (!hooks?.discardScheduleDispatch) {
        throw new Error(
          'scheduled run termination requires a dispatch-settlement hook',
        );
      }
      await hooks.discardScheduleDispatch(
        result.cleanup.scheduleDispatch.scheduleId,
        result.cleanup.scheduleDispatch.dispatchId,
        runId,
      );
    }
    const ownership = this.runOwnership(this.env);
    if (!ownership.release) {
      throw new Error('run termination requires ownership release support');
    }
    const released = await ownership.release('run', runId, owner);
    if (!released) {
      const current = await ownership.owner('run', runId);
      if (current) {
        throw new Error(`run '${runId}' ownership could not be released`);
      }
    }
    return runtime.completeTerminalCleanup(
      workflowId,
      runId,
      result.cleanup.revision,
    );
  }

  async #startSource(
    principal: ExecutionPrincipal,
    workflowId: string,
    runId: string,
    scheduleId: unknown,
    dispatchId: unknown,
  ): Promise<{
    owner: DurableObjectRunOwner;
    target?: ScheduleSourceWorkflowTarget;
  }> {
    if (scheduleId === undefined) {
      if (dispatchId !== undefined) {
        throw new InvalidRunRequestError(
          'dispatchId is only valid for a scheduled run',
        );
      }
      return {
        owner: Object.freeze({ kind: principal.kind, id: principal.id }),
      };
    }
    if (!isPathSafeId(scheduleId) || !isPathSafeId(dispatchId)) {
      throw new InvalidRunRequestError(
        'scheduleId and dispatchId are required path-safe identifiers',
      );
    }
    const schedules = this.scheduleSource(this.env);
    const source = schedules
      ? await resolveScheduleStartOwner(
          schedules,
          this.runOwnership(this.env),
          scheduleId,
          dispatchId,
          runId,
          { type: 'workflow', workflowId },
        )
      : undefined;
    if (!source) {
      throw new InvalidRunRequestError(
        'scheduled run source is missing or does not match the prepared workflow target',
      );
    }
    return source;
  }

  async #reserveRunOwner(
    runId: string,
    owner: DurableObjectRunOwner,
    token: string,
  ): Promise<void> {
    if (
      !(await this.runOwnership(this.env).reserveAll(
        [{ kind: 'run', resourceId: runId }],
        owner,
        token,
      ))
    ) {
      throw new Error(
        `run '${runId}' is unavailable or owned by another principal`,
      );
    }
  }

  async #withOperationLock<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.#operationTail;
    let release: () => void = () => undefined;
    this.#operationTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  async #armRunOwnerRecovery(recovery: RunOwnerRecovery): Promise<void> {
    const storage = this.state?.storage;
    if (!storage) return;
    if (!storage.setAlarm) {
      throw new Error('run owner recovery requires Durable Object alarms');
    }
    await storage.setAlarm(Date.now() + RUN_OWNER_RECOVERY_DELAY_MS);
    await storage.put(RUN_OWNER_RECOVERY_KEY, recovery);
    await storage.setAlarm(Date.now() + RUN_OWNER_RECOVERY_DELAY_MS);
  }

  async #clearRunOwnerRecovery(): Promise<void> {
    const storage = this.state?.storage;
    if (!storage) return;
    await storage.delete(RUN_OWNER_RECOVERY_KEY);
    await storage.deleteAlarm?.();
  }

  async #clearRunOwnerRecoveryBestEffort(): Promise<void> {
    try {
      await this.#clearRunOwnerRecovery();
    } catch (error) {
      console.error('run owner recovery cleanup failed', error);
    }
  }

  async #settleRunOwnerBestEffort(
    recovery: RunOwnerRecovery,
    release: boolean,
  ): Promise<void> {
    try {
      await this.runOwnership(this.env).settleReservation(
        recovery.token,
        release ? [{ kind: 'run', resourceId: recovery.runId }] : [],
      );
      await this.#clearRunOwnerRecoveryBestEffort();
    } catch (error) {
      console.error('run owner recovery settlement failed', error);
      try {
        await this.#rearmRunOwnerRecovery();
      } catch (alarmError) {
        console.error('run owner recovery rearm failed', alarmError);
      }
    }
  }

  async #rearmRunOwnerRecovery(): Promise<void> {
    const storage = this.state?.storage;
    if (!storage?.setAlarm) {
      throw new Error('run owner recovery requires Durable Object alarms');
    }
    await storage.setAlarm(Date.now() + RUN_OWNER_RECOVERY_DELAY_MS);
  }

  async #recoverRunOwner(recovery: RunOwnerRecovery): Promise<void> {
    const summary = await this.#ensureRuntime().recoverStartAttempt(
      recovery.workflowId,
      recovery.runId,
      recovery.token,
    );
    await this.runOwnership(this.env).settleReservation(
      recovery.token,
      summary ? [] : [{ kind: 'run', resourceId: recovery.runId }],
    );
    await this.#clearRunOwnerRecovery();
  }

  #runOwnerRecovery(value: unknown): RunOwnerRecovery {
    if (value === null || typeof value !== 'object') {
      throw new Error('stored run owner recovery is malformed');
    }
    const stored = value as Partial<RunOwnerRecovery>;
    if (
      stored.version !== 1 ||
      !isPathSafeId(stored.workflowId) ||
      !isPathSafeId(stored.runId) ||
      !isPathSafeId(stored.token)
    ) {
      throw new Error('stored run owner recovery is malformed');
    }
    return {
      version: 1,
      workflowId: stored.workflowId,
      runId: stored.runId,
      token: stored.token,
    };
  }

  async #recoverPendingRunOwner(): Promise<void> {
    const stored = await this.state?.storage?.get<unknown>(
      RUN_OWNER_RECOVERY_KEY,
    );
    if (stored !== undefined) {
      await this.#recoverRunOwner(this.#runOwnerRecovery(stored));
    }
  }

  async alarm(): Promise<void> {
    await this.#withOperationLock(async () => {
      await this.#rearmRunOwnerRecovery();
      try {
        await verifyDurableObjectDeploymentIdentity(this.state, this.env);
        const stored = await this.state?.storage?.get<RunOwnerRecovery>(
          RUN_OWNER_RECOVERY_KEY,
        );
        if (!stored) {
          await this.state?.storage?.deleteAlarm?.();
          return;
        }
        await this.#recoverRunOwner(this.#runOwnerRecovery(stored));
      } catch (error) {
        await this.#rearmRunOwnerRecovery();
        throw error;
      }
    });
  }

  async #route(request: Request): Promise<Response> {
    const segments = new URL(request.url).pathname.split('/').filter(Boolean);
    if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);
    const [, workflowId, runId, action] = segments;

    if (request.method === 'POST' && segments.length === 1) {
      return this.#withOperationLock(async () => {
        const principal = this.#trustedExecutionPrincipal(request);
        const body = await readJson<StartBody>(request);
        if (!body || typeof body.workflowId !== 'string') {
          return json({ error: 'workflowId is required' }, 400);
        }
        // The DO never generates a runId (INV-1): the trusted Worker mints the
        // id and addresses this instance with it. A start without one is a
        // caller bug, not a request for generation.
        if (typeof body.runId !== 'string') {
          return json(
            { error: 'runId is required (server-minted by the run router)' },
            400,
          );
        }
        const workflowId = body.workflowId;
        const runId = body.runId;
        if (!isPathSafeId(workflowId) || !isPathSafeId(runId)) {
          throw new InvalidRunRequestError(
            'workflowId and runId must be URL-path-safe identifiers',
          );
        }
        this.#assertRunIdentity(workflowId, runId);
        const source = await this.#startSource(
          principal,
          workflowId,
          runId,
          body.scheduleId,
          body.dispatchId,
        );
        const runtime = this.#ensureRuntime();
        await this.#recoverPendingRunOwner();
        const existing = await runtime.status(workflowId, runId);
        if (existing) {
          const registered = await this.runOwnership(this.env).owner(
            'run',
            runId,
          );
          if (
            !registered ||
            registered.kind !== source.owner.kind ||
            registered.id !== source.owner.id
          ) {
            throw new Error(
              `existing run '${runId}' has no matching committed owner`,
            );
          }
          throw new RunAlreadyExistsError(workflowId, runId, existing.status);
        }
        const recovery: RunOwnerRecovery = {
          version: 1,
          workflowId,
          runId,
          token: crypto.randomUUID(),
        };
        await this.#armRunOwnerRecovery(recovery);
        try {
          await this.#reserveRunOwner(runId, source.owner, recovery.token);
          let summary: RunSummary;
          try {
            summary = await runtime.start(workflowId, {
              runId,
              inputData: source.target
                ? source.target.inputData
                : body.inputData,
              initialState: source.target
                ? source.target.initialState
                : body.initialState,
              ...(source.target?.requestContext !== undefined
                ? { storedRequestContext: source.target.requestContext }
                : {}),
              requestedBy: principal.id,
              requestedByKind: principal.kind,
              attemptToken: recovery.token,
              ...(body.deadlineMs === undefined
                ? {}
                : { deadlineMs: body.deadlineMs as number }),
            });
          } catch (error) {
            let persisted: RunSummary | null | undefined;
            try {
              persisted = await runtime.recoverStartAttempt(
                workflowId,
                runId,
                recovery.token,
              );
            } catch {
              await this.#rearmRunOwnerRecovery();
              throw error;
            }
            if (persisted) {
              await this.#settleRunOwnerBestEffort(recovery, false);
              return json(persisted);
            }
            await this.#settleRunOwnerBestEffort(recovery, true);
            throw error;
          }
          await this.#settleRunOwnerBestEffort(recovery, false);
          // DL-018: the authoritative RunSummary is the run-progress frame; push it
          // to any subscribed run-channel socket at this lifecycle boundary.
          this.#broadcastRunSummary(summary);
          return json(summary);
        } catch (error) {
          const stored = await this.state?.storage?.get<unknown>(
            RUN_OWNER_RECOVERY_KEY,
          );
          if (stored !== undefined) {
            await this.#rearmRunOwnerRecovery();
          }
          throw error;
        }
      });
    }
    if (
      request.method === 'GET' &&
      segments.length === 3 &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const runtime = this.#ensureRuntime();
      const summary = await runtime.status(workflowId, runId);
      if (!summary) return json({ error: 'run not found' }, 404);
      return json(summary);
    }
    if (
      request.method === 'GET' &&
      segments.length === 4 &&
      action === 'dispatch-status' &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      return this.#withOperationLock(async () => {
        const runtime = this.#ensureRuntime();
        await this.#recoverPendingRunOwner();
        const summary = await runtime.status(workflowId, runId);
        if (!summary) return json({ error: 'run not found' }, 404);
        return json(summary);
      });
    }
    if (
      request.method === 'GET' &&
      segments.length === 4 &&
      action === 'stream' &&
      workflowId &&
      runId
    ) {
      const isUpgrade =
        request.headers.get('upgrade')?.toLowerCase() === 'websocket';
      const state = this.state;
      if (!isUpgrade || !state?.acceptWebSocket) {
        // The per-run WS stream needs an Upgrade handshake AND the workerd
        // Hibernatable-WebSocket API; off workerd (node/vitest) or on a plain
        // GET, poll `GET /runs/:workflowId/:runId` instead. Fail with 426
        // (Upgrade Required), never a 500 — the WS path is proven by the spike.
        return json(
          {
            error:
              'websocket upgrade required for run streaming (workerd-only; poll GET /runs/:workflowId/:runId as the fallback)',
          },
          426,
        );
      }
      // The trusted Worker already verified the run ticket and routed by
      // ticket.runId to idFromName; re-bind to this instance's identity (INV-1)
      // before accepting so a mis-routed upgrade is refused.
      this.#assertRunIdentity(workflowId, runId);
      const runtime = this.#ensureRuntime();
      const snapshot = await runtime.status(workflowId, runId);
      if (!snapshot) return json({ error: 'run not found' }, 404);
      const { 0: client, 1: server } = newWebSocketPair();
      state.acceptWebSocket(server);
      // On-connect snapshot: seed the new subscriber with the current
      // authoritative summary (DL-018) so it need not wait for the next
      // lifecycle transition. Nothing to send if the run is not yet queryable.
      safeSend(server, runFrame(snapshot));
      return new Response(null, {
        status: 101,
        webSocket: client,
      } as unknown as ResponseInit);
    }
    if (
      request.method === 'POST' &&
      segments.length === 4 &&
      action === 'resume' &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const body = (await readJson<ResumeBody>(request)) ?? {};
      const requestedBy = this.#requestedBy(body.requestedBy);
      const requestedByKind = this.#requestedByKind(body.requestedByKind);
      return this.#withOperationLock(async () => {
        const runtime = this.#ensureRuntime();
        const summary = await runtime.resume(workflowId, runId, {
          step: body.step,
          resumeData: body.resumeData,
          requestedBy,
          requestedByKind,
          ...(body.deadlineMs === undefined
            ? {}
            : { deadlineMs: body.deadlineMs as number }),
        });
        // DL-018: broadcast the post-resume authoritative summary.
        this.#broadcastRunSummary(summary);
        return json(summary);
      });
    }
    if (
      request.method === 'POST' &&
      segments.length === 4 &&
      (action === 'terminate' || action === 'terminate-replay') &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const principal = this.#trustedExecutionPrincipal(request);
      const runtime = this.#ensureRuntime();
      const preflightOwner = await this.runOwnership(this.env).owner(
        'run',
        runId,
      );
      if (action === 'terminate') {
        await runtime.cancelActiveExecution(workflowId, runId, 'cancelled', [
          principal,
          preflightOwner ?? principal,
        ]);
      }
      return this.#withOperationLock(async () => {
        const owner = await this.runOwnership(this.env).owner('run', runId);
        if (action === 'terminate-replay') {
          const summary = await runtime.status(workflowId, runId);
          if (
            summary?.status !== 'cancelled' &&
            summary?.status !== 'timed_out'
          ) {
            throw new UnknownRunError(workflowId, runId);
          }
        }
        const result = await runtime.terminateAsPrincipal(
          workflowId,
          runId,
          principal,
          owner ?? principal,
        );
        const summary = await this.#finalizeTerminal(
          runtime,
          workflowId,
          runId,
          owner ?? principal,
          result,
        );
        this.#broadcastRunSummary(summary);
        return json(summary);
      });
    }
    if (
      request.method === 'POST' &&
      segments.length === 4 &&
      action === 'deadline' &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const principal = this.#trustedExecutionPrincipal(request);
      const body = (await readJson<DeadlineBody>(request)) ?? {};
      const cas: RunLifecycleCas = {
        expectedRevision: body.expectedRevision as number,
        ...(body.expectedDeadlineAt === undefined
          ? {}
          : { expectedDeadlineAt: body.expectedDeadlineAt as number }),
      };
      const runtime = this.#ensureRuntime();
      const preflightOwner = await this.runOwnership(this.env).owner(
        'run',
        runId,
      );
      await runtime.cancelActiveExecution(
        workflowId,
        runId,
        'timed_out',
        [principal, preflightOwner ?? principal],
        cas,
      );
      return this.#withOperationLock(async () => {
        const owner = await this.runOwnership(this.env).owner('run', runId);
        if (!owner) {
          const summary = await runtime.status(workflowId, runId);
          if (summary?.status !== 'timed_out') {
            throw new UnknownRunError(workflowId, runId);
          }
        }
        const result = await runtime.timeOutAsPrincipal(
          workflowId,
          runId,
          cas,
          principal,
          owner ?? principal,
        );
        if (!result.casMatched || result.cleanup.cleanupCompleted) {
          return json(result.summary);
        }
        const summary = await this.#finalizeTerminal(
          runtime,
          workflowId,
          runId,
          owner ?? principal,
          result,
        );
        this.#broadcastRunSummary(summary);
        return json(summary);
      });
    }
    return json({ error: 'not found' }, 404);
  }

  /**
   * Fan the authoritative RunSummary out to every subscribed run-channel
   * socket after start, resume, terminate, or deadline expiry, plus the
   * on-connect snapshot. No-op when the DO exposes no getWebSockets
   * (node/vitest, or any host without the Hibernatable-WebSocket API), so the
   * HTTP surface is unchanged off workerd.
   */
  #broadcastRunSummary(summary: RunSummary): void {
    const sockets = this.state?.getWebSockets?.();
    if (!sockets) return;
    const frame = runFrame(summary);
    for (const ws of sockets) {
      safeSend(ws, frame);
    }
  }

  // Hibernation wake handlers — workerd invokes these BY NAME on the instance
  // when a hibernated run-channel socket receives a frame, closes, or errors,
  // so they must exist for a live socket to survive DO eviction. The run
  // channel is broadcast-only (progress flows server->client via
  // #broadcastRunSummary), so there is no client->server protocol beyond a
  // keepalive and no per-socket state to release — a closed socket simply
  // drops out of getWebSockets(). Only exercised under workerd (the spike).
  webSocketMessage(ws: WebSocketLike, message: string | ArrayBuffer): void {
    // Lightweight keepalive: answer a client 'ping' with 'pong'.
    if (message === 'ping') {
      ws.send('pong');
    }
  }

  webSocketClose(_ws: WebSocketLike, _code: number, _reason: string): void {
    // Nothing to reconcile; the closed socket leaves getWebSockets() on its own.
  }

  webSocketError(_ws: WebSocketLike, _error: unknown): void {
    // Nothing to reconcile on a broadcast-only channel.
  }
}

/** The run-channel wire frame containing the authoritative RunSummary. */
function runFrame(summary: RunSummary): string {
  return JSON.stringify({ type: 'run', summary });
}

async function readJson<T>(request: Request): Promise<T | null> {
  try {
    return (await request.json()) as T;
  } catch {
    return null;
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}
