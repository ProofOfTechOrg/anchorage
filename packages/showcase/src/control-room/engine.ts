// The control room's enforcement plumbing: drive the REAL breakwater
// primitives (PolicyEngine, RBACMiddleware, the tool-policy evaluators,
// AuditLogger) over scripted agent output, exactly the way the library's own
// tests do — synthetic Mastra stream parts, one shared `state` object per
// stream, an `abort` that throws, and the runner's reprocess convention for
// hold-back flushes. Nothing here fakes a decision: every allow/deny in the
// control plane is a real evaluator/gate result. Only the agent output is
// scripted (the demo is deterministic by design).
//
// Import discipline: ONLY breakwater's browser-clean subpaths (policy-engine,
// rbac, audit) and @mastra/core/request-context. The barrel and connector-sdk
// pull @mastra/core/tools (codeMode → node child_process/fs), which cannot be
// bundled for the browser — so createConnector is deliberately not used here;
// the connector gates are composed from their underlying evaluators instead.
//
// DOM-free on purpose — engine.test.ts runs this under node, including the
// zero-leak assertion that no character of a detected span is ever emitted.

import { RequestContext } from '@mastra/core/request-context';
import { type AuditEvent, AuditLogger } from '@proofoftech/breakwater/audit';
import type {
  PolicyDecision,
  PolicyEngine,
  ToolCallContext,
  ToolPolicyEvaluator,
} from '@proofoftech/breakwater/policy-engine';

/** The enforcement layers a scenario exercises (card badges + event rows). */
export type GuardrailLayer =
  | 'policy'
  | 'rbac'
  | 'egress'
  | 'isolation'
  | 'approval'
  | 'audit';

/**
 * One control-plane entry. 'audit' rows carry the REAL AuditEvent a gate
 * recorded (the same record a production sink would export to a SIEM);
 * 'blocked' marks the moment a guardrail stopped the scenario; 'note' rows
 * narrate scripted context (clearly not enforcement output).
 */
export type EngineEvent =
  | { kind: 'audit'; audit: AuditEvent }
  | { kind: 'blocked'; layer: GuardrailLayer; reason: string }
  | { kind: 'note'; text: string };

/** What a finished scenario reports back to the card. */
export interface ScenarioOutcome {
  /** 'blocked' means a guardrail fired (the demo's success case). */
  status: 'blocked' | 'clean';
  headline: string;
}

/** Everything a scenario needs from the UI shell, injected for testability. */
export interface ScenarioContext {
  /** The signed-in actor, from the server's catalog echo. */
  actor: { id: string; role: string };
  /** The sandbox tenant, segmenting isolation scope. */
  tenantId: string;
  /** Append already-released text to the agent transcript. */
  emitText(text: string): void;
  emitEvent(event: EngineEvent): void;
  /** Pacing seam; tests inject an instant resolve. */
  sleep(ms: number): Promise<void>;
}

/** Thrown by the injected `abort` — the processor contract expects a throw. */
export class ScenarioTripwire extends Error {
  constructor(reason: string) {
    super(reason);
    this.name = 'ScenarioTripwire';
  }
}

/** An AuditLogger whose sink forwards every record to the control plane. */
export function scenarioAudit(
  emitEvent: (event: EngineEvent) => void,
): AuditLogger {
  return new AuditLogger({
    sink: (audit) => {
      emitEvent({ kind: 'audit', audit });
    },
  });
}

/** A RequestContext seeded with the given entries (actor, scopes). */
export function contextWith(entries: Record<string, unknown>): RequestContext {
  const requestContext = new RequestContext();
  for (const [key, value] of Object.entries(entries)) {
    requestContext.set(key, value);
  }
  return requestContext;
}

type StreamArgs = Parameters<PolicyEngine['processOutputStream']>[0];
type StreamChunk = StreamArgs['part'];
type InputArgs = Parameters<PolicyEngine['processInput']>[0];

// The core's take-and-clear stash key for end-chunk flushes — literal per
// @mastra/core's stream-reprocess contract, mirrored by breakwater's tests.
const REPROCESS_KEY = '__mastraReprocessPart';

function textDelta(text: string): StreamChunk {
  return {
    runId: 'control-room',
    from: 'AGENT',
    type: 'text-delta',
    payload: { id: 'answer', text },
  } as unknown as StreamChunk;
}

function textEnd(): StreamChunk {
  return {
    runId: 'control-room',
    from: 'AGENT',
    type: 'text-end',
    payload: { id: 'answer' },
  } as unknown as StreamChunk;
}

function finishChunk(): StreamChunk {
  return {
    runId: 'control-room',
    from: 'AGENT',
    type: 'finish',
    payload: {},
  } as unknown as StreamChunk;
}

function textOf(chunk: unknown): string {
  const text = (chunk as { payload?: { text?: unknown } } | null | undefined)
    ?.payload?.text;
  return typeof text === 'string' ? text : '';
}

const abortThrowing = (reason?: string): never => {
  throw new ScenarioTripwire(reason ?? 'blocked');
};

/** Word-ish deltas so the transcript streams like model tokens. */
export function tokenize(transcript: string): readonly string[] {
  return transcript.match(/\S+\s*/g) ?? [];
}

export interface StreamGuardedOptions {
  engine: PolicyEngine;
  transcript: string;
  requestContext: RequestContext;
  emitText(text: string): void;
  sleep(ms: number): Promise<void>;
  /** Delay between deltas; the default reads like token pacing. */
  delayMs?: number;
}

/**
 * Stream a scripted answer through PolicyEngine.processOutputStream. Emits
 * ONLY what the engine releases (under holdBack that lags the frontier by the
 * policies' windows), then drives the end-of-stream flush through the
 * reprocess convention: feed 'text-end'/'finish', and while the engine
 * stashes the trigger under the reprocess key, emit the returned flush delta,
 * clear the stash, and re-feed the trigger. A denial rejects the in-flight
 * call — everything already emitted stays clean, the rest never existed.
 */
export async function streamGuarded(
  options: StreamGuardedOptions,
): Promise<{ blocked: false } | { blocked: true; reason: string }> {
  const { engine, requestContext, emitText, sleep } = options;
  const delayMs = options.delayMs ?? 30;
  const state: Record<string, unknown> = {};
  const streamParts: StreamChunk[] = [];

  const drive = async (part: StreamChunk): Promise<unknown> => {
    streamParts.push(part);
    return engine.processOutputStream({
      part,
      streamParts: [...streamParts],
      state,
      retryCount: 0,
      requestContext,
      abort: abortThrowing,
    } as unknown as StreamArgs);
  };

  // Feed an end/finish trigger, then loop the reprocess convention until the
  // engine stops stashing it (each pass releases one coalesced flush delta).
  const driveFlush = async (trigger: StreamChunk): Promise<void> => {
    let outcome = await drive(trigger);
    while (state[REPROCESS_KEY] !== undefined) {
      const stashed = state[REPROCESS_KEY] as StreamChunk;
      delete state[REPROCESS_KEY];
      const flushed = textOf(outcome);
      if (flushed) {
        emitText(flushed);
        await sleep(delayMs);
      }
      outcome = await drive(stashed);
    }
  };

  try {
    for (const token of tokenize(options.transcript)) {
      const released = await drive(textDelta(token));
      const text = textOf(released);
      if (text) emitText(text);
      await sleep(delayMs);
    }
    await driveFlush(textEnd());
    await driveFlush(finishChunk());
    return { blocked: false };
  } catch (error) {
    if (error instanceof ScenarioTripwire) {
      return { blocked: true, reason: error.message };
    }
    throw error;
  }
}

/** A format-2 user message, the shape breakwater's input gates read. */
function userMessage(text: string): InputArgs['messages'][number] {
  return {
    id: `control-room-${Math.random().toString(36).slice(2)}`,
    role: 'user',
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  } as InputArgs['messages'][number];
}

/**
 * Run one input-phase gate (PolicyEngine.processInput or
 * RBACMiddleware.processInput — same args contract) over a user message.
 */
export async function screenInput(
  gate: {
    processInput(args: InputArgs): unknown;
  },
  text: string,
  requestContext: RequestContext,
): Promise<{ blocked: false } | { blocked: true; reason: string }> {
  try {
    await gate.processInput({
      messages: [userMessage(text)],
      systemMessages: [],
      state: {},
      retryCount: 0,
      requestContext,
      abort: abortThrowing,
      // messageList is required by the type but unread by both gates.
    } as unknown as InputArgs);
    return { blocked: false };
  } catch (error) {
    if (error instanceof ScenarioTripwire) {
      return { blocked: true, reason: error.message };
    }
    throw error;
  }
}

/**
 * Evaluate one tool-policy evaluator (networkEgress, tenantIsolation,
 * crossWorkflowIsolation) over a synthetic tool call — the SAME evaluator the
 * connector SDK runs internally, invoked directly so the browser bundle never
 * pulls the createTool wrapper (which drags @mastra/core/tools → node deps).
 */
export async function evaluateGate(
  evaluator: ToolPolicyEvaluator,
  call: ToolCallContext,
): Promise<PolicyDecision> {
  return evaluator.evaluate(call);
}
