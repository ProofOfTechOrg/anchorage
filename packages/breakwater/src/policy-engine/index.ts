// SPDX-License-Identifier: Apache-2.0
// Policy Engine — pre-gate (input) and post-gate (output) policy evaluation
// as a single Mastra processor registered in both inputProcessors and
// outputProcessors. Policies are evaluator functions returning
// { allowed } | { allowed: false, reason }.
//
// Output is gated in two places so agent.stream() cannot leak forbidden text:
// processOutputStream gates each streamed chunk against the output accumulated
// so far, and processOutputResult is the authoritative final gate (and the only
// one for non-streaming agent.generate()).
//
// Output is gated per CHANNEL — 'answer' (client-visible text), 'reasoning'
// (the model's reasoning trace), 'object' (structured-output snapshots) —
// each accumulated and evaluated independently; a policy declares which
// channels it gates (default: answer only). Tool-boundary policies (network
// egress, write-permission, cross-workflow isolation) live in tool-policy.ts,
// enforced by the connector SDK's execute wrapper; data retention is a
// storage-layer property, shipped as flowsafe's purgeExpiredWorkflowRuns —
// see docs/policy-engine-design.md.

import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import type {
  ProcessInputArgs,
  ProcessInputResult,
  ProcessOutputResultArgs,
  ProcessOutputStreamArgs,
  Processor,
} from '@mastra/core/processors';
import type { RequestContext } from '@mastra/core/request-context';
import type { ChunkType } from '@mastra/core/stream';

import { type AuditLogger, agentAuditDetail } from '../audit/index.js';
import { type Actor, actorFromRequestContext } from '../rbac/index.js';
import type { PolicyDecision } from './tool-policy.js';

/** Agent lifecycle phase evaluated by a policy. */
export type PolicyPhase = 'input' | 'output';

/**
 * Which output surface the gated text belongs to. 'answer' is the
 * client-visible answer text (and the channel input gating always runs
 * under); 'reasoning' is the model's reasoning trace; 'object' is structured
 * output, gated as the JSON-stringified latest snapshot.
 *
 * Under `@mastra/core` 1.50, the `object` channel is available only during
 * streaming because the final output result has no structured-object field.
 * Direct structured output is also emitted as answer text, so policies that
 * include `answer` still inspect it. An object-only policy requires an audit
 * sink so the engine can report the non-streaming coverage gap.
 */
export type OutputChannel = 'answer' | 'reasoning' | 'object';

/** Input passed to one policy evaluation. */
export interface PolicyContext {
  /** Lifecycle phase being evaluated. */
  phase: PolicyPhase;
  /** Output channel `text` came from. Always 'answer' in the input phase. */
  channel: OutputChannel;
  /**
   * The gated messages. Empty during streaming output — processOutputStream
   * exposes no discrete messages; the shipped evaluators read only `text`.
   */
  messages: MastraDBMessage[];
  /**
   * Concatenated text of the gated content: input messages, one channel of
   * the streamed output accumulated so far, or the final output result.
   */
  text: string;
  /** Mastra request context associated with the agent call. */
  requestContext?: RequestContext;
  /**
   * Streaming only: a scratch object private to this policy instance that
   * persists across the chunks of one stream (absent in the input/result
   * phases). Evaluators MAY keep incremental-scan cursors here (see
   * denyPatterns); evaluators that ignore it stay pure and re-scan `text`.
   */
  streamState?: Record<string, unknown>;
}

/** Policy evaluated by `PolicyEngine` at selected phases and channels. */
export interface PolicyEvaluator {
  /** Stable policy name used in audit events and denial messages. */
  name: string;
  /** Phases this policy gates. Default: both. */
  phases?: readonly PolicyPhase[];
  /**
   * Output channels this policy gates. Default: ['answer'] — evaluators
   * written before channels existed keep seeing only client-visible text.
   */
  channels?: readonly OutputChannel[];
  /**
   * Hold-back hint (chars): the trailing window of streamed text that must
   * stay unemitted for this policy to catch a violation straddling the
   * emission frontier. Consulted only when the engine's `holdBack` option is
   * on. Policies without the hint contribute 0 — hint your evaluator to get
   * hold-back coverage. `Infinity` buffers everything until stream finish.
   */
  holdBackChars?: number;
  /** Decide whether the supplied policy context is allowed. */
  evaluate(context: PolicyContext): PolicyDecision | Promise<PolicyDecision>;
}

const DEFAULT_CHANNELS: readonly OutputChannel[] = ['answer'];

/** Text parts of format-2 message content, joined for policy matching. */
export function extractMessageText(
  messages: readonly MastraDBMessage[],
): string {
  const chunks: string[] = [];
  for (const message of messages) {
    let hasTextPart = false;
    for (const part of message.content.parts) {
      if (part.type === 'text' && typeof part.text === 'string') {
        chunks.push(part.text);
        hasTextPart = true;
      }
    }
    // content.content usually mirrors the text parts; counting both would
    // double-count length-based policies. It is only authoritative when the
    // message has no text parts (legacy/tool-only shapes).
    if (!hasTextPart && typeof message.content.content === 'string') {
      chunks.push(message.content.content);
    }
  }
  return chunks.join('\n');
}

// Per-stream accumulated text by channel, kept in the processor's `state`
// (core persists that object across every method call of one request), so
// accumulation is O(chunk) instead of rebuilding from all streamParts on
// every chunk. Keys are namespaced: `state` is per-processor, but a subclass
// or wrapper sharing it must not collide with the accumulator.
const CHANNELS_STATE_KEY = 'breakwater.channels';
// Per-policy incremental-scan namespaces, keyed by policy index — names can
// collide across instances (two denyPatterns both named 'deny-patterns');
// indexes cannot.
const POLICY_STATE_KEY = 'breakwater.policyState';

interface ChannelTexts {
  answer: string;
  reasoning: string;
  object: string;
}

function channelTextsOf(state: Record<string, unknown>): ChannelTexts {
  let texts = state[CHANNELS_STATE_KEY] as ChannelTexts | undefined;
  if (!texts) {
    texts = { answer: '', reasoning: '', object: '' };
    state[CHANNELS_STATE_KEY] = texts;
  }
  return texts;
}

function policyStreamStateOf(
  state: Record<string, unknown>,
  index: number,
): Record<string, unknown> {
  let namespaces = state[POLICY_STATE_KEY] as
    | Record<number, Record<string, unknown>>
    | undefined;
  if (!namespaces) {
    namespaces = {};
    state[POLICY_STATE_KEY] = namespaces;
  }
  let namespace = namespaces[index];
  if (!namespace) {
    namespace = {};
    namespaces[index] = namespace;
  }
  return namespace;
}

// ---------------------------------------------------------------------------
// Hold-back buffering (opt-in via PolicyEngineOptions.holdBack)
// ---------------------------------------------------------------------------

// Well-known ProcessorRunner state key: a stream processor may return one
// chunk AND stash a second part under this key; the runner re-drives the
// stashed part through the full output-processor chain after the returned
// chunk is emitted. Not exported from any public @mastra/core subpath —
// literal per @mastra/core dist/processors/stream-reprocess.d.ts
// (REPROCESS_PART_KEY, v1.49.0). Drift protection: the real-ProcessorRunner
// tripwire test in policy-engine.test.ts drives core's actual drain, so a
// core rename/semantics change fails the suite instead of silently
// degrading the finish-flush.
const REPROCESS_PART_KEY = '__mastraReprocessPart';

// Hold-back state, next to the channel accumulator in the processor's
// per-request `state`.
const HOLD_STATE_KEY = 'breakwater.holdBack';

// The two append-only text channels hold-back applies to. The object channel
// needs no window: intermediate snapshots are suppressed outright.
type HoldableChannel = 'answer' | 'reasoning';

type DeltaChunk = Extract<
  ChunkType,
  { type: 'text-delta' | 'reasoning-delta' }
>;

interface HeldChannel {
  /** Evaluated-clean text not yet emitted (the trailing window + backlog). */
  pending: string;
  /** Last delta chunk of this channel — template for coalesced emissions. */
  shape: DeltaChunk;
}

type HoldState = Partial<Record<HoldableChannel, HeldChannel>>;

function holdStateOf(state: Record<string, unknown>): HoldState {
  let hold = state[HOLD_STATE_KEY] as HoldState | undefined;
  if (!hold) {
    hold = {};
    state[HOLD_STATE_KEY] = hold;
  }
  return hold;
}

// Rebuilds on the channel's own chunk shape so ids/metadata stay coherent;
// only the text differs. The cast is sound: shape's type/payload pairing is
// preserved and text is a string in both delta payloads.
function coalescedDelta(shape: DeltaChunk, text: string): DeltaChunk {
  return { ...shape, payload: { ...shape.payload, text } } as DeltaChunk;
}

// Per-channel hold-back window: max holdBackChars over output-phase policies
// gating that channel. Policies without the hint contribute 0.
function holdBackWindowFor(
  policies: readonly PolicyEvaluator[],
  channel: HoldableChannel,
): number {
  let window = 0;
  for (const policy of policies) {
    if (policy.phases && !policy.phases.includes('output')) continue;
    if (!(policy.channels ?? DEFAULT_CHANNELS).includes(channel)) continue;
    window = Math.max(window, policy.holdBackChars ?? 0);
  }
  return window;
}

/** Configuration for `PolicyEngine`. */
export interface PolicyEngineOptions {
  /** Policies evaluated in array order. */
  policies: readonly PolicyEvaluator[];
  /** Optional audit logger for policy decisions and evaluator failures. */
  audit?: AuditLogger;
  /** Audit resource. Defaults to the stable processor identifier. */
  resource?: string;
  /**
   * Opt-in zero-leak streaming: hold back a trailing window of each text
   * channel so a violating span is caught BEFORE any of it is emitted.
   * Windows come from the registered policies' `holdBackChars` hints (per
   * channel, max wins). Released text arrives as modified delta chunks;
   * intermediate 'object' snapshots are suppressed (only a passing
   * 'object-result' is emitted); the held tail is flushed at the channel's
   * end chunk ('text-end'/'reasoning-end') and, as a backstop for streams
   * without end chunks, at 'finish' — both through the runner's reprocess
   * convention, so the flush precedes its end marker. The guarantee is
   * therefore PER SEGMENT: everything flushed at an end chunk was evaluated
   * clean against all channel text so far, but a match completing across
   * segment boundaries (multi-step or multi-text-block runs) aborts the
   * stream after earlier segments were already released — bounded by the
   * window for string patterns, the whole prior segment for RegExp
   * (Infinity) policies. Default false — evaluated chunks flow through
   * unmodified, and already-emitted earlier chunks of a violating span may
   * have leaked by abort time.
   */
  holdBack?: boolean;
}

/**
 * Mastra Processor implementing input/output policy gating — see the module
 * comment for the phase/channel model.
 *
 * Under `@mastra/core` 1.50, a final output result has no structured-object
 * field. The constructor therefore requires an audit sink when a policy
 * selects `object` without `answer`. The first final-result call then emits
 * one coverage warning for that engine instance. Policies that include
 * `answer` inspect the JSON text emitted by direct structured output.
 *
 * The constructor also rejects an explicit input policy whose channels
 * exclude `answer`, because input evaluation has no other channel.
 */
export class PolicyEngine implements Processor<'breakwater-policy-engine'> {
  /** Stable Mastra processor identifier. */
  readonly id = 'breakwater-policy-engine' as const;
  readonly #policies: readonly PolicyEvaluator[];
  readonly #audit?: AuditLogger;
  readonly #resource: string;
  readonly #holdBack: boolean;
  readonly #holdBackWindow: Record<HoldableChannel, number>;
  readonly #objectOnlyPolicyNames: readonly string[];
  #objectChannelFenceWarned = false;

  constructor(options: PolicyEngineOptions) {
    for (const policy of options.policies) {
      if (
        policy.phases?.includes('input') &&
        policy.channels !== undefined &&
        !policy.channels.includes('answer')
      ) {
        throw new TypeError(
          `PolicyEngine: policy '${policy.name}' declares phases including 'input' but channels excluding 'answer' — processInput only ever evaluates the answer channel, so this policy would never run on input. Include 'answer' in channels or drop 'input' from phases.`,
        );
      }
    }
    this.#policies = options.policies;
    this.#audit = options.audit;
    this.#resource = options.resource ?? this.id;
    this.#holdBack = options.holdBack ?? false;
    this.#holdBackWindow = {
      answer: holdBackWindowFor(options.policies, 'answer'),
      reasoning: holdBackWindowFor(options.policies, 'reasoning'),
    };
    this.#objectOnlyPolicyNames = options.policies
      .filter(
        (policy) =>
          policy.channels?.includes('object') &&
          !policy.channels.includes('answer'),
      )
      .map((policy) => policy.name);
    // D1 fence (construction time): an object-only policy has zero
    // result-phase coverage under @mastra/core 1.50.0, and the runtime
    // warning it would otherwise get rides the OPTIONAL audit sink — with no
    // sink the gap is entirely silent. Reject the combination rather than
    // ship an unenforceable policy that can never surface. Reuses the
    // object-only set computed above.
    if (this.#objectOnlyPolicyNames.length > 0 && options.audit === undefined) {
      const names = this.#objectOnlyPolicyNames.join(', ');
      const plural = this.#objectOnlyPolicyNames.length === 1 ? 'y' : 'ies';
      throw new TypeError(
        `PolicyEngine: polic${plural} [${names}] scoped to the 'object' channel without 'answer' cannot be enforced on non-streaming generate() results under @mastra/core 1.50.0 (OutputResult carries no structured-object field) and would silently no-op without an audit sink to carry the one-time warning — provide options.audit, or include 'answer' in channels.`,
      );
    }
  }

  async processInput(args: ProcessInputArgs): Promise<ProcessInputResult> {
    const actor = actorFromRequestContext(args.requestContext) ?? null;
    const evaluated = await this.#evaluate(
      {
        phase: 'input',
        channel: 'answer',
        messages: args.messages,
        text: extractMessageText(args.messages),
        requestContext: args.requestContext,
      },
      actor,
      args.abort,
    );
    this.#recordAllowed('input', actor, evaluated, args.requestContext);
    return args.messages;
  }

  async processOutputResult(
    args: ProcessOutputResultArgs,
  ): Promise<MastraDBMessage[]> {
    this.#warnObjectChannelGapOnce(args.requestContext);
    // result.text is the authoritative generation output (non-optional in
    // core); messages also carry earlier conversation turns the output
    // policies should not re-gate. This is the final gate for both
    // agent.generate() and, after the stream drains, agent.stream().
    const actor = actorFromRequestContext(args.requestContext) ?? null;
    const evaluated = await this.#evaluate(
      {
        phase: 'output',
        channel: 'answer',
        messages: args.messages,
        text: args.result.text,
        requestContext: args.requestContext,
      },
      actor,
      args.abort,
    );
    // Reasoning is gated from the per-step aggregates. OutputResult carries
    // no structured-object field, so the object channel is gated in-stream
    // only ('object'/'object-result' chunks); structured output transported
    // as JSON answer text is covered by the answer pass above.
    const reasoningText = args.result.steps
      .map((step) => step.reasoningText)
      .filter((text): text is string => typeof text === 'string' && text !== '')
      .join('\n');
    if (reasoningText !== '') {
      evaluated.push(
        ...(await this.#evaluate(
          {
            phase: 'output',
            channel: 'reasoning',
            messages: args.messages,
            text: reasoningText,
            requestContext: args.requestContext,
          },
          actor,
          args.abort,
        )),
      );
    }
    // ONE terminal allowed record per result — channel passes aggregated,
    // deduplicated — not one record per channel.
    this.#recordAllowed(
      'output',
      actor,
      [...new Set(evaluated)],
      args.requestContext,
    );
    return args.messages;
  }

  // agent.stream() emits chunks to the client before processOutputResult
  // runs, so the result gate alone lets forbidden output through mid-stream.
  // Each gated chunk feeds its channel's accumulated text — text-delta →
  // answer, reasoning-delta → reasoning, object/object-result → the latest
  // stringified snapshot — and that channel is evaluated on the accumulated
  // total before the chunk is emitted: a length cap needs the cumulative sum,
  // and a pattern split across chunks is caught on the chunk that completes
  // it — aborted before that chunk is emitted. Without holdBack the residual
  // limit is that already-emitted earlier chunks of a violating span have
  // leaked by abort time; with holdBack on, each channel's trailing window
  // stays unemitted, so evaluation always runs on text the client has not
  // fully seen and the abort lands before ANY char of the span is emitted.
  // Ungated chunk types pass through untouched. Either way,
  // processOutputResult remains the authoritative final gate — a driver that
  // never emits 'finish' can truncate hold-back tail emission, but can never
  // leak ungated text.
  async processOutputStream(
    args: ProcessOutputStreamArgs,
  ): Promise<ChunkType | null | undefined> {
    const { part } = args;
    const texts = channelTextsOf(args.state);
    let channel: OutputChannel;
    let delta: DeltaChunk | undefined;
    if (part.type === 'text-delta') {
      // The typeof guard stops a malformed chunk (payload.text not a string)
      // from coercing e.g. "undefined" into the tracked text.
      if (typeof part.payload.text !== 'string') return part;
      texts.answer += part.payload.text;
      channel = 'answer';
      delta = part;
    } else if (part.type === 'reasoning-delta') {
      if (typeof part.payload.text !== 'string') return part;
      texts.reasoning += part.payload.text;
      channel = 'reasoning';
      delta = part;
    } else if (part.type === 'object' || part.type === 'object-result') {
      // Successive 'object' chunks are growing partial snapshots, not deltas
      // (ChunkType, stream/types.d.ts): replace, never concatenate. The
      // stringify lib type lies — it returns undefined for undefined input
      // (a malformed chunk), which must not corrupt the tracked text.
      const snapshot: string | undefined = JSON.stringify(part.object);
      texts.object = snapshot ?? '';
      channel = 'object';
    } else {
      return this.#holdBack ? this.#forwardUngated(args) : part;
    }
    const actor = actorFromRequestContext(args.requestContext) ?? null;
    // No terminal "allowed" record here: one per chunk would flood the audit
    // log. processOutputResult emits the single terminal record at stream end.
    // abortOnError=true: Mastra's stream driver emits the chunk on a raw throw
    // and only suppresses it on a TripWire (abort), so an evaluator crash here
    // must abort, not rethrow.
    await this.#evaluate(
      {
        phase: 'output',
        channel,
        messages: [],
        text: texts[channel],
        requestContext: args.requestContext,
      },
      actor,
      args.abort,
      true,
      args.state,
    );
    if (!this.#holdBack) return part;
    if (delta && (channel === 'answer' || channel === 'reasoning')) {
      return this.#releaseHeld(args, channel, delta);
    }
    // Intermediate 'object' snapshots are suppressed under hold-back
    // (evaluated, never emitted); the final object-result is emitted once it
    // passes. Trade-off: consumers get only the final object.
    return part.type === 'object-result' ? part : null;
  }

  // Hold-back release for a just-evaluated delta. The full accumulated
  // channel text (INCLUDING the held tail) was evaluated clean above, so a
  // violation straddling the emission frontier would already have aborted —
  // everything except the trailing window is therefore releasable, returned
  // as a MODIFIED chunk carrying the releasable prefix; the tail stays
  // pending (null when nothing is releasable yet).
  #releaseHeld(
    args: ProcessOutputStreamArgs,
    channel: HoldableChannel,
    part: DeltaChunk,
  ): ChunkType | null {
    const window = this.#holdBackWindow[channel];
    const hold = holdStateOf(args.state);
    const held = hold[channel];
    // Window 0 (e.g. only length policies): nothing is ever held for this
    // channel; the chunk flows through unmodified.
    if (window === 0 && (!held || held.pending === '')) return part;
    let entry = held;
    if (!entry) {
      entry = { pending: '', shape: part };
      hold[channel] = entry;
    }
    entry.shape = part;
    entry.pending += part.payload.text;
    const releaseLength = Math.max(0, entry.pending.length - window);
    if (releaseLength === 0) return null;
    const releasable = entry.pending.slice(0, releaseLength);
    entry.pending = entry.pending.slice(releaseLength);
    return coalescedDelta(part, releasable);
  }

  // Ungated chunk types under hold-back. A channel's end chunk flushes that
  // channel's held tail; 'finish' drains any channel still pending (streams
  // without end chunks) — both via the reprocess convention, returning the
  // coalesced flush and stashing the trigger part for the runner to re-drive
  // through the chain until nothing is pending. 'error'/'abort' drop
  // pending: the stream is dead, and emitting evaluated-clean tail text
  // after the failure the client already saw would reorder the stream.
  // Everything else passes through with pending untouched (per-delta release
  // already respects the window, so no mid-stream flush is needed).
  #forwardUngated(args: ProcessOutputStreamArgs): ChunkType {
    const { part } = args;
    const hold = holdStateOf(args.state);
    if (part.type === 'error' || part.type === 'abort') {
      hold.answer = undefined;
      hold.reasoning = undefined;
      return part;
    }
    // A channel's end chunk closes its segment: flush the held tail FIRST,
    // then re-drive the end chunk — otherwise the tail would surface after
    // its end marker (or only at finish), reordering the stream for clean
    // runs. The tail was already evaluated clean against the full
    // accumulated channel text, so nothing unvetted is released; the
    // zero-leak guarantee is per segment (see PolicyEngineOptions.holdBack).
    const endedChannel =
      part.type === 'text-end'
        ? ('answer' as const)
        : part.type === 'reasoning-end'
          ? ('reasoning' as const)
          : undefined;
    if (endedChannel) {
      return this.#flushHeld(hold[endedChannel], part, args.state) ?? part;
    }
    if (part.type !== 'finish') return part;
    for (const channel of ['answer', 'reasoning'] as const) {
      const flush = this.#flushHeld(hold[channel], part, args.state);
      if (flush) return flush;
    }
    return part;
  }

  // Coalesce a channel's pending tail into a single delta, stash the
  // triggering part for the runner to re-drive, and return the flush —
  // undefined when nothing is pending. The stash convention is
  // core-version-coupled (REPROCESS_PART_KEY), so every flush goes through
  // this one path.
  #flushHeld(
    held: HeldChannel | undefined,
    part: ChunkType,
    state: Record<string, unknown>,
  ): ChunkType | undefined {
    if (!held || held.pending === '') return undefined;
    const flush = coalescedDelta(held.shape, held.pending);
    held.pending = '';
    state[REPROCESS_PART_KEY] = part;
    return flush;
  }

  // Runs phase- and channel-applicable policies against `context`. On denial
  // or evaluator error it records the audit event and aborts/throws (fail
  // closed), never returning past a violation. Returns the evaluated policy
  // names for the caller's terminal "allowed" record — which the streaming
  // path omits. abortOnError converts an evaluator crash into abort() instead
  // of a raw rethrow; the streaming path needs it (see processOutputStream),
  // while input/result rethrow because core re-throws non-TripWire errors
  // (also failing closed). During streaming, `streamAccumulator` (the
  // processor's per-request state) hands each policy a private namespace,
  // exposed as context.streamState for incremental scanning.
  async #evaluate(
    context: PolicyContext,
    actor: Actor | null,
    abort: (reason: string) => never,
    abortOnError = false,
    streamAccumulator?: Record<string, unknown>,
  ): Promise<string[]> {
    const { phase, channel } = context;
    const evaluated: string[] = [];
    for (const [index, policy] of this.#policies.entries()) {
      if (policy.phases && !policy.phases.includes(phase)) continue;
      if (!(policy.channels ?? DEFAULT_CHANNELS).includes(channel)) continue;
      evaluated.push(policy.name);
      let decision: PolicyDecision;
      try {
        decision = await policy.evaluate(
          streamAccumulator
            ? {
                ...context,
                streamState: policyStreamStateOf(streamAccumulator, index),
              }
            : context,
        );
      } catch (error) {
        // An evaluator crash is worse than a denial; it must not leave less
        // audit evidence than one. Opaque exception text may contain the
        // inspected payload, so the audit and streaming tripwire stay static.
        const reason = 'policy evaluation failed';
        this.#audit?.record({
          actor,
          action: `agent.${phase}.policy`,
          resource: this.#resource,
          decision: 'error',
          reason,
          detail: agentAuditDetail(context.requestContext, {
            policy: policy.name,
            channel,
          }),
        });
        if (abortOnError) abort(reason);
        throw error;
      }
      if (!decision.allowed) {
        const reason = `${policy.name}: ${decision.reason}`;
        this.#audit?.record({
          actor,
          action: `agent.${phase}.policy`,
          resource: this.#resource,
          decision: 'denied',
          reason: 'policy denied',
          detail: agentAuditDetail(context.requestContext, {
            policy: policy.name,
            channel,
          }),
        });
        abort(reason);
      }
    }
    return evaluated;
  }

  // D1 fence (see the class doc + OutputChannel's coverage caveat): a policy
  // scoped to 'object' without 'answer' has zero coverage at the result
  // phase under @mastra/core 1.50.0 — warn once per engine instance instead
  // of silently doing nothing on every processOutputResult call. The
  // constructor now REJECTS this configuration when no audit sink is present
  // (D1), so #audit is guaranteed to exist whenever there is an object-only
  // policy to warn about; the optional-chain remains only as defense in depth.
  #warnObjectChannelGapOnce(requestContext?: RequestContext): void {
    if (
      this.#objectChannelFenceWarned ||
      this.#objectOnlyPolicyNames.length === 0
    ) {
      return;
    }
    this.#objectChannelFenceWarned = true;
    const names = this.#objectOnlyPolicyNames.join(', ');
    const plural = this.#objectOnlyPolicyNames.length === 1 ? 'y' : 'ies';
    this.#audit?.record({
      actor: null,
      action: 'agent.output.policy',
      resource: this.#resource,
      decision: 'error',
      reason: `polic${plural} [${names}] scoped to the 'object' channel without 'answer' cannot be enforced on non-streaming generate() results under @mastra/core 1.50.0 (OutputResult carries no structured-object field) — gate via agent.stream() or include 'answer' in channels`,
      detail: agentAuditDetail(requestContext, {
        policies: [...this.#objectOnlyPolicyNames],
      }),
    });
  }

  #recordAllowed(
    phase: PolicyPhase,
    actor: Actor | null,
    evaluated: string[],
    requestContext?: RequestContext,
  ): void {
    this.#audit?.record({
      actor,
      action: `agent.${phase}.policy`,
      resource: this.#resource,
      decision: 'allowed',
      detail: agentAuditDetail(requestContext, { evaluated }),
    });
  }
}

/**
 * Deny when any pattern matches the gated text. Strings match as
 * case-insensitive substrings; RegExps match as-is. Gates ALL output
 * channels by default (answer, reasoning, object) — leak prevention is its
 * purpose, and a secret is no less leaked through a reasoning trace; narrow
 * with `options.channels` when a channel must stay ungated. The 'object'
 * channel is enforced in-stream only (see OutputChannel's coverage caveat).
 *
 * Substring matching is plain toLowerCase — no Unicode folding or
 * normalization — so alternate spellings evade it (e.g. 'strasse' does not
 * match 'straße'). Do not rely on it alone against adversarial input.
 */
export function denyPatterns(
  patterns: readonly (RegExp | string)[],
  options: {
    name?: string;
    phases?: readonly PolicyPhase[];
    channels?: readonly OutputChannel[];
    /**
     * Override the computed hold-back hint — e.g. a caller-known match
     * bound for a RegExp, which otherwise forces Infinity (buffer-all).
     */
    holdBackChars?: number;
  } = {},
): PolicyEvaluator {
  // g/y-flagged RegExps mutate lastIndex across .test() calls; a shared
  // engine would then let a blocked message through on the next request.
  // Strip those flags once, at construction.
  const compiled = patterns.map((pattern) => {
    if (typeof pattern === 'string') return pattern.toLowerCase();
    return pattern.global || pattern.sticky
      ? new RegExp(pattern.source, pattern.flags.replace(/[gy]/g, ''))
      : pattern;
  });
  const stringPatterns = compiled.filter(
    (pattern): pattern is string => typeof pattern === 'string',
  );
  const allStrings = stringPatterns.length === compiled.length;
  // A substring match ending in newly-appended text must start within
  // maxPatternLength-1 chars before the previous scan frontier, so rescanning
  // only that window is equivalent to rescanning everything — the O(n²)
  // streaming fix.
  const maxPatternLength = stringPatterns.reduce(
    (max, pattern) => Math.max(max, pattern.length),
    0,
  );
  // Zero-leak hint: a string match straddling the emission frontier spans at
  // most maxPatternLength-1 already-held chars; an arbitrary RegExp match is
  // unbounded, so any RegExp forces Infinity unless the caller overrides.
  const holdBackChars =
    options.holdBackChars ??
    (allStrings ? Math.max(0, maxPatternLength - 1) : Number.POSITIVE_INFINITY);
  return {
    name: options.name ?? 'deny-patterns',
    phases: options.phases,
    channels: options.channels ?? ['answer', 'reasoning', 'object'],
    holdBackChars,
    evaluate({ text, channel, streamState }): PolicyDecision {
      // Incremental streaming scan — string patterns only (an arbitrary
      // regex has no bounded lookbehind window, so any RegExp forces a full
      // scan per chunk), and never for the object channel, whose text is a
      // REPLACED snapshot, not append-only.
      if (allStrings && streamState && channel !== 'object') {
        const cursorKey = `scannedUpTo:${channel}`;
        const cursor = streamState[cursorKey];
        const scannedUpTo = typeof cursor === 'number' ? cursor : 0;
        const window = text
          .slice(Math.max(0, scannedUpTo - (maxPatternLength - 1)))
          .toLowerCase();
        for (const pattern of stringPatterns) {
          if (window.includes(pattern)) {
            return {
              allowed: false,
              reason: `matched blocked pattern ${pattern}`,
            };
          }
        }
        streamState[cursorKey] = text.length;
        return { allowed: true };
      }
      const lower = text.toLowerCase();
      for (const pattern of compiled) {
        const matched =
          typeof pattern === 'string'
            ? lower.includes(pattern)
            : pattern.test(text);
        if (matched) {
          return {
            allowed: false,
            reason: `matched blocked pattern ${String(pattern)}`,
          };
        }
      }
      return { allowed: true };
    },
  };
}

/**
 * Deny when the gated text exceeds maxChars. Defaults to the output phase
 * and the answer channel — reasoning does NOT count toward an answer cap.
 * Cap another channel with an explicit second instance, e.g.
 * `maxTextLength(50_000, { channels: ['reasoning'] })`.
 */
export function maxTextLength(
  maxChars: number,
  options: {
    name?: string;
    phases?: readonly PolicyPhase[];
    channels?: readonly OutputChannel[];
  } = {},
): PolicyEvaluator {
  return {
    name: options.name ?? 'max-text-length',
    phases: options.phases ?? ['output'],
    channels: options.channels ?? DEFAULT_CHANNELS,
    // A length violation completes on the current chunk — no straddle window.
    holdBackChars: 0,
    evaluate({ text }): PolicyDecision {
      return text.length <= maxChars
        ? { allowed: true }
        : {
            allowed: false,
            reason: `text length ${text.length} exceeds limit ${maxChars}`,
          };
    },
  };
}

export type {
  ClassifierPolicyOptions,
  PiiSecretsDetectorId,
  PiiSecretsOptions,
} from './content-inspection.js';
// Agent-boundary content-inspection evaluators — see content-inspection.ts.
export {
  classifierPolicy,
  PII_SECRETS_DETECTOR_IDS,
  piiSecrets,
} from './content-inspection.js';
export type {
  BackgroundExecutionOptions,
  CrossWorkflowIsolationOptions,
  NetworkEgressOptions,
  PolicyDecision,
  SideEffect,
  ToolCallContext,
  ToolPolicyEvaluator,
  WritePermissionsPolicy,
} from './tool-policy.js';
// Tool-boundary policies — evaluated by the connector SDK's execute
// wrapper, not this processor. See tool-policy.ts. PolicyDecision lives
// there (the leaf module) and is shared by both seams.
export {
  approvalRequired,
  backgroundExecution,
  crossWorkflowIsolation,
  egressDomainAllowed,
  ISOLATION_SCOPE_CONTEXT_KEY,
  LLM_BACKGROUND_OVERRIDE_KEY,
  networkEgress,
  tenantIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from './tool-policy.js';
