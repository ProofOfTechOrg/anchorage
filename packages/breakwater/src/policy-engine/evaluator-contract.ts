// SPDX-License-Identifier: Apache-2.0
// Evaluator contract — the phase, channel, context and evaluator shapes a
// policy is written against.
//
// A type-only leaf, so content-inspection.ts and any other evaluator module
// take the contract without importing the barrel that imports them back.

import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import type { RequestContext } from '@mastra/core/request-context';

import type { PolicyDecision } from './tool-policy.js';

/** Agent lifecycle phase evaluated by a policy. */
export type PolicyPhase = 'input' | 'output';

/**
 * Which output surface the gated text belongs to. 'answer' is the
 * client-visible answer text (and the channel input gating always runs
 * under); 'reasoning' is the model's reasoning trace; 'object' is structured
 * output, gated as the canonical JSON latest snapshot.
 *
 * Under the supported `@mastra/core` peer, the engine sees the `object`
 * channel only for object chunks that flow through the processor chain
 * (model-native streaming). A `generate()` result's parsed object and core's
 * `StructuredOutputProcessor` chunks never pass through the chain.
 * `createGuardedAgent` rejects structured output because a wrapper gate would
 * run only after Mastra had exposed the parsed value. An object-only policy
 * still requires an audit sink so a standalone engine records a fail-closed
 * result-phase coverage error.
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
   * exposes no discrete messages — and empty at a standalone
   * `createContentPolicyGate` boundary, which has only the rendered text. The
   * shipped evaluators read only `text`.
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
