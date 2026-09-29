// SPDX-License-Identifier: Apache-2.0
// The messages a refused call leaves on Mastra's message list, removed before
// an input gate stops the call.
//
// Mastra's durable finish saves the call's input and response messages to
// memory without checking for a tripwire, and titles a new thread from the
// list's messages with the agent's model. A later call does not evaluate
// the history memory loads, so input a gate refused, or text an application
// input processor moved into another message, would otherwise reach a model
// unread.
//
// @internal

import type { MessageList } from '@mastra/core/agent/message-list';

import {
  addedMessageIds,
  type ProcessorAddition,
  type PromptSnapshot,
  takeProcessorAdditions,
  unmatchedMessageIds,
} from './processor-additions.js';

/** @internal What a refusal knows of the application input processors' work. */
export interface Refusal {
  /**
   * The additions the policy engine took from the list's record. Without
   * them, the refusal takes what the record holds.
   */
  readonly additions?: readonly ProcessorAddition[];
  /** The refusing processor's snapshot of the list, taken before it ran. */
  readonly snapshot?: PromptSnapshot | undefined;
}

// Runs each step and then `stop`, even when a step throws. `stop`'s throw
// replaces a step's: an error other than a tripwire would let Mastra's
// durable preparation run the model.
function runThenStop(
  steps: readonly (() => unknown)[],
  stop: () => never,
): never {
  const [step, ...rest] = steps;
  if (step === undefined) return stop();
  try {
    step();
  } finally {
    runThenStop(rest, stop);
  }
}

/**
 * @internal Remove from `messageList` the call's input, its response
 * messages, each message the application input processors' record names, and,
 * with a snapshot, each message the snapshot does not match; then stop the
 * call with `stop`. Without a list nothing is removed. Each removal runs even
 * when an earlier one throws, and `stop` runs last; its throw replaces a
 * removal's.
 */
export function stopWithoutCallMessages(
  messageList: MessageList | null | undefined,
  refusal: Refusal,
  stop: () => never,
): never {
  if (messageList == null) return stop();
  const { snapshot } = refusal;
  return runThenStop(
    [
      () => messageList.clear.input.db(),
      () => messageList.clear.response.db(),
      () =>
        messageList.removeByIds(
          addedMessageIds(
            refusal.additions ?? takeProcessorAdditions(messageList),
          ),
        ),
      ...(snapshot === undefined
        ? []
        : [
            () =>
              messageList.removeByIds(
                unmatchedMessageIds(messageList, snapshot),
              ),
          ]),
    ],
    stop,
  );
}
