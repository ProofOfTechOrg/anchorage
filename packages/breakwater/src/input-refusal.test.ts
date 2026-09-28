// SPDX-License-Identifier: Apache-2.0

import {
  type MastraDBMessage,
  MessageList,
} from '@mastra/core/agent/message-list';
import { describe, expect, it } from 'vitest';

import { stopWithoutCallMessages } from './input-refusal.js';
import { snapshotPromptMessages } from './processor-additions.js';

class Stopped extends Error {}

const stop = (): never => {
  throw new Stopped('stopped');
};

function message(
  id: string,
  text: string,
  role: MastraDBMessage['role'] = 'user',
): MastraDBMessage {
  return {
    id,
    role,
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
}

const idsOf = (list: MessageList): string[] =>
  list.get.all.db().map(({ id }) => id);

describe('stopWithoutCallMessages', () => {
  it('removes the input and response messages and keeps the remembered ones before it stops', () => {
    // #given
    const list = new MessageList()
      .add(message('remembered', 'earlier'), 'memory')
      .add(message('input', 'refused'), 'input')
      .add(message('response', 'added', 'assistant'), 'response');

    // #when / #then
    expect(() => stopWithoutCallMessages(list, {}, stop)).toThrow(Stopped);
    expect(idsOf(list)).toEqual(['remembered']);
  });

  it('stops when there is no message list', () => {
    // #when / #then
    expect(() => stopWithoutCallMessages(undefined, {}, stop)).toThrow(Stopped);
  });

  it('stops, with its own error, when the removal throws', () => {
    // #given — a host-built value in place of Mastra's list
    const notAList = { clear: {} } as unknown as MessageList;

    // #when / #then
    expect(() => stopWithoutCallMessages(notAList, {}, stop)).toThrow(Stopped);
  });

  it('removes the recorded and the unmatched messages when clearing the input and responses throws', () => {
    // #given — a list whose clearing throws
    const list = new MessageList()
      .add(message('remembered', 'earlier'), 'memory')
      .add(message('recorded', 'added by an earlier processor'), 'context');
    const snapshot = snapshotPromptMessages(list);
    list.add(message('unmatched', 'added by this processor'), 'context');
    Object.defineProperty(list, 'clear', {
      get() {
        throw new Error('clear failure');
      },
    });

    // #when / #then
    expect(() =>
      stopWithoutCallMessages(
        list,
        {
          additions: [
            {
              kind: 'message',
              message: message('recorded', 'added by an earlier processor'),
            },
          ],
          snapshot,
        },
        stop,
      ),
    ).toThrow(Stopped);
    expect(idsOf(list)).toEqual(['remembered']);
  });

  it('removes the messages added and changed since the snapshot and keeps an unchanged one', () => {
    // #given
    const list = new MessageList()
      .add(message('unchanged', 'earlier question'), 'memory')
      .add(message('changed', 'earlier answer'), 'memory');
    const snapshot = snapshotPromptMessages(list);
    list.removeByIds(['changed']);
    list.add(message('changed', 'rewritten answer'), 'memory');
    list.add(message('added', 'context note'), 'context');

    // #when / #then
    expect(() => stopWithoutCallMessages(list, { snapshot }, stop)).toThrow(
      Stopped,
    );
    expect(idsOf(list)).toEqual(['unchanged']);
  });
});
