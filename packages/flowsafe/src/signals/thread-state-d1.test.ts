// SPDX-License-Identifier: Apache-2.0
// D1ThreadStateStorage get/set/delete round-trip — the state-signal lanes and the
// goal record (Track F) both ride this (threadId, type) key.

import { describe, expect, it } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import type { SignalDatabase } from './d1-shared.js';
import { D1ThreadStateStorage } from './thread-state-d1.js';

function store(): D1ThreadStateStorage {
  const db = sqliteUnitDatabase(openSqlite()) as unknown as SignalDatabase;
  return new D1ThreadStateStorage(db, '');
}

describe('D1ThreadStateStorage', () => {
  it('sets and gets a value for a (threadId, type) pair', async () => {
    const s = store();
    await s.setState({
      threadId: 'acme_t1',
      type: 'goal',
      value: { objective: 'ship it', status: 'active', runsUsed: 0 },
    });
    expect(await s.getState({ threadId: 'acme_t1', type: 'goal' })).toEqual({
      objective: 'ship it',
      status: 'active',
      runsUsed: 0,
    });
  });

  it('returns undefined for an unset pair', async () => {
    const s = store();
    expect(
      await s.getState({ threadId: 'acme_t1', type: 'task' }),
    ).toBeUndefined();
  });

  it('is full-replacement (setState overwrites, not merges)', async () => {
    const s = store();
    await s.setState({
      threadId: 'acme_t1',
      type: 'goal',
      value: { a: 1, b: 2 },
    });
    await s.setState({ threadId: 'acme_t1', type: 'goal', value: { a: 9 } });
    expect(await s.getState({ threadId: 'acme_t1', type: 'goal' })).toEqual({
      a: 9,
    });
  });

  it('keeps (threadId, type) pairs disjoint', async () => {
    const s = store();
    await s.setState({ threadId: 'acme_t1', type: 'goal', value: 'g' });
    await s.setState({ threadId: 'acme_t1', type: 'task', value: 't' });
    await s.setState({ threadId: 'other_t1', type: 'goal', value: 'x' });
    expect(await s.getState({ threadId: 'acme_t1', type: 'goal' })).toBe('g');
    expect(await s.getState({ threadId: 'acme_t1', type: 'task' })).toBe('t');
    expect(await s.getState({ threadId: 'other_t1', type: 'goal' })).toBe('x');
  });

  it('deletes a value', async () => {
    const s = store();
    await s.setState({ threadId: 'acme_t1', type: 'goal', value: 'g' });
    await s.deleteState({ threadId: 'acme_t1', type: 'goal' });
    expect(
      await s.getState({ threadId: 'acme_t1', type: 'goal' }),
    ).toBeUndefined();
  });
});
