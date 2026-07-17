// SPDX-License-Identifier: Apache-2.0
// D1NotificationsStorage round-trip / coalescing / listDue / update — mirrors the
// core InMemoryNotificationsStorage behavior over the real node:sqlite adapter.

import { describe, expect, it } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import type { SignalDatabase } from './d1-shared.js';
import { D1NotificationsStorage } from './notifications-d1.js';

function store(): D1NotificationsStorage {
  const db = d1DatabaseLike(openSqlite()) as unknown as SignalDatabase;
  return new D1NotificationsStorage(db, '');
}

describe('D1NotificationsStorage', () => {
  it('creates and round-trips a notification with defaults', async () => {
    const s = store();
    const created = await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'pr.opened',
      summary: 'PR #1 opened',
      payload: { number: 1 },
    });
    expect(created.status).toBe('pending');
    expect(created.priority).toBe('medium');
    expect(created.coalescedCount).toBe(1);
    const got = await s.getNotification({
      threadId: 'acme_t1',
      id: created.id,
    });
    expect(got?.summary).toBe('PR #1 opened');
    expect(got?.payload).toEqual({ number: 1 });
    expect(got?.createdAt).toBeInstanceOf(Date);
  });

  it('coalesces onto a pending record sharing a coalesceKey (bumps the count)', async () => {
    const s = store();
    const first = await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'CI running',
      coalesceKey: 'ci-run-9',
    });
    const second = await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'CI passed',
      coalesceKey: 'ci-run-9',
    });
    // Same record id, summary refreshed, count bumped.
    expect(second.id).toBe(first.id);
    expect(second.summary).toBe('CI passed');
    expect(second.coalescedCount).toBe(2);
    const list = await s.listNotifications({ threadId: 'acme_t1' });
    expect(list).toHaveLength(1);
  });

  it('does NOT coalesce across a different kind, source, or resource', async () => {
    const s = store();
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'a',
      coalesceKey: 'k',
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'deploy', // different kind
      summary: 'b',
      coalesceKey: 'k',
    });
    expect(await s.listNotifications({ threadId: 'acme_t1' })).toHaveLength(2);
  });

  it('listDueNotifications returns only pending rows past their deliverAt', async () => {
    const s = store();
    const now = new Date('2026-01-01T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'due',
      deliverAt: new Date(now.getTime() - 1000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'b',
      summary: 'future',
      deliverAt: new Date(now.getTime() + 60_000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'c',
      summary: 'no deliverAt (never due via listDue)',
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['due']);
  });

  it('updateNotification stamps the status timestamp and filters by status', async () => {
    const s = store();
    const created = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'hi',
    });
    const updated = await s.updateNotification({
      threadId: 'acme_t1',
      id: created.id,
      status: 'delivered',
    });
    expect(updated.status).toBe('delivered');
    expect(updated.deliveredAt).toBeInstanceOf(Date);
    const pending = await s.listNotifications({
      threadId: 'acme_t1',
      status: 'pending',
    });
    expect(pending).toHaveLength(0);
    const delivered = await s.listNotifications({
      threadId: 'acme_t1',
      status: ['delivered', 'seen'],
    });
    expect(delivered).toHaveLength(1);
  });

  it('updateNotification throws for a missing record', async () => {
    const s = store();
    await expect(
      s.updateNotification({ threadId: 'acme_t1', id: 'nope', status: 'seen' }),
    ).rejects.toThrow(/not found/i);
  });

  it('scopes list/get to the threadId (a foreign thread never bleeds in)', async () => {
    const s = store();
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'acme',
    });
    await s.createNotification({
      threadId: 'other_t1',
      source: 'x',
      kind: 'a',
      summary: 'other',
    });
    const acme = await s.listNotifications({ threadId: 'acme_t1' });
    expect(acme).toHaveLength(1);
    expect(acme[0]?.summary).toBe('acme');
  });

  it('coalesces DETERMINISTICALLY onto the earliest-created candidate (ORDER BY)', async () => {
    // #given — two DISTINCT pending records of one (thread, source, kind): one
    // matchable by dedupeKey, one by coalesceKey. The EARLIER-created is INSERTED
    // SECOND, so insertion (rowid) order and createdAt order DISAGREE — the
    // ORDER BY createdAt is the only thing that makes the pick deterministic.
    const s = store();
    const early = new Date('2026-01-01T00:00:00.000Z');
    const late = new Date('2026-01-01T00:05:00.000Z');
    const lateRec = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'late',
      dedupeKey: 'd',
      createdAt: late,
    });
    const earlyRec = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'early',
      coalesceKey: 'c',
      createdAt: early,
    });
    expect(earlyRec.id).not.toBe(lateRec.id);

    // #when — a create matching BOTH keys
    const merged = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'merged',
      dedupeKey: 'd',
      coalesceKey: 'c',
    });

    // #then — it coalesces onto the earliest-created (NOT the first-inserted),
    // proving ORDER BY createdAt rather than unspecified row order.
    expect(merged.id).toBe(earlyRec.id);
    expect(merged.coalescedCount).toBe(2);
    expect(await s.listNotifications({ threadId: 'acme_t1' })).toHaveLength(2);
  });

  it('listDueNotifications treats summaryAt (not just deliverAt) as due', async () => {
    const s = store();
    const now = new Date('2026-01-01T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'summary-due',
      summaryAt: new Date(now.getTime() - 1000),
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['summary-due']);
  });

  it('listDueNotifications is inclusive at the boundary (deliverAt === now is due)', async () => {
    const s = store();
    const now = new Date('2026-01-01T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'exact',
      summary: 'exact',
      deliverAt: now,
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'future',
      summary: 'future',
      deliverAt: new Date(now.getTime() + 1),
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['exact']);
  });

  it('listDueNotifications returns a BATCH ordered by earliest due time, honoring limit', async () => {
    const s = store();
    const now = new Date('2026-01-02T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'b',
      summary: 'second',
      deliverAt: new Date(now.getTime() - 1000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'first',
      deliverAt: new Date(now.getTime() - 5000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'c',
      summary: 'third',
      summaryAt: new Date(now.getTime() - 100),
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['first', 'second', 'third']);
    const limited = await s.listDueNotifications({ now, limit: 2 });
    expect(limited.map((r) => r.summary)).toEqual(['first', 'second']);
  });
});
