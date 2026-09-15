// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import { ExecutionFenceUnreadableError } from './execution-admission.js';
import {
  decodeRawWorkflowSnapshotResult,
  prepareRawWorkflowSnapshotRead,
  readRawWorkflowSnapshot,
  type SnapshotDatabase,
} from './workflow-snapshot-row.js';

const ADDRESS = {
  tablePrefix: 'tenant_',
  workflowId: 'workflow',
  runId: 'run',
};
const ROW = {
  workflow_name: 'workflow',
  run_id: 'run',
  resourceId: null,
  snapshot: '{"text":"λ"}',
  createdAt: 'unchanged-created',
  updatedAt: 'unchanged-updated',
};

describe('raw workflow snapshots', () => {
  it('rejects inherited slots instead of accepting an apparently populated raw result', () => {
    const rows = Object.setPrototypeOf(
      new Array(1),
      Object.assign(Object.create(Array.prototype), { 0: ROW }),
    );
    expect(Object.hasOwn(rows, 0)).toBe(false);
    expect(() =>
      decodeRawWorkflowSnapshotResult({ results: rows }, ADDRESS),
    ).toThrow('row is malformed');
  });
  it('does not let a custom iterator hide a malformed raw row', () => {
    const rows = [null];
    Object.defineProperty(rows, Symbol.iterator, {
      value: function* () {
        yield ROW;
      },
    });
    expect(() =>
      decodeRawWorkflowSnapshotResult({ results: rows }, ADDRESS),
    ).toThrow('row is malformed');
  });
  it('reads exact six fields and canonical address from native SQLite', async () => {
    const sql = openSqlite();
    sql.exec(
      'CREATE TABLE tenant_mastra_workflow_snapshot (workflow_name TEXT, run_id TEXT, resourceId TEXT, snapshot TEXT, createdAt TEXT, updatedAt TEXT)',
    );
    sql
      .prepare(
        'INSERT INTO tenant_mastra_workflow_snapshot VALUES (?, ?, ?, ?, ?, ?)',
      )
      .run(...Object.values(ROW));
    const db = sqliteUnitDatabase(sql) as SnapshotDatabase;
    const value = await readRawWorkflowSnapshot(
      db,
      { ...ADDRESS, tablePrefix: 'TENANT_' },
      { missingTable: 'error' },
    );
    expect(value).toEqual({
      ...ADDRESS,
      resourceId: null,
      snapshot: ROW.snapshot,
      createdAt: ROW.createdAt,
      updatedAt: ROW.updatedAt,
    });
    expect(Object.isFrozen(value)).toBe(true);
    expect(
      await readRawWorkflowSnapshot(
        db,
        { ...ADDRESS, runId: 'absent' },
        { missingTable: 'error' },
      ),
    ).toBeUndefined();
  });

  it('distinguishes optional table absence from a missing required table', async () => {
    const db = sqliteUnitDatabase(openSqlite()) as SnapshotDatabase;
    await expect(
      readRawWorkflowSnapshot(db, ADDRESS, { missingTable: 'empty' }),
    ).resolves.toBeUndefined();
    await expect(
      readRawWorkflowSnapshot(db, ADDRESS, { missingTable: 'error' }),
    ).rejects.toBeInstanceOf(ExecutionFenceUnreadableError);
    const failure = new Error(
      'no such table: tenant_mastra_workflow_snapshot',
      { cause: new Error('disk corruption') },
    );
    const prepared = db.prepare('SELECT 1');
    prepared.bind = () => prepared;
    prepared.all = vi.fn().mockRejectedValue(failure);
    await expect(
      readRawWorkflowSnapshot({ prepare: () => prepared }, ADDRESS, {
        missingTable: 'empty',
      }),
    ).rejects.toMatchObject({ cause: failure });
  });

  it.each([
    null,
    {},
    { results: [], success: false },
    { results: [], success: undefined },
    { results: [ROW, ROW] },
    { results: new Array(1) },
    { results: [null] },
    ...Object.keys(ROW).map((field) => ({
      results: [
        Object.fromEntries(
          Object.entries(ROW).filter(([key]) => key !== field),
        ),
      ],
    })),
    ...[
      'workflow_name',
      'run_id',
      'snapshot',
      'createdAt',
      'updatedAt',
      'resourceId',
    ].map((field) => ({ results: [{ ...ROW, [field]: 7 }] })),
    { results: [{ ...ROW, run_id: 'other' }] },
    { results: [{ ...ROW, workflow_name: 'other' }] },
  ])('refuses malformed rows or envelopes %#', (result) => {
    expect(() => decodeRawWorkflowSnapshotResult(result, ADDRESS)).toThrow();
  });

  it('does not confuse SELECT metadata with row cardinality or parse JSON', () => {
    expect(
      decodeRawWorkflowSnapshotResult(
        { results: [], meta: { changes: 1 } },
        ADDRESS,
      ),
    ).toBeUndefined();
    expect(
      decodeRawWorkflowSnapshotResult(
        { results: [{ ...ROW, snapshot: 'not JSON' }], meta: { changes: 0 } },
        ADDRESS,
      )?.snapshot,
    ).toBe('not JSON');
  });

  it('validates and returns one captured envelope array', () => {
    let reads = 0;
    const result = {
      get results() {
        reads += 1;
        return reads === 1 ? [ROW] : [{ ...ROW, run_id: 'replacement' }];
      },
    };
    expect(decodeRawWorkflowSnapshotResult(result, ADDRESS)?.runId).toBe('run');
    expect(reads).toBe(1);
  });

  it('captures each array element before validating and decoding it', () => {
    let reads = 0;
    const rows: unknown[] = [];
    Object.defineProperty(rows, 0, {
      get() {
        return ++reads === 1 ? ROW : undefined;
      },
    });
    expect(
      decodeRawWorkflowSnapshotResult({ results: rows }, ADDRESS)?.runId,
    ).toBe('run');
    expect(reads).toBe(1);
  });

  it('captures address values once before waiting and rejects bad addresses before prepare', async () => {
    const sql = openSqlite();
    const db = sqliteUnitDatabase(sql) as SnapshotDatabase;
    const prepared = db.prepare('SELECT 1');
    prepared.bind = () => prepared;
    let release!: (value: { results: (typeof ROW)[] }) => void;
    prepared.all = vi.fn(
      () =>
        new Promise<{ results: (typeof ROW)[] }>((resolve) => {
          release = resolve;
        }),
    ) as typeof prepared.all;
    const source = { ...ADDRESS };
    const result = readRawWorkflowSnapshot(
      { prepare: () => prepared },
      source,
      { missingTable: 'error' },
    );
    source.runId = 'changed';
    release({ results: [ROW] });
    expect((await result)?.runId).toBe('run');
    const prepare = vi.fn();
    for (const address of [
      { ...ADDRESS, runId: '../bad' },
      { ...ADDRESS, tablePrefix: 'bad-' },
      { ...ADDRESS, tablePrefix: null },
    ]) {
      expect(() =>
        prepareRawWorkflowSnapshotRead({ prepare }, address as typeof ADDRESS),
      ).toThrow();
    }
    expect(prepare).not.toHaveBeenCalled();
  });
});
