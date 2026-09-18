// SPDX-License-Identifier: Apache-2.0

import type {
  D1Database,
  D1PreparedStatement,
} from '@cloudflare/workers-types';
import { describe, expect, it } from 'vitest';
import { D1FleetStateDatabase } from '../src/d1-fleet-state-database.js';
import type { MigrationDatabase } from '../src/migration-ledger.js';

interface PreparedCall {
  readonly sql: string;
  readonly bindings: readonly unknown[];
  readonly bound: boolean;
}

interface FakeScript {
  readonly prepareError?: Error;
  readonly bindError?: Error;
  readonly all?: () => unknown;
  readonly run?: () => unknown;
  readonly batch?: () => unknown;
}

interface FakeDatabase {
  readonly binding: D1Database;
  readonly prepared: PreparedCall[];
  readonly bindCalls: (readonly unknown[])[];
  readonly executedStatements: PreparedCall[];
  readonly batchStatements: (readonly Readonly<PreparedCall>[])[];
}

function envelope(results: readonly unknown[] = []): unknown {
  return { success: true, meta: {}, results };
}

function exactly(message: string): RegExp {
  return new RegExp(`^${message.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`);
}

function fakeDatabase(script: FakeScript = {}): FakeDatabase {
  const prepared: PreparedCall[] = [];
  const bindCalls: (readonly unknown[])[] = [];
  const executedStatements: PreparedCall[] = [];
  const batchStatements: (readonly Readonly<PreparedCall>[])[] = [];
  const statementCalls = new WeakMap<D1PreparedStatement, PreparedCall>();
  const fakeStatement = (call: PreparedCall): D1PreparedStatement => {
    const statement = {
      bind(...bindings: readonly unknown[]) {
        if (script.bindError) throw script.bindError;
        bindCalls.push(bindings);
        return fakeStatement({
          sql: call.sql,
          bindings,
          bound: true,
        });
      },
      all() {
        executedStatements.push(call);
        return Promise.resolve(
          script.all === undefined ? envelope() : script.all(),
        );
      },
      run() {
        executedStatements.push(call);
        return Promise.resolve(
          script.run === undefined ? envelope() : script.run(),
        );
      },
    } as D1PreparedStatement;
    statementCalls.set(statement, call);
    return statement;
  };
  const binding = {
    prepare(sql: string) {
      if (script.prepareError) throw script.prepareError;
      const call: PreparedCall = { sql, bindings: [], bound: false };
      prepared.push(call);
      return fakeStatement(call);
    },
    batch(statements: D1PreparedStatement[]) {
      const calls = statements.map((statement) => {
        const call = statementCalls.get(statement);
        if (!call) throw new Error('fake received an unknown statement');
        return call;
      });
      batchStatements.push(calls);
      return Promise.resolve(
        script.batch === undefined
          ? calls.map(() => envelope())
          : script.batch(),
      );
    },
  } as D1Database;
  return {
    binding,
    prepared,
    bindCalls,
    executedStatements,
    batchStatements,
  };
}

describe('D1FleetStateDatabase', () => {
  it('binds exact values and skips bind for empty bindings', async () => {
    const fake = fakeDatabase();
    const database = new D1FleetStateDatabase(fake.binding);
    const bindings = [1, 'two', null] as const;

    await database.query('SELECT empty');
    await database.query('SELECT bound', bindings);

    expect(fake.prepared).toEqual([
      { sql: 'SELECT empty', bindings: [], bound: false },
      { sql: 'SELECT bound', bindings: [], bound: false },
    ]);
    expect(fake.bindCalls).toEqual([bindings]);
    expect(fake.executedStatements).toEqual([
      { sql: 'SELECT empty', bindings: [], bound: false },
      { sql: 'SELECT bound', bindings, bound: true },
    ]);
  });

  it('returns query rows as received in their original order', async () => {
    const rows = [{ id: 2 }, { id: 1 }];
    const fake = fakeDatabase({ all: () => envelope(rows) });

    const result = await new D1FleetStateDatabase(fake.binding).query(
      'INSERT INTO records RETURNING id',
    );

    expect(result).toBe(rows);
    expect(result).toEqual([{ id: 2 }, { id: 1 }]);
  });

  it('accepts query envelope and meta extensions', async () => {
    const rows = [{ id: 1 }];
    const fake = fakeDatabase({
      all: () => ({
        success: true,
        meta: { duration: 1, rows_read: 2, extra: true },
        results: rows,
        served_by: 'v3',
      }),
    });

    const result = await new D1FleetStateDatabase(fake.binding).query(
      'SELECT extended',
    );

    expect(result).toBe(rows);
  });

  it('resolves execute acknowledgements without results and discards rows', async () => {
    const valid = fakeDatabase({ run: () => envelope([{ changed: 1 }]) });
    await expect(
      new D1FleetStateDatabase(valid.binding).execute('UPDATE records'),
    ).resolves.toBeUndefined();

    const withoutResults = fakeDatabase({
      run: () => ({ success: true, meta: {} }),
    });
    await expect(
      new D1FleetStateDatabase(withoutResults.binding).execute(
        'UPDATE records',
      ),
    ).resolves.toBeUndefined();

    const extended = fakeDatabase({
      run: () => ({
        success: true,
        meta: { duration: 1, extra: true },
        results: [],
        served_by: 'v3',
      }),
    });
    await expect(
      new D1FleetStateDatabase(extended.binding).execute('UPDATE records'),
    ).resolves.toBeUndefined();
  });

  it('returns batch result sets in order and preserves an empty middle set', async () => {
    const rows = [[{ id: 1 }], [], [{ id: 3 }]];
    const fake = fakeDatabase({
      batch: () => rows.map((result) => envelope(result)),
    });

    const result = await new D1FleetStateDatabase(fake.binding).batch([
      { sql: 'one', bindings: ['first'] },
      { sql: 'two' },
      { sql: 'three', bindings: ['third'] },
    ]);

    expect(result).toEqual(rows);
    expect(result[0]).toBe(rows[0]);
    expect(result[1]).toBe(rows[1]);
    expect(result[2]).toBe(rows[2]);
    expect(fake.batchStatements).toEqual([
      [
        { sql: 'one', bindings: ['first'], bound: true },
        { sql: 'two', bindings: [], bound: false },
        { sql: 'three', bindings: ['third'], bound: true },
      ],
    ]);
  });

  it('resolves an empty batch without calling the binding batch method', async () => {
    const fake = fakeDatabase();

    await expect(
      new D1FleetStateDatabase(fake.binding).batch([]),
    ).resolves.toEqual([]);
    expect(fake.batchStatements).toEqual([]);
  });

  it('refuses a batch result count that differs from the statement count', async () => {
    const statements = [{ sql: 'one' }, { sql: 'two' }, { sql: 'three' }];
    const fake = fakeDatabase({
      batch: () => [envelope(), envelope()],
    });
    const message = `D1 returned 2 batch results for ${statements.length} statements`;

    await expect(
      new D1FleetStateDatabase(fake.binding).batch(statements),
    ).rejects.toThrow(exactly(message));
  });

  it('refuses a non-array batch response', async () => {
    const fake = fakeDatabase({ batch: () => envelope() });

    await expect(
      new D1FleetStateDatabase(fake.binding).batch([{ sql: 'one' }]),
    ).rejects.toThrow(exactly('D1 returned a malformed batch response'));
  });

  it('refuses malformed acknowledgements and result shapes', async () => {
    const queryMessage = 'D1 query returned a malformed result';
    const executeMessage = 'D1 execute returned a malformed result';
    const malformedIndex = 1;
    const batchMessage = `D1 batch statement ${malformedIndex} returned a malformed result`;
    const malformedAcknowledgements: readonly unknown[] = [
      null,
      1,
      [],
      { success: false, meta: {}, results: [] },
      { meta: {}, results: [] },
      { success: true, error: 'failed', meta: {}, results: [] },
      { success: true, results: [] },
      { success: true, meta: null, results: [] },
      { success: true, meta: 1, results: [] },
      { success: true, meta: [], results: [] },
    ];

    for (const malformed of malformedAcknowledgements) {
      const query = fakeDatabase({ all: () => malformed });
      await expect(
        new D1FleetStateDatabase(query.binding).query('SELECT malformed'),
      ).rejects.toThrow(exactly(queryMessage));

      const execute = fakeDatabase({ run: () => malformed });
      await expect(
        new D1FleetStateDatabase(execute.binding).execute('UPDATE malformed'),
      ).rejects.toThrow(exactly(executeMessage));

      const batch = fakeDatabase({
        batch: () => [envelope(), malformed],
      });
      await expect(
        new D1FleetStateDatabase(batch.binding).batch([
          { sql: 'valid' },
          { sql: 'malformed' },
        ]),
      ).rejects.toThrow(exactly(batchMessage));
    }

    const malformedResults: readonly unknown[] = [
      { success: true, meta: {} },
      { success: true, meta: {}, results: [1] },
      { success: true, meta: {}, results: [null] },
      { success: true, meta: {}, results: [[]] },
    ];

    for (const malformed of malformedResults) {
      const query = fakeDatabase({ all: () => malformed });
      await expect(
        new D1FleetStateDatabase(query.binding).query('SELECT malformed'),
      ).rejects.toThrow(exactly(queryMessage));

      const batch = fakeDatabase({
        batch: () => [envelope(), malformed],
      });
      await expect(
        new D1FleetStateDatabase(batch.binding).batch([
          { sql: 'valid' },
          { sql: 'malformed' },
        ]),
      ).rejects.toThrow(exactly(batchMessage));
    }
  });

  it('propagates prepare, bind, and binding operation failures unchanged', async () => {
    const prepareError = new Error('prepare failed');
    await expect(
      new D1FleetStateDatabase(fakeDatabase({ prepareError }).binding).query(
        'SELECT failed',
      ),
    ).rejects.toBe(prepareError);

    const bindError = new Error('bind failed');
    await expect(
      new D1FleetStateDatabase(fakeDatabase({ bindError }).binding).query(
        'SELECT failed',
        ['value'],
      ),
    ).rejects.toBe(bindError);

    const allError = new Error('all failed');
    await expect(
      new D1FleetStateDatabase(
        fakeDatabase({ all: () => Promise.reject(allError) }).binding,
      ).query('SELECT failed'),
    ).rejects.toBe(allError);

    const runError = new Error('run failed');
    await expect(
      new D1FleetStateDatabase(
        fakeDatabase({ run: () => Promise.reject(runError) }).binding,
      ).execute('UPDATE failed'),
    ).rejects.toBe(runError);

    const batchError = new Error('batch failed');
    await expect(
      new D1FleetStateDatabase(
        fakeDatabase({ batch: () => Promise.reject(batchError) }).binding,
      ).batch([{ sql: 'failed' }]),
    ).rejects.toBe(batchError);
  });

  it('requires the Workers D1 prepare and batch interface', () => {
    const message =
      'D1FleetStateDatabase requires the Workers D1Database prepare/batch interface';
    expect(() => Reflect.construct(D1FleetStateDatabase, [null])).toThrow(
      exactly(message),
    );
    expect(() =>
      Reflect.construct(D1FleetStateDatabase, [{ prepare() {} }]),
    ).toThrow(exactly(message));
  });

  it('is assignable to the migration database port', () => {
    const binding = fakeDatabase().binding;
    const migration: MigrationDatabase = new D1FleetStateDatabase(binding);

    expect(migration).toBeInstanceOf(D1FleetStateDatabase);
  });
});
