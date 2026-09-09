// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { D1Database } from '@cloudflare/workers-types';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import {
  DirectReferenceJournal,
  DirectReferenceJournalError,
} from '../scripts/direct-reference-journal.js';

const binding = JSON.stringify({
  configSha256: 'a'.repeat(64),
  accountId: 'fixture-account',
});
const token = (operationId: string, revision: number) =>
  JSON.stringify({ version: 1, operationId, revision });

describe.sequential('direct reference journal in native D1', {
  timeout: 30_000,
}, () => {
  let directory: string;
  let server: TestHarness;
  let db: D1Database;

  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'direct-journal-'));
    const main = join(directory, 'worker.ts');
    await writeFile(
      main,
      `import {DirectReferenceJournal} from ${JSON.stringify(fileURLToPath(new URL('../scripts/direct-reference-journal.ts', import.meta.url)))};
      export default {async fetch(request, env) {
        const runKey = new URL(request.url).searchParams.get('run');
        const journal = new DirectReferenceJournal(env.DB, runKey, ${JSON.stringify(binding)});
        if (request.method === 'POST') {
          const start = await journal.freezeStart('inventory-before', async () => ({operationId: crypto.randomUUID(), inputJson: '{"fixture":true}'}));
          await journal.rememberToken('inventory-before', JSON.stringify({version:1, operationId:start.operationId, revision:2}));
          await journal.recordInterruption('{"fixture":"workerd"}');
        }
        return Response.json({operation: await journal.readOperation('inventory-before'), witness: await journal.readInterruption()});
      }};`,
    );
    server = createTestHarness({
      root: directory,
      workers: [
        {
          config: {
            name: 'direct-journal-harness',
            main,
            compatibility_date: '2026-08-06',
            compatibility_flags: ['nodejs_compat'],
            d1_databases: [
              {
                binding: 'DB',
                database_name: 'direct-journal-harness',
                database_id: '00000000-0000-0000-0000-000000000000',
              },
            ],
          },
        },
      ],
    });
    await server.listen();
    db = (await server.getWorker<{ DB: D1Database }>().getEnv()).DB;
  }, 30_000);

  afterAll(async () => {
    try {
      await server?.close();
    } finally {
      if (directory) await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  function fixture() {
    const runKey = randomUUID();
    return { runKey, journal: new DirectReferenceJournal(db, runKey, binding) };
  }

  it.each([
    'revoked',
    'prototype-trap',
    'code-getter',
    'unknown-code',
    'message',
  ] as const)('normalizes hostile database rejection: %s', async (kind) => {
    let rejection: unknown;
    if (kind === 'revoked') {
      const { proxy, revoke } = Proxy.revocable({}, {});
      revoke();
      rejection = proxy;
    } else if (kind === 'prototype-trap')
      rejection = new Proxy(
        {},
        {
          getPrototypeOf() {
            throw new Error('prototype-secret-sentinel');
          },
        },
      );
    else {
      rejection = new DirectReferenceJournalError('missing-start');
      Object.defineProperty(
        rejection,
        kind === 'message' ? 'message' : 'code',
        kind === 'code-getter'
          ? {
              get() {
                throw new Error('code-secret-sentinel');
              },
            }
          : { value: 'unrecognized-secret-sentinel' },
      );
    }
    const wrapped = new Proxy(db, {
      get(target, key) {
        if (key === 'batch')
          return async () => {
            throw rejection;
          };
        const value = Reflect.get(target, key);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const journal = new DirectReferenceJournal(wrapped, randomUUID(), binding);
    const code = kind === 'message' ? 'missing-start' : 'journal-state';
    await expect(
      journal.readOperation('inventory-before'),
    ).rejects.toMatchObject({
      name: 'DirectReferenceJournalError',
      code,
      message: code,
    });
  });

  it('executes the journal inside workerd and reloads its records', async () => {
    const { runKey, journal } = fixture();
    const url = `https://journal.test/?run=${runKey}`;
    const created = await server.getWorker().fetch(url, { method: 'POST' });
    expect(created.status).toBe(200);
    const result = await created.json();
    expect(result).toMatchObject({
      operation: {
        slot: 'inventory-before',
        inputJson: '{"fixture":true}',
        tokenRevision: 2,
      },
      witness: '{"fixture":"workerd"}',
    });
    const read = await server.getWorker().fetch(url);
    expect(read.status).toBe(200);
    expect(await read.json()).toEqual(result);
    expect(await journal.readOperation('inventory-before')).toEqual(
      (result as { operation: unknown }).operation,
    );
  });

  it.each([
    'cleanup-a',
    'decommission-a',
  ] as const)('adopts %s identity only from a Fleet token', async (slot) => {
    const { runKey, journal } = fixture();
    await expect(
      journal.freezeStart(slot, async () => ({
        operationId: randomUUID(),
        inputJson: '{}',
      })),
    ).rejects.toMatchObject({ code: 'journal-state' });
    expect(await journal.readOperation(slot)).toBeUndefined();
    await journal.freezeStart(slot, async () => ({
      operationId: null,
      inputJson: '{}',
    }));
    await db
      .prepare(
        'UPDATE direct_reference_operations SET operation_id=? WHERE run_key=?',
      )
      .bind(randomUUID(), runKey)
      .run();
    await expect(journal.readOperation(slot)).rejects.toMatchObject({
      code: 'journal-state',
    });
  });

  it.each([
    'operation-id',
    'slot-copy',
  ] as const)('detects frozen-start metadata corruption: %s', async (kind) => {
    const { runKey, journal } = fixture();
    await journal.freezeStart('inventory-before', async () => ({
      operationId: randomUUID(),
      inputJson: '{}',
    }));
    if (kind === 'operation-id')
      await db
        .prepare(
          'UPDATE direct_reference_operations SET operation_id=? WHERE run_key=?',
        )
        .bind(randomUUID(), runKey)
        .run();
    else
      await db
        .prepare(
          'INSERT INTO direct_reference_operations (run_key,slot,operation_kind,operation_id,start_json,start_sha256) SELECT run_key,?,operation_kind,operation_id,start_json,start_sha256 FROM direct_reference_operations WHERE run_key=?',
        )
        .bind('inventory-after', runKey)
        .run();
    await expect(
      new DirectReferenceJournal(db, runKey, binding).readOperation(
        kind === 'operation-id' ? 'inventory-before' : 'inventory-after',
      ),
    ).rejects.toMatchObject({ code: 'journal-state' });
  });

  it('rejects token records copied into another identity', async () => {
    const { runKey, journal } = fixture();
    await journal.freezeStart('cleanup-a', async () => ({
      operationId: null,
      inputJson: '{}',
    }));
    await journal.freezeStart('cleanup-b', async () => ({
      operationId: null,
      inputJson: '{}',
    }));
    await journal.rememberToken('cleanup-a', token(randomUUID(), 1));
    await db
      .prepare(`UPDATE direct_reference_operations SET (operation_id,token_json,token_sha256,token_revision) =
      (SELECT operation_id,token_json,token_sha256,token_revision FROM direct_reference_operations WHERE run_key=? AND slot='cleanup-a')
      WHERE run_key=? AND slot='cleanup-b'`)
      .bind(runKey, runKey)
      .run();
    await expect(journal.readOperation('cleanup-b')).rejects.toMatchObject({
      code: 'journal-state',
    });
  });

  it('rejects interruption records copied into another identity', async () => {
    const { runKey, journal } = fixture();
    const other = fixture();
    await journal.recordInterruption('{}');
    await other.journal.readInterruption();
    await db
      .prepare(`UPDATE direct_reference_run SET (interruption_json,interruption_sha256) =
      (SELECT interruption_json,interruption_sha256 FROM direct_reference_run WHERE run_key=?) WHERE run_key=?`)
      .bind(runKey, other.runKey)
      .run();
    await expect(other.journal.readInterruption()).rejects.toMatchObject({
      code: 'journal-state',
    });
  });

  it('serializes concurrent initial starts and replays without producing new input', async () => {
    const { runKey, journal } = fixture();
    const other = new DirectReferenceJournal(db, runKey, binding);
    const first = {
      operationId: randomUUID(),
      inputJson: JSON.stringify({ records: ['first'] }),
    };
    const second = {
      operationId: randomUUID(),
      inputJson: JSON.stringify({ records: ['second'] }),
    };
    const results = await Promise.all([
      journal.freezeStart('inventory-before', async () => first),
      other.freezeStart('inventory-before', async () => second),
    ]);
    expect(results[0]).toEqual(results[1]);
    expect([first.inputJson, second.inputJson]).toContain(
      results[0]?.inputJson,
    );
    const changed = vi.fn(async () => ({
      operationId: randomUUID(),
      inputJson: '{"records":["changed"]}',
    }));
    const reloaded = new DirectReferenceJournal(db, runKey, binding);
    expect(await reloaded.freezeStart('inventory-before', changed)).toEqual(
      results[0],
    );
    expect(changed).not.toHaveBeenCalled();
    expect(Object.isFrozen(results[0])).toBe(true);
  });

  it('refuses a changed run binding without replacing the original', async () => {
    const { runKey, journal } = fixture();
    await journal.readOperation('audit-before');
    const other = new DirectReferenceJournal(
      db,
      runKey,
      '{"accountId":"other"}',
    );
    await expect(other.readOperation('audit-before')).rejects.toMatchObject({
      code: 'run-binding-mismatch',
    });
    expect(
      await db
        .prepare(
          'SELECT binding_json FROM direct_reference_run WHERE run_key=?',
        )
        .bind(runKey)
        .first('binding_json'),
    ).toBe(binding);
  });

  it('keeps the highest same-operation token despite delayed and terminal-like older echoes', async () => {
    const { runKey, journal } = fixture();
    const operationId = randomUUID();
    await journal.freezeStart('cleanup-recovery', async () => ({
      operationId: null,
      inputJson: '{"release":"failed-recovery"}',
    }));
    await Promise.all([
      journal.rememberToken('cleanup-recovery', token(operationId, 4)),
      new DirectReferenceJournal(db, runKey, binding).rememberToken(
        'cleanup-recovery',
        token(operationId, 2),
      ),
    ]);
    await journal.rememberToken('cleanup-recovery', token(operationId, 1));
    await journal.rememberToken('cleanup-recovery', token(operationId, 4));
    const record = await journal.readOperation('cleanup-recovery');
    expect(record).toMatchObject({
      operationId,
      tokenRevision: 4,
      tokenJson: token(operationId, 4),
    });
    expect(record).not.toHaveProperty('complete');
    await expect(
      journal.rememberToken('cleanup-recovery', token(randomUUID(), 5)),
    ).rejects.toMatchObject({ code: 'operation-mismatch' });
    expect(await journal.readOperation('cleanup-recovery')).toEqual(record);
  });

  it('requires a start row and rejects invalid hint metadata', async () => {
    const { journal } = fixture();
    const operationId = randomUUID();
    await expect(
      journal.rememberToken('migration-next', token(operationId, 1)),
    ).rejects.toMatchObject({ code: 'missing-start' });
    await journal.freezeStart('migration-next', async () => ({
      operationId,
      inputJson: '{"records":[]}',
    }));
    for (const invalid of [
      'null',
      '[]',
      JSON.stringify({ operationId, revision: -1 }),
      JSON.stringify({ operationId, revision: 1.5 }),
      JSON.stringify({ revision: 1 }),
    ])
      await expect(
        journal.rememberToken('migration-next', invalid),
      ).rejects.toBeInstanceOf(DirectReferenceJournalError);
    expect(
      (await journal.readOperation('migration-next'))?.tokenJson,
    ).toBeNull();
  });

  it('records one interruption witness across concurrent instances and reload', async () => {
    const { runKey, journal } = fixture();
    const witnesses = [
      '{"revision":2,"claim":"first"}',
      '{"revision":3,"claim":"second"}',
    ];
    const outcomes = await Promise.all(
      witnesses.map((witness) =>
        new DirectReferenceJournal(db, runKey, binding).recordInterruption(
          witness,
        ),
      ),
    );
    expect(outcomes.filter(Boolean)).toHaveLength(1);
    expect(await journal.readInterruption()).toBe(
      witnesses[outcomes.indexOf(true)],
    );
    expect(await journal.recordInterruption('{"revision":4}')).toBe(false);
    expect(
      await new DirectReferenceJournal(db, runKey, binding).readInterruption(),
    ).toBe(witnesses[outcomes.indexOf(true)]);
  });

  it.each([
    'json',
    'hash',
    'kind',
    'token-pair',
  ] as const)('refuses corrupted stored %s data', async (kind) => {
    const { runKey, journal } = fixture();
    const operationId = randomUUID();
    await journal.freezeStart('audit-before', async () => ({
      operationId,
      inputJson: '{"records":[]}',
    }));
    if (kind === 'json')
      await db
        .prepare(
          'UPDATE direct_reference_operations SET start_json=?,start_sha256=? WHERE run_key=?',
        )
        .bind(
          'bad-json',
          createHash('sha256').update('bad-json').digest('hex'),
          runKey,
        )
        .run();
    if (kind === 'hash')
      await db
        .prepare(
          'UPDATE direct_reference_operations SET start_sha256=? WHERE run_key=?',
        )
        .bind('0'.repeat(64), runKey)
        .run();
    if (kind === 'kind')
      await db
        .prepare(
          'UPDATE direct_reference_operations SET operation_kind=? WHERE run_key=?',
        )
        .bind('inventory', runKey)
        .run();
    if (kind === 'token-pair') {
      await journal.rememberToken('audit-before', token(operationId, 1));
      await db
        .prepare(
          'UPDATE direct_reference_operations SET token_revision=2 WHERE run_key=?',
        )
        .bind(runKey)
        .run();
    }
    await expect(
      new DirectReferenceJournal(db, runKey, binding).readOperation(
        'audit-before',
      ),
    ).rejects.toMatchObject({ code: 'journal-state' });
  });

  it('enforces nullable token pairs in the actual database', async () => {
    const { runKey, journal } = fixture();
    await journal.freezeStart('decommission-a', async () => ({
      operationId: null,
      inputJson: '{}',
    }));
    await expect(
      db
        .prepare(
          'UPDATE direct_reference_operations SET token_revision=1 WHERE run_key=?',
        )
        .bind(runKey)
        .run(),
    ).rejects.toThrow(/constraint/i);
    expect(
      (await journal.readOperation('decommission-a'))?.tokenRevision,
    ).toBeNull();
  });

  it('refuses ignored hint and witness writes instead of claiming success', async () => {
    const { runKey, journal } = fixture();
    const operationId = randomUUID();
    await journal.freezeStart('migration-next', async () => ({
      operationId,
      inputJson: '{}',
    }));
    const suffix = runKey.replaceAll('-', '');
    await db
      .prepare(
        `CREATE TRIGGER ignore_hint_${suffix} BEFORE UPDATE OF token_json ON direct_reference_operations WHEN NEW.run_key='${runKey}' BEGIN SELECT RAISE(IGNORE); END`,
      )
      .run();
    await expect(
      journal.rememberToken('migration-next', token(operationId, 1)),
    ).rejects.toMatchObject({ code: 'journal-state' });
    await db
      .prepare(
        `CREATE TRIGGER ignore_witness_${suffix} BEFORE UPDATE OF interruption_json ON direct_reference_run WHEN NEW.run_key='${runKey}' BEGIN SELECT RAISE(IGNORE); END`,
      )
      .run();
    await expect(
      journal.recordInterruption('{"revision":2}'),
    ).rejects.toMatchObject({ code: 'journal-state' });
    expect(await journal.readInterruption()).toBeNull();
  });

  it('bounds serialized control inputs before inserting an operation', async () => {
    const { journal } = fixture();
    await expect(
      journal.freezeStart('inventory-after', async () => ({
        operationId: randomUUID(),
        inputJson: JSON.stringify({ value: 'x'.repeat(256 * 1024) }),
      })),
    ).rejects.toMatchObject({ code: 'journal-state' });
    expect(await journal.readOperation('inventory-after')).toBeUndefined();
  });

  it('retries failed initialization and hides the underlying error', async () => {
    let failed = false;
    const wrapped = new Proxy(db, {
      get(target, key) {
        if (key === 'batch')
          return async (statements: Parameters<D1Database['batch']>[0]) => {
            if (!failed) {
              failed = true;
              throw new Error('secret-sentinel');
            }
            return target.batch(statements);
          };
        const value = Reflect.get(target, key);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const journal = new DirectReferenceJournal(wrapped, randomUUID(), binding);
    await expect(
      journal.readOperation('inventory-before'),
    ).rejects.toMatchObject({
      code: 'journal-state',
      message: 'journal-state',
    });
    await expect(
      journal.readOperation('inventory-before'),
    ).resolves.toBeUndefined();
  });
});
