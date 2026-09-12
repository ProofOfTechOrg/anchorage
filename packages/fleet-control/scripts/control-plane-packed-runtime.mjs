// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { copyFile, realpath, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { isAbsolute, join, relative } from 'node:path';
import { createTestHarness } from 'wrangler';

export async function verifyControlPlanePackedRuntime({
  consumerDirectory,
  packageRoot,
}) {
  const fixture = join(consumerDirectory, 'control-plane-packed-worker.ts');
  await copyFile(
    join(packageRoot, 'scripts/control-plane-packed-worker.ts'),
    fixture,
  );
  const require = createRequire(join(consumerDirectory, 'package.json'));
  const entry = await realpath(
    require.resolve('@proofoftech/fleet-control/cloudflare-control-plane'),
  );
  const relativeEntry = relative(await realpath(consumerDirectory), entry);
  assert.ok(
    !relativeEntry.startsWith('..') && !isAbsolute(relativeEntry),
    'Worker entry must resolve inside the installed consumer',
  );
  const packageRequire = createRequire(join(packageRoot, 'package.json'));
  await copyFile(
    packageRequire.resolve('@types/node/async_hooks.d.ts'),
    join(consumerDirectory, 'packed-async-hooks.d.ts'),
  );
  await writeFile(
    join(consumerDirectory, 'tsconfig.worker-runtime.json'),
    JSON.stringify(
      {
        compilerOptions: {
          target: 'ES2022',
          lib: ['ES2022'],
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          strict: true,
          noEmit: true,
          skipLibCheck: false,
          types: ['@cloudflare/workers-types'],
        },
        files: ['control-plane-packed-worker.ts', 'packed-async-hooks.d.ts'],
      },
      null,
      2,
    ),
  );
  execFileSync(
    join(packageRoot, 'node_modules/.bin/tsc'),
    ['-p', 'tsconfig.worker-runtime.json'],
    { cwd: consumerDirectory, encoding: 'utf8', stdio: 'inherit' },
  );
  const server = createTestHarness({
    root: consumerDirectory,
    workers: [
      {
        config: {
          name: 'fleet-control-packed-runtime',
          main: fixture,
          compatibility_date: '2026-08-06',
          d1_databases: ['FLEET_DB', 'QUOTA_DB', 'FIXTURE_DB', 'TENANT_DB'].map(
            (binding, index) => ({
              binding,
              database_name: `packed-${binding.toLowerCase()}`,
              database_id: `00000000-0000-4000-8000-${String(index + 1).padStart(12, '0')}`,
            }),
          ),
          r2_buckets: [{ binding: 'EXPORTS', bucket_name: 'packed-exports' }],
        },
      },
    ],
  });
  const invocations = new Set();
  const evidence = {
    compatibilityDate: '2026-08-06',
    compatibilityFlags: [],
    entry: relativeEntry,
  };
  try {
    await server.listen();
    const worker = server.getWorker();
    async function probe(action, token) {
      const response = await worker.fetch('/packed-control-plane', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          action,
          ...(token === undefined ? {} : { token }),
        }),
        signal: AbortSignal.timeout(30_000),
      });
      const envelope = await response.json();
      assert.equal(response.status, 200, JSON.stringify(envelope));
      assert.equal(invocations.has(envelope.invocationId), false);
      invocations.add(envelope.invocationId);
      return envelope.result;
    }
    const schema = await probe('schema');
    assert.deepEqual(schema.rows, [{ value: 'native-d1' }]);
    assert.deepEqual(schema.batch, [[{ value: 'native-d1' }]]);
    assert.deepEqual(schema.records, [null, null]);
    assert.equal(schema.requests, 0);
    evidence.schema = true;

    const lease = await probe('lease-winner');
    assert.match(JSON.stringify(lease.contender), /already being modified/);
    assert.equal(lease.beforeRelease, 0);
    assert.equal(lease.record.phase, 'cleanup-advancing');
    assert.equal(lease.cleanup.status, 'pending');
    let token = lease.cleanup.token;
    assert.equal(lease.record.cleanupIntent.revision, token.revision);
    const future = await probe('cleanup', {
      ...token,
      revision: token.revision + 1,
    });
    assert.match(JSON.stringify(future.error), /future|ahead/i);
    assert.equal(future.requests, 0);
    const malformed = await probe('cleanup', { invalid: true });
    assert.ok(malformed.error);
    assert.equal(malformed.requests, 0);
    const old = token;
    let terminal;
    let calls = 0;
    for (; calls < 24; calls += 1) {
      const advanced = await probe('cleanup', token);
      assert.equal(advanced.error, undefined, JSON.stringify(advanced));
      assert.ok(advanced.outcome);
      if (advanced.outcome.status === 'complete') {
        assert.equal(advanced.record, undefined);
        assert.deepEqual(advanced.databases, []);
        terminal = advanced.outcome;
        break;
      }
      assert.equal(advanced.outcome.status, 'pending');
      assert.ok(advanced.outcome.token.revision > token.revision);
      token = advanced.outcome.token;
      if (calls === 0) {
        const stale = await probe('cleanup', old);
        assert.deepEqual(stale.outcome.token, token);
        assert.equal(stale.requests, 0);
      }
    }
    assert.ok(terminal, 'bounded cleanup did not reach its receipt');
    const replay = await probe('cleanup', terminal.token);
    assert.deepEqual(replay.outcome.receipt, terminal.receipt);
    assert.equal(replay.requests, 0);
    evidence.lifecycle = {
      continuationFetches: calls + 1,
      receipt: terminal.receipt,
    };

    const quota = await probe('quota');
    assert.equal(quota.acquired, false);
    assert.equal(quota.error.name, 'AbortError');
    assert.equal(quota.count, 1100);
    assert.equal(quota.countAfterFreshInstance, 1100);
    assert.ok(quota.completedBlockedBatches > 0);
    evidence.quota = quota;

    const streamed = await probe('r2-write');
    assert.equal(streamed.error, undefined);
    assert.equal(streamed.size, 1_048_576);
    assert.equal(streamed.bytesEqual, true);
    assert.deepEqual(streamed.readback, streamed.expected);
    assert.equal(streamed.result.sha256, streamed.expected.sha256);
    assert.equal(streamed.result.size, streamed.expected.size);
    const receipt = await probe('receipt');
    const receiptReplay = await probe('receipt');
    assert.equal(receipt.error, undefined);
    assert.equal(receiptReplay.error, undefined);
    assert.equal(receipt.result.size, receipt.expected.size);
    assert.equal(receipt.result.sha256, receipt.expected.sha256);
    assert.deepEqual(receiptReplay.result, receipt.result);
    assert.equal(receiptReplay.objectCount, 1);
    assert.deepEqual(receiptReplay.readback, receipt.expected);
    const collision = await probe('receipt-mismatch');
    assert.match(JSON.stringify(collision.error), /collision differs/);
    assert.equal(collision.objectCount, 1);
    assert.deepEqual(collision.readback, receipt.expected);
    assert.equal(collision.bytesEqual, true);
    evidence.r2 = {
      size: streamed.size,
      sha256: streamed.expected.sha256,
      receipt: receipt.result,
    };

    for (const stale of [false, true]) {
      const queued = await probe(stale ? 'queue-stale' : 'queue-live');
      const b = `packed${stale ? 'stale' : 'live'}b`;
      assert.equal(queued.queuedPhase, 'database-create-authorized');
      assert.equal(queued.beforeRelease, 0);
      const creates = queued.seen.filter(
        (request) => request.method === 'POST' && request.name === b,
      );
      assert.equal(creates.length, stale ? 0 : 1);
      for (const request of queued.seen)
        assert.equal(request.context, request.name?.endsWith('a') ? 'A' : 'B');
      assert.match(JSON.stringify(queued.aError), /packed A held-read failure/);
      assert.match(
        JSON.stringify(queued.bError),
        stale
          ? /lease is no longer owned/
          : /packed B create reached transport/,
      );
      evidence[stale ? 'queuedStaleAuthority' : 'queuedContext'] = queued;
    }
    evidence.fetchInvocations = invocations.size;
    process.stdout.write(
      `fleet-control packed Worker runtime: ${JSON.stringify(evidence)}\n`,
    );
    return evidence;
  } finally {
    await server.close();
  }
}
