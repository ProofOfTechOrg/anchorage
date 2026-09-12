// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFile, realpath, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { isAbsolute, join, relative } from 'node:path';
import { createTestHarness } from 'wrangler';

const FINDING = {
  tenantTag: 'unknown',
  environment: 'unknown',
  kind: 'stale-route',
  detail: 'packed workload observation',
};

function expectedDigest(count) {
  const hash = createHash('sha256');
  for (let index = 0; index < count; index += 1)
    hash.update(
      `workload${index}\0workload-${index}\0database-${index}\0namespace-${index}\n`,
    );
  return hash.digest('hex');
}

export async function verifyControlPlanePackedWorkload({
  consumerDirectory,
  packageRoot,
}) {
  const fixture = join(consumerDirectory, 'control-plane-packed-workload.ts');
  await copyFile(
    join(packageRoot, 'scripts/control-plane-packed-workload.ts'),
    fixture,
  );
  const require = createRequire(join(consumerDirectory, 'package.json'));
  const entry = await realpath(
    require.resolve('@proofoftech/fleet-control/cloudflare-control-plane'),
  );
  const relativeEntry = relative(await realpath(consumerDirectory), entry);
  assert.ok(
    !relativeEntry.startsWith('..') && !isAbsolute(relativeEntry),
    'workload entry must resolve inside the installed consumer',
  );
  await writeFile(
    join(consumerDirectory, 'tsconfig.worker-workload.json'),
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
        files: ['control-plane-packed-workload.ts'],
      },
      null,
      2,
    ),
  );
  execFileSync(
    join(packageRoot, 'node_modules/.bin/tsc'),
    ['-p', 'tsconfig.worker-workload.json'],
    { cwd: consumerDirectory, stdio: 'inherit' },
  );
  const server = createTestHarness({
    root: consumerDirectory,
    workers: [
      {
        config: {
          name: 'fleet-control-packed-workload',
          main: fixture,
          compatibility_date: '2026-08-06',
          d1_databases: ['FLEET_DB', 'QUOTA_DB'].map((binding, index) => ({
            binding,
            database_name: `workload-${binding.toLowerCase()}`,
            database_id: `00000000-0000-4000-8000-${String(index + 1).padStart(12, '0')}`,
          })),
          r2_buckets: [{ binding: 'EXPORTS', bucket_name: 'workload-exports' }],
        },
      },
    ],
  });
  const invocations = new Set();
  const evidence = {
    entry: relativeEntry,
    compatibilityDate: '2026-08-06',
    compatibilityFlags: [],
    concurrency: 1,
    measurement: {
      elapsed:
        'local end-to-end wall time; includes native D1 and response transport',
      cpuMs: null,
      peakIsolateBytes: null,
      unavailableReason:
        'Wrangler TestHarness exposes runtime logs but no public isolate heap or CPU counter',
      limits:
        'Local success does not establish enforcement of Cloudflare CPU, memory or query limits',
      providerScope:
        'Materialization workload: current records exit audit before provider inspection; earlier large-profile ownership facts are seeded observations',
      d1Rows:
        'Returned operation rows include native lookahead rows; metadata is reported as supplied by local D1',
      d1Scope:
        'Counters cover the factory Fleet binding, excluding fixture setup/readback; binding calls and contained SQL statements are distinct. Production query-budget interpretation is unverified.',
    },
    profiles: {},
  };
  try {
    await server.listen();
    const worker = server.getWorker();
    async function probe(profile, action, extra = {}, acceptError = false) {
      const started = performance.now();
      const response = await worker.fetch('/packed-workload', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ profile, action, ...extra }),
        signal: AbortSignal.timeout(120000),
      });
      const body = await response.json();
      const elapsedMs = performance.now() - started;
      assert.equal(invocations.has(body.invocationId), false);
      invocations.add(body.invocationId);
      if (!acceptError)
        assert.equal(response.status, 200, JSON.stringify(body));
      if (body.providerRequests !== undefined)
        assert.equal(body.providerRequests, 0);
      return { ...body, status: response.status, elapsedMs };
    }
    async function seed(profile, count) {
      let offset = 0;
      let summary;
      for (let calls = 0; calls < Math.ceil(count / 100); calls += 1) {
        const response = await probe(profile, 'seed-generation', { offset });
        assert.ok(response.result.next > offset);
        offset = response.result.next;
        assert.equal(response.result.done, offset === count);
        if (response.result.done) summary = response.result.summary;
      }
      assert.equal(offset, count);
      assert.ok(summary);
      assert.equal(summary.factCount, count * 4);
      assert.ok(summary.payloads.payloadBytes > 0);
      assert.ok(summary.payloads.maximumPayloadBytes <= 16 * 1024);
      return summary;
    }
    async function read(profile, count, routes) {
      const response = await probe(profile, 'read');
      assert.deepEqual(response.result, {
        deployments: count,
        databases: count,
        namespaces: count,
        routes,
        findings: [FINDING],
        identityDigest: expectedDigest(count),
      });
      assert.equal(response.metrics.inventoryFacts, count * 4);
      assert.equal(response.metrics.inventoryRows, count * 4 + routes + 1);
      return response;
    }
    async function start(profile, extra = {}, acceptError = false) {
      const response = await probe(profile, 'start', extra, acceptError);
      if (response.status === 200) {
        assert.equal(response.result.outcome.status, 'pending');
        assert.equal(response.result.pins, 1);
        assert.equal(
          response.result.run.progress.revision,
          response.result.outcome.token.revision,
        );
      }
      return response;
    }
    async function abandon(profile, state = 'failed') {
      const response = await probe(profile, 'abandon');
      assert.equal(response.result.pins, 0);
      assert.equal(response.result.run.state, state);
      return response;
    }
    const profile = 'representative';
    const seeded = await seed(profile, 32);
    const bulk = await read(profile, 32, 0);
    const begun = await start(profile);
    let outcome = begun.result.outcome;
    let calls = 0;
    let maximumWallMs = 0;
    while (outcome.status === 'pending' && calls < 80) {
      const advanced = await probe(profile, 'continue', {
        token: outcome.token,
      });
      assert.equal(
        advanced.result.run.progress.revision,
        advanced.result.outcome.token.revision,
      );
      assert.ok(
        advanced.result.outcome.token.revision > outcome.token.revision,
      );
      maximumWallMs = Math.max(maximumWallMs, advanced.elapsedMs);
      outcome = advanced.result.outcome;
      calls += 1;
    }
    assert.equal(outcome.status, 'complete');
    assert.equal(outcome.result.recordCount, 32);
    assert.equal(outcome.result.findingCount, 1);
    const findings = await probe(profile, 'findings');
    assert.deepEqual(findings.result.page.findings, [FINDING]);
    assert.equal(findings.result.page.done, true);
    assert.equal(findings.result.pins, 1);
    const released = await abandon(profile, 'finalized');
    evidence.profiles[profile] = {
      seed: seeded,
      bulk,
      start: begun,
      continuationFetches: calls,
      maximumWallMs,
      result: outcome.result,
      abandonment: released,
    };

    for (const [name, count] of [
      ['page-boundary', 1001],
      ['record-ceiling', 10000],
    ]) {
      const measured = {
        count,
        checkpoint:
          'real start; prior findings/facts and late cursor seeded directly in native D1',
      };
      evidence.profiles[name] = measured;
      measured.seed = await seed(name, count);
      measured.bulk = await read(name, count, count - 1);
      measured.start = await start(name);
      assert.equal(measured.start.result.intake.count, count);
      let offset = 0;
      let token;
      for (let calls = 0; calls < Math.ceil((count - 1) / 100); calls += 1) {
        const response = await probe(name, 'seed-facts', { offset });
        assert.ok(response.result.next > offset);
        offset = response.result.next;
        if (response.result.done) token = response.result.token;
      }
      assert.equal(offset, count - 1);
      assert.ok(token);
      const advanced = await probe(name, 'continue', { token });
      assert.equal(advanced.result.outcome.status, 'pending');
      assert.deepEqual(advanced.result.outcome.stage, {
        step: 'per-record',
        recordOrdinal: count,
      });
      assert.deepEqual(
        advanced.result.run.progress.stage,
        advanced.result.outcome.stage,
      );
      assert.equal(advanced.result.run.state, 'running');
      assert.equal(advanced.result.pins, 1);
      assert.equal(advanced.result.outcome.token.revision, token.revision + 1);
      assert.equal(
        advanced.result.run.progress.revision,
        advanced.result.outcome.token.revision,
      );
      assert.equal(advanced.metrics.recordPages, Math.ceil(count / 1000));
      assert.equal(
        advanced.metrics.recordRows,
        count + Math.ceil(count / 1000) - 1,
      );
      const facts = 2 * (count - 1);
      assert.equal(advanced.metrics.factPages, Math.ceil(facts / 1000));
      assert.equal(
        advanced.metrics.factRows,
        facts + Math.ceil(facts / 1000) - 1,
      );
      assert.equal(advanced.metrics.inventoryFacts, count * 4);
      const stale = await probe(name, 'continue', { token });
      assert.deepEqual(
        stale.result.outcome.token,
        advanced.result.outcome.token,
      );
      for (const field of [
        'inventoryRows',
        'inventoryFacts',
        'recordPages',
        'factPages',
      ])
        assert.equal(stale.metrics[field], 0);
      measured.continuation = advanced;
      measured.stale = stale;
      measured.abandonment = await abandon(name);
      measured.outcome = 'completed measured calls';
    }

    const corruptStart = await start('page-boundary', { variant: 2 });
    await probe('page-boundary', 'corrupt');
    const corruptRead = await probe('page-boundary', 'read', {}, true);
    assert.equal(corruptRead.status, 500);
    assert.match(corruptRead.error.message, /corrupt|manifest/);
    const corruptAudit = await probe('page-boundary', 'continue', {
      variant: 2,
      token: corruptStart.result.outcome.token,
    });
    assert.equal(corruptAudit.result.outcome.status, 'failed');
    assert.equal(
      corruptAudit.result.outcome.failure.reason,
      'generation-unavailable',
    );
    assert.equal(corruptAudit.result.pins, 0);
    evidence.corruption = { bulk: corruptRead, audit: corruptAudit };

    const bytesProfile = 'intake-byte-boundary';
    const byteSeed = await seed(bytesProfile, 200);
    const byteBulk = await read(bytesProfile, 200, 0);
    const over = await start(bytesProfile, { variant: 3, over: true }, true);
    assert.equal(over.status, 500);
    assert.match(
      over.error.message,
      /canonical intake exceeds the intake byte bound/,
    );
    const refusedState = await probe(bytesProfile, 'state', { variant: 3 });
    assert.equal(refusedState.result.run, null);
    assert.equal(refusedState.result.pins, 0);
    const byteEvidence = {
      seed: byteSeed,
      bulk: byteBulk,
      aboveBound: over,
      refusedState,
    };
    evidence.profiles[bytesProfile] = byteEvidence;
    const exact = await start(bytesProfile);
    byteEvidence.start = exact;
    assert.equal(exact.result.intake.bytes, 16 * 1024 * 1024);
    assert.ok(exact.result.intake.maximumItemBytes < 96 * 1024);
    byteEvidence.continuation = await probe(bytesProfile, 'continue', {
      token: exact.result.outcome.token,
    });
    const byteContinuation = byteEvidence.continuation.result;
    assert.equal(byteContinuation.outcome.status, 'pending');
    assert.deepEqual(byteContinuation.outcome.stage, {
      step: 'registration-orphans',
      rowOrdinal: 0,
    });
    assert.deepEqual(
      byteContinuation.run.progress.stage,
      byteContinuation.outcome.stage,
    );
    assert.equal(byteContinuation.run.state, 'running');
    assert.equal(byteContinuation.pins, 1);
    assert.equal(
      byteContinuation.outcome.token.revision,
      exact.result.outcome.token.revision + 1,
    );
    assert.equal(
      byteContinuation.run.progress.revision,
      byteContinuation.outcome.token.revision,
    );
    byteEvidence.abandonment = await abandon(bytesProfile);
    byteEvidence.outcome = 'completed measured calls';
    evidence.fetchInvocations = invocations.size;
    evidence.runtimeLogs = server.getLogs();
    process.stdout.write(
      `fleet-control packed conditional workloads: ${JSON.stringify(evidence)}\n`,
    );
    return evidence;
  } catch (error) {
    process.stdout.write(
      `fleet-control packed conditional workload failure: ${JSON.stringify({ evidence, runtimeLogs: server.getLogs(), error: String(error) })}\n`,
    );
    throw error;
  } finally {
    await server.close();
  }
}
