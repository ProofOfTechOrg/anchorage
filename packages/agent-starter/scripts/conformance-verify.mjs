// SPDX-License-Identifier: Apache-2.0

/**
 * Drive the Workers for Platforms artifact contract against real workerd, in
 * the gate's own order, so a shape or ordering defect is found here rather than
 * on a paid namespace.
 *
 * The topology mirrors the fleet one: `wrangler dev` runs the candidate as the
 * primary Worker with the trusted state and a `StateEgress` stand-in beside it,
 * so every Durable Object call really crosses a script boundary. The v1-to-v2
 * release update is a restart with the other state configuration; the state
 * script name is stable across both, exactly as fleet control keeps it.
 *
 * FOUR THINGS THIS CANNOT PROVE — only `pnpm fleet-control:credentialed`
 * against a paid namespace can:
 *   1. `cpu-over-limit`: workerd locally does not enforce `limits.cpu_ms`.
 *   2. Platform-layer egress denial: there is no local outbound-Worker
 *      interception for the candidate's own fetch, so the denied status here is
 *      the upstream's, not the platform's.
 *   3. Durable Object namespace retention and `keep_bindings` secret
 *      preservation across a same-name upload.
 *   4. Decommission refusal on a non-empty bucket, export integrity, and the
 *      zero-residual sweep.
 *
 * Port: CONFORMANCE_VERIFY_PORT (default 8821) for wrangler and the next port
 * for the upstream stub. Every harness sharing
 * scripts/workerd-server-lifecycle.mjs takes a distinct default so two can run
 * at once: spike:verify 8799, durability-benchmark 8801, spike:verify:llm 8811.
 */

import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  DEPLOYMENT_SENTINEL_DDL,
  DEPLOYMENT_SENTINEL_TABLE,
} from '@proofoftech/flowsafe/deployment-identity-protocol';

import {
  createWorkerdServerLifecycle,
  parsePort,
} from '../../../scripts/workerd-server-lifecycle.mjs';

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const WRANGLER = join(packageRoot, 'node_modules/.bin/wrangler');
const CONFIG_DIR = join(packageRoot, 'conformance');
const PORT = parsePort(
  process.env.CONFORMANCE_VERIFY_PORT ?? 8821,
  'CONFORMANCE_VERIFY_PORT',
);
const UPSTREAM_PORT = PORT + 1;
const BASE = `http://127.0.0.1:${PORT}`;
const CONTRACT = JSON.parse(
  readFileSync(join(packageRoot, 'src/conformance/contract.json'), 'utf8'),
);
const ACTIONS_URL = `${BASE}${CONTRACT.httpPath}`;
const SOCKET_URL = `ws://127.0.0.1:${PORT}${CONTRACT.webSocketPath}`;
/** `localhost` is allowlisted by the harness outbound stub; `127.0.0.1` is not. */
const ALLOWED_UPSTREAM = `http://localhost:${UPSTREAM_PORT}/probe`;
const DENIED_UPSTREAM = `http://127.0.0.1:${UPSTREAM_PORT}/probe`;
const DENIED_STATUS = 403;
const CONTRACT_VERSION = CONTRACT.contractVersion;
// The contract encodes every action as POST. Only these operations are reads,
// so only these are safe to replay when Miniflare loses the response channel.
const REPLAY_SAFE_ACTIONS = new Set([
  'application-bindings',
  'connector-egress-allowed',
  'connector-egress-denied',
  'cpu-control',
  'flowsafe-status',
  'r2-absent',
  'r2-read',
  'state-egress-allowed',
  'state-egress-denied',
  'state-marker-get',
]);

/**
 * LOCAL HARNESS FIXTURES, and they must equal what the wrangler configurations
 * carry. `--var` cannot enforce that here: in multi-worker dev wrangler spreads
 * those flags into the PRIMARY configuration only, so the trusted state and
 * outbound workers would keep the files' values while the candidate took these
 * — one divergence traded for another. `scripts/conformance-config-check.test.mjs`
 * asserts the equality across all four files instead. Never real credentials.
 */
const DEPLOYMENT_TENANT = 'tenanta';
const APPLICATION_SECRET = 'conformance-local-application-secret';

let currentStep = 'startup';
let upstream;
let temporaryDirectory;
const lifecycle = createWorkerdServerLifecycle({ port: PORT });

function assert(condition, label, detail) {
  if (condition) return;
  const suffix =
    detail === undefined ? '' : `\n  got: ${JSON.stringify(detail)}`;
  throw new Error(`${label}${suffix}`);
}

/** The gate's own discipline: extra or missing top-level fields both fail. */
function assertExactKeys(value, expected, label) {
  const actual = Object.keys(value ?? {}).sort();
  assert(
    JSON.stringify(actual) === JSON.stringify([...expected].sort()),
    `${label} fields are not the v1 contract`,
    actual,
  );
}

async function step(label, run) {
  currentStep = label;
  const startedAt = Date.now();
  process.stdout.write(`\n> ${label}\n`);
  const result = await run();
  process.stdout.write(
    `  ok (${((Date.now() - startedAt) / 1000).toFixed(1)}s)\n`,
  );
  return result;
}

async function action(name, input = {}, expectedStatus = 200) {
  const { status, body } = await lifecycle.requestJson(
    (recoverySignal) =>
      fetch(ACTIONS_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          contractVersion: CONTRACT_VERSION,
          action: name,
          ...input,
        }),
        redirect: 'manual',
        ...(recoverySignal === undefined ? {} : { signal: recoverySignal }),
      }),
    {
      requestLabel: `conformance ${name}`,
      replaySafe: REPLAY_SAFE_ACTIONS.has(name),
    },
  );
  assert(
    status === expectedStatus,
    `${name} returned ${status}, expected ${expectedStatus}`,
    body,
  );
  assert(
    body.contractVersion === CONTRACT_VERSION && body.action === name,
    `${name} returned another contract version or action`,
    body,
  );
  return body;
}

function socketFrame(name, input = {}) {
  const nonce = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(SOCKET_URL);
    const timer = setTimeout(() => {
      socket.close();
      reject(new Error(`${name} WebSocket timed out`));
    }, 15_000);
    socket.addEventListener('open', () => {
      socket.send(
        JSON.stringify({
          contractVersion: CONTRACT_VERSION,
          action: name,
          ...input,
          nonce,
        }),
      );
    });
    socket.addEventListener('message', (event) => {
      clearTimeout(timer);
      socket.close();
      try {
        const frame = JSON.parse(String(event.data));
        assert(
          frame.contractVersion === CONTRACT_VERSION &&
            frame.action === name &&
            frame.nonce === nonce,
          `${name} WebSocket returned another contract, action, or nonce`,
          frame,
        );
        resolve(frame);
      } catch (error) {
        reject(error);
      }
    });
    socket.addEventListener('error', () => {
      clearTimeout(timer);
      reject(new Error(`${name} WebSocket failed`));
    });
    socket.addEventListener('close', (event) => {
      clearTimeout(timer);
      if (event.code !== 1000 && event.code !== 1005) {
        reject(
          new Error(`${name} WebSocket closed ${event.code}: ${event.reason}`),
        );
      }
    });
  });
}

function startUpstream() {
  return new Promise((resolve) => {
    const server = createServer((request, response) => {
      // Stand in for the platform's own denial on the candidate's direct fetch,
      // which has no local interception point. Host-based so the decision is
      // the same shape the outbound Worker makes.
      const host = (request.headers.host ?? '').split(':')[0];
      const allowed = host === 'localhost';
      response.writeHead(allowed ? 200 : DENIED_STATUS, {
        'content-type': 'text/plain',
      });
      response.end(allowed ? 'allowed' : 'denied');
    });
    server.listen(UPSTREAM_PORT, '127.0.0.1', () => resolve(server));
  });
}

/**
 * Fleet control seeds the deployment sentinel in `provisionDeployment` before
 * any Worker is uploaded; locally nothing does, so every Durable Object entry
 * would refuse with "the database carries no deployment sentinel". Seeding it
 * here reproduces provisioning step two rather than relaxing the check.
 */
function seedDeploymentSentinel() {
  const sql = join(temporaryDirectory, 'deployment-sentinel.sql');
  writeFileSync(
    sql,
    [
      `${DEPLOYMENT_SENTINEL_DDL};`,
      `INSERT OR IGNORE INTO ${DEPLOYMENT_SENTINEL_TABLE} (id, tenant_tag, provisioned_at) VALUES (1, '${DEPLOYMENT_TENANT}', '${new Date().toISOString()}');`,
    ].join('\n'),
  );
  const result = spawnSync(
    WRANGLER,
    [
      'd1',
      'execute',
      'DB',
      '--local',
      '--persist-to',
      temporaryDirectory,
      '--config',
      join(CONFIG_DIR, 'wrangler.state-v1.jsonc'),
      '--file',
      sql,
      '--yes',
    ],
    { cwd: packageRoot, stdio: 'pipe', encoding: 'utf8' },
  );
  if (result.status !== 0) {
    throw new Error(
      `seeding the deployment sentinel failed: ${result.stderr || result.stdout}`,
    );
  }
}

function launchWrangler(candidateConfig, stateConfig) {
  return () =>
    spawn(
      WRANGLER,
      [
        'dev',
        '--port',
        String(PORT),
        '--ip',
        '127.0.0.1',
        '--persist-to',
        temporaryDirectory,
        '--config',
        join(CONFIG_DIR, candidateConfig),
        '--config',
        join(CONFIG_DIR, stateConfig),
        '--config',
        join(CONFIG_DIR, 'wrangler.harness-outbound.jsonc'),
      ],
      {
        cwd: packageRoot,
        // Its own process group: the stop protocol SIGKILLs the group so an
        // orphaned workerd cannot keep serving the port.
        detached: true,
        stdio: ['ignore', 'inherit', 'inherit'],
        env: {
          ...process.env,
          CLOUDFLARE_API_TOKEN: '',
          CLOUDFLARE_ACCOUNT_ID: '',
        },
      },
    );
}

function toHex(bytes) {
  return Array.from(new Uint8Array(bytes), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

function hexBytes(count) {
  return toHex(crypto.getRandomValues(new Uint8Array(count)));
}

async function hmacHex(secret, message) {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(message),
  );
  return toHex(signature);
}

async function probeApplicationBindings() {
  const nonce = hexBytes(32);
  const result = await action('application-bindings', { nonce });
  assertExactKeys(
    result,
    [
      'action',
      'contractVersion',
      'secretHmacSha256',
      'secretName',
      'secretPlaintextExposed',
      'variableName',
      'variableValue',
    ],
    'application-bindings response',
  );
  assert(
    result.variableName === CONTRACT.applicationVariableName &&
      result.variableValue === CONTRACT.applicationVariableValue &&
      result.secretName === CONTRACT.applicationSecretBinding &&
      result.secretPlaintextExposed === false,
    'application binding probe did not return the exact variable and secret name',
    result,
  );
  assert(
    result.secretHmacSha256 === (await hmacHex(APPLICATION_SECRET, nonce)),
    'application secret HMAC challenge failed',
  );
}

async function probeEgress() {
  for (const [name, url, field] of [
    ['connector-egress-allowed', ALLOWED_UPSTREAM, 'allowed'],
    ['connector-egress-denied', DENIED_UPSTREAM, 'denied'],
    ['state-egress-allowed', ALLOWED_UPSTREAM, 'allowed'],
    ['state-egress-denied', DENIED_UPSTREAM, 'denied'],
  ]) {
    const result = await action(name, { url });
    assertExactKeys(
      result,
      ['action', 'contractVersion', field, 'upstreamStatus'],
      `${name} response`,
    );
    assert(result[field] === true, `${name} did not report ${field}`, result);
    if (field === 'denied') {
      assert(
        result.upstreamStatus === DENIED_STATUS,
        `${name} returned the wrong denial status`,
        result,
      );
    } else {
      assert(
        Number.isSafeInteger(result.upstreamStatus) &&
          result.upstreamStatus >= 200 &&
          result.upstreamStatus < 400,
        `${name} did not reach the allowed upstream`,
        result,
      );
    }
  }
}

async function probeR2() {
  const key = `conformance/${crypto.randomUUID()}`;
  const value = hexBytes(32);
  const written = await action('r2-write', { key, value });
  assertExactKeys(
    written,
    ['action', 'contractVersion', 'key', 'written'],
    'r2-write response',
  );
  assert(
    written.key === key && written.written === true,
    'R2 write failed',
    written,
  );
  const read = await action('r2-read', { key });
  assertExactKeys(
    read,
    ['action', 'contractVersion', 'key', 'value'],
    'r2-read response',
  );
  assert(
    read.key === key && read.value === value,
    'R2 read was not exact',
    read,
  );
  const deleted = await action('r2-delete', { key });
  assertExactKeys(
    deleted,
    ['action', 'contractVersion', 'deleted', 'key'],
    'r2-delete response',
  );
  assert(deleted.deleted === true, 'R2 delete failed', deleted);
  const absent = await action('r2-absent', { key });
  assertExactKeys(
    absent,
    ['absent', 'action', 'contractVersion', 'key'],
    'r2-absent response',
  );
  assert(
    absent.absent === true,
    'R2 object remains after candidate deletion',
    absent,
  );
}

async function probeAudit() {
  const nonce = crypto.randomUUID();
  const result = await action('audit-proxy', { nonce });
  assertExactKeys(
    result,
    ['accepted', 'action', 'contractVersion', 'nonce'],
    'audit-proxy response',
  );
  assert(
    result.accepted === true && result.nonce === nonce,
    'audit proxy did not accept the attributed event',
    result,
  );
}

async function main() {
  temporaryDirectory = mkdtempSync(join(tmpdir(), 'anchorage-conformance-'));
  await step('preflight and deployment sentinel', async () => {
    await lifecycle.preflight();
    seedDeploymentSentinel();
    upstream = await startUpstream();
  });

  const marker = `conformance:${crypto.randomUUID()}`;
  let started;

  await step('start release v1 (candidate + trusted state v1 + outbound)', () =>
    lifecycle.start('v1', () => ({
      child: launchWrangler(
        'wrangler.candidate.jsonc',
        'wrangler.state-v1.jsonc',
      )(),
    })),
  );

  await step('state marker put and get', async () => {
    for (const name of ['state-marker-put', 'state-marker-get']) {
      const result = await action(name, { marker });
      assertExactKeys(
        result,
        ['action', 'contractVersion', 'marker'],
        `${name} response`,
      );
      assert(
        result.marker === marker,
        `${name} returned another marker`,
        result,
      );
    }
  });

  await step('application bindings', probeApplicationBindings);
  await step('audit proxy', probeAudit);
  await step('connector and state egress', probeEgress);

  await step('WebSocket nonce echo', async () => {
    const frame = await socketFrame('nonce-echo');
    assertExactKeys(
      frame,
      ['action', 'contractVersion', 'nonce'],
      'nonce-echo frame',
    );
  });

  await step('CPU control', async () => {
    const result = await action('cpu-control');
    assertExactKeys(
      result,
      ['action', 'completed', 'contractVersion'],
      'cpu-control response',
    );
    assert(result.completed === true, 'CPU control request did not complete');
  });

  await step('application R2 lifecycle', () => probeR2());

  await step('FlowSafe run suspends before its effect', async () => {
    const effectNonce = crypto.randomUUID();
    started = await action('flowsafe-start', { effectNonce });
    assertExactKeys(
      started,
      [
        'action',
        'approvalId',
        'contractVersion',
        'effectCount',
        'revision',
        'runId',
        'status',
      ],
      'flowsafe-start response',
    );
    assert(
      started.status === 'pending' &&
        started.effectCount === 0 &&
        Number.isSafeInteger(started.revision),
      'FlowSafe run did not suspend before its effect',
      started,
    );
  });

  await step('WebSocket approval update on v1', async () => {
    const frame = await socketFrame('flowsafe-approval-update', {
      runId: started.runId,
      approvalId: started.approvalId,
      revision: started.revision,
    });
    assertExactKeys(
      frame,
      [
        'action',
        'approvalId',
        'contractVersion',
        'nonce',
        'revision',
        'runId',
        'status',
      ],
      'flowsafe-approval-update frame',
    );
    assert(
      frame.runId === started.runId &&
        frame.approvalId === started.approvalId &&
        frame.revision === started.revision &&
        frame.status === 'pending',
      'FlowSafe WebSocket did not deliver the suspended approval update',
      frame,
    );
  });

  await step('release update: restart on trusted state v2', async () => {
    await lifecycle.stop();
    // Release two is the same candidate bytes with the one added Durable
    // Object binding, which is exactly how the gate derives it.
    await lifecycle.start('v2', () => ({
      child: launchWrangler(
        'wrangler.candidate-v2.jsonc',
        'wrangler.state-v2.jsonc',
      )(),
    }));
  });

  await step('the v1 state marker survived the release update', async () => {
    const result = await action('state-marker-get', { marker });
    assertExactKeys(
      result,
      ['action', 'contractVersion', 'marker'],
      'state-marker-get response',
    );
    assert(
      result.marker === marker,
      'the state marker did not survive the release update',
      result,
    );
  });

  await step('the class the v2 migration added is reachable', async () => {
    const nonce = crypto.randomUUID();
    const result = await action('state-new-class', { nonce });
    assertExactKeys(
      result,
      ['action', 'contractVersion', 'nonce', 'stored'],
      'state-new-class response',
    );
    assert(
      result.nonce === nonce && result.stored === true,
      'new Durable Object class did not persist its probe',
      result,
    );
  });

  await step('application bindings after the update', probeApplicationBindings);

  await step('approval resumes exactly one effect', async () => {
    const approved = await action('flowsafe-approve', {
      runId: started.runId,
      approvalId: started.approvalId,
      revision: started.revision,
    });
    assertExactKeys(
      approved,
      [
        'action',
        'approvalId',
        'contractVersion',
        'effectCount',
        'resumed',
        'runId',
        'status',
      ],
      'flowsafe-approve response',
    );
    assert(
      approved.runId === started.runId &&
        approved.approvalId === started.approvalId &&
        approved.status === 'approved' &&
        approved.resumed === true &&
        approved.effectCount === 1,
      'FlowSafe approval did not resume exactly one effect',
      approved,
    );
  });

  await step('terminal D1 state', async () => {
    const terminal = await action('flowsafe-status', { runId: started.runId });
    assertExactKeys(
      terminal,
      ['action', 'contractVersion', 'effectCount', 'runId', 'terminalD1'],
      'flowsafe-status response',
    );
    assert(
      terminal.runId === started.runId &&
        terminal.terminalD1 === true &&
        terminal.effectCount === 1,
      'FlowSafe terminal D1 state or exactly-once effect is absent',
      terminal,
    );
  });

  await step('both replays are rejected after exactly one effect', async () => {
    for (const name of ['flowsafe-replay-decision', 'flowsafe-replay-resume']) {
      const replay = await action(
        name,
        {
          runId: started.runId,
          approvalId: started.approvalId,
          revision: started.revision,
        },
        409,
      );
      assertExactKeys(
        replay,
        ['action', 'contractVersion', 'effectCount', 'rejected', 'runId'],
        `${name} response`,
      );
      assert(
        replay.runId === started.runId &&
          replay.rejected === true &&
          replay.effectCount === 1,
        `${name} was not rejected after exactly one effect`,
        replay,
      );
    }
  });

  // The gate runs its R2 lifecycle a second time after the release update,
  // when the decommission fixture is written and then cleared, so the
  // post-update path gets the same exact-key assertions as the first pass.
  await step('application R2 lifecycle after the release update', () =>
    probeR2(),
  );
}

async function cleanup() {
  // The lifecycle owns stopping workerd and proving the port refuses; the
  // upstream stub and the state directory are this harness's to release.
  await lifecycle.cleanup(async () => {
    await new Promise((resolve) => {
      if (upstream) upstream.close(() => resolve(undefined));
      else resolve(undefined);
    });
    if (temporaryDirectory) {
      rmSync(temporaryDirectory, { recursive: true, force: true });
    }
  });
}

/**
 * Cleanup must never mask the failure that caused it. `lifecycle.cleanup`
 * rethrows a stop failure, which matters on its own — an unreleased port breaks
 * the next run — but only after the original diagnosis has been printed.
 */
async function cleanupReporting() {
  try {
    await cleanup();
    return true;
  } catch (error) {
    console.error(`cleanup failed: ${error?.stack ?? error}`);
    return false;
  }
}

// An interrupted harness must still stop workerd; otherwise the next run finds
// the port held by an orphan rather than a clean failure.
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    void cleanupReporting().then(() => process.exit(1));
  });
}

try {
  await main();
  console.log(
    [
      '\nconformance artifacts verified locally.',
      'NOT proven here (only pnpm fleet-control:credentialed can):',
      '  - cpu-over-limit termination by the platform',
      '  - platform-layer connector egress denial',
      '  - Durable Object namespace retention and keep_bindings secret preservation',
      '  - decommission refusal on a non-empty bucket, export integrity, zero residuals',
    ].join('\n'),
  );
  if (!(await cleanupReporting())) process.exitCode = 1;
} catch (error) {
  console.error(`\nFAILED at: ${currentStep}\n${error?.stack ?? error}`);
  // Let wrangler flush the Worker's own structured error before the group kill
  // takes it away; that log is usually the actual diagnosis.
  await new Promise((resolve) => setTimeout(resolve, 2_000));
  await cleanupReporting();
  process.exitCode = 1;
}
