import { spawn, spawnSync } from 'node:child_process';
import {
  appendFileSync,
  closeSync,
  constants,
  existsSync,
  fstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  localProbe,
  readSecretFile,
  runNotificationStagingControl,
  sealedRetryFence,
  trustedD1Checks,
} from './notification-staging-control.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const flowsafe = dirname(here);
const wrangler = join(flowsafe, '../../node_modules/.bin/wrangler');
const config = join(here, 'notification-staging.wrangler.jsonc');
const database = 'anchorage-flowsafe-remote-step-staging-db';
const versionPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const accountPattern = /^[0-9a-f]{32}$/;
const workerName = JSON.parse(readFileSync(config, 'utf8')).name;
assert(/^[a-z0-9-]+$/.test(workerName), 'invalid staging Worker name');

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function privateDiagnostic(error, secrets = []) {
  const parts = [];
  const visit = (value) => {
    if (!(value instanceof Error)) return;
    parts.push(value.message);
    if (value instanceof AggregateError) value.errors.forEach(visit);
    if (value.cause) visit(value.cause);
  };
  visit(error);
  return parts
    .join(' | ')
    .replaceAll(secrets[0] ?? '\u0000', '[redacted]')
    .replaceAll(secrets[1] ?? '\u0000', '[redacted]')
    .slice(0, 4_096);
}

function privateFile(path) {
  assert(
    typeof path === 'string' && path.startsWith('/'),
    'absolute private path required',
  );
  const info = statSync(path);
  assert(
    info.isFile() && (info.mode & 0o077) === 0,
    'private file permissions required',
  );
  return readFileSync(path, 'utf8').trim();
}

export function createWranglerControl({ command = spawnSync, token }) {
  function run(args, timeout = 30_000) {
    const result = command(wrangler, args, {
      cwd: flowsafe,
      encoding: 'utf8',
      timeout,
      env: {
        ...process.env,
        CLOUDFLARE_API_TOKEN: token,
        NO_COLOR: '1',
        WRANGLER_SEND_METRICS: 'false',
      },
    });
    if (result.status !== 0) {
      const details = [
        `exit=${result.status ?? 'none'}`,
        `signal=${result.signal ?? 'none'}`,
        result.error?.message,
        result.stderr?.slice(-2_048),
      ]
        .filter(Boolean)
        .join(' | ');
      throw new Error(`Wrangler ${args[0]} ${args[1]} failed`, {
        cause: new Error(details),
      });
    }
    return result.stdout;
  }
  function json(args) {
    const value = JSON.parse(run(args));
    assert(value !== null, 'Wrangler returned null');
    return value;
  }
  function deployments() {
    const list = json(['deployments', 'list', '--config', config, '--json']);
    assert(Array.isArray(list) && list.length > 0, 'deployment list is empty');
    const ordered = list
      .map((deployment) => {
        const createdAt = Date.parse(deployment.created_on);
        assert(
          Number.isFinite(createdAt),
          'deployment timestamp is unreadable',
        );
        return { deployment, createdAt };
      })
      .sort((a, b) => b.createdAt - a.createdAt);
    assert(
      ordered.length === 1 || ordered[0].createdAt > ordered[1].createdAt,
      'latest deployment is ambiguous',
    );
    return ordered.map(({ deployment }) => deployment);
  }
  function fullVersion(deployment) {
    assert(
      deployment.versions?.length === 1 &&
        deployment.versions[0].percentage === 100,
      'deployment is not a single full version',
    );
    const id = deployment.versions[0].version_id;
    assert(versionPattern.test(id), 'invalid deployment version');
    return id;
  }
  function currentVersion() {
    return fullVersion(deployments()[0]);
  }
  function proofEnabled(version) {
    assert(versionPattern.test(version), 'invalid version');
    const detail = json([
      'versions',
      'view',
      version,
      '--config',
      config,
      '--json',
    ]);
    const bindings = detail.resources?.bindings;
    assert(Array.isArray(bindings), 'version bindings are unreadable');
    const flag = bindings.filter(
      (binding) => binding.name === 'NOTIFICATION_PROOF_ENABLED',
    );
    assert(
      flag.length === 1 && flag[0].type === 'plain_text',
      'proof flag binding differs',
    );
    return flag[0].text === 'true';
  }
  return {
    currentVersion,
    proofEnabled,
    priorOffVersion(expectedEnabled) {
      const ordered = deployments();
      assert(
        ordered.length > 1 && fullVersion(ordered[0]) === expectedEnabled,
        'retry deployment history differs',
      );
      const prior = fullVersion(ordered[1]);
      assert(!proofEnabled(prior), 'prior deployment is not off');
      return prior;
    },
    query(sql) {
      const output = json([
        'd1',
        'execute',
        database,
        '--remote',
        '--yes',
        '--json',
        '--config',
        config,
        '--command',
        sql,
      ]);
      assert(
        Array.isArray(output) &&
          output.length === 1 &&
          output[0].success === true &&
          Array.isArray(output[0].results),
        'D1 read failed',
      );
      return output[0].results;
    },
    deployOffIfCurrent(expectedVersion, outdir) {
      assert(versionPattern.test(expectedVersion), 'invalid expected version');
      assert(
        currentVersion() === expectedVersion,
        'enabled deployment changed',
      );
      assert(
        proofEnabled(expectedVersion),
        'expected deployment is not enabled',
      );
      const tracked = JSON.parse(readFileSync(config, 'utf8'));
      assert(
        tracked.vars?.NOTIFICATION_PROOF_ENABLED === 'false' &&
          tracked.workers_dev === false,
        'tracked off config differs',
      );
      run(['deploy', '--config', config, '--outdir', outdir], 180_000);
      const next = currentVersion();
      assert(
        next !== expectedVersion && proofEnabled(next) === false,
        'off deploy is unconfirmed',
      );
      return next;
    },
  };
}

export async function sealFence(probe, baselineEpoch = 0) {
  assert(
    baselineEpoch === 0 || baselineEpoch === 1,
    'invalid watchdog baseline',
  );
  for (let attempt = 0; attempt < 3; attempt++) {
    const current = await probe('GET', '/admin/execution-fence');
    assert(
      current.state === 'migration-locked' || current.state === 'open',
      'watchdog found an unexpected fence state',
    );
    const initial =
      current.state === 'migration-locked' &&
      current.mutationEpoch === baselineEpoch &&
      current.transitionRevision === baselineEpoch &&
      current.requireMutationEpoch === (baselineEpoch !== 0);
    const open =
      current.state === 'open' &&
      current.mutationEpoch === baselineEpoch + 1 &&
      current.transitionRevision === baselineEpoch + 1 &&
      current.requireMutationEpoch === true;
    if (current.state === 'migration-locked' && !initial) {
      confirmedFence(current, baselineEpoch);
      return current;
    }
    assert(initial || open, 'watchdog fence metadata differs');
    try {
      await probe('POST', '/admin/execution-fence', {
        expected: current.state,
        next: 'migration-locked',
        expectedMutationEpoch: current.mutationEpoch,
        expectedRevision: current.transitionRevision,
        advanceMutationEpoch: true,
      });
    } catch {
      // The authoritative read resolves a lost response or a competing open CAS.
    }
  }
  const current = await probe('GET', '/admin/execution-fence');
  confirmedFence(current, baselineEpoch);
  return current;
}

function settingsFromEnv(env) {
  const port = Number(env.FLOWSAFE_STAGING_PROBE_PORT);
  const expectedVersion = env.FLOWSAFE_STAGING_ENABLED_VERSION;
  assert(
    Number.isInteger(port) && port > 0 && port < 65536,
    'invalid local probe port',
  );
  assert(
    versionPattern.test(expectedVersion),
    'expected enabled version required',
  );
  const probeSecretPath = env.FLOWSAFE_STAGING_PROBE_SECRET_FILE;
  const cfTokenPath = env.FLOWSAFE_CF_TOKEN_FILE;
  const secret = readSecretFile(probeSecretPath);
  const token = privateFile(cfTokenPath);
  assert(token.length > 0, 'Cloudflare token is empty');
  const accountId = env.FLOWSAFE_CF_ACCOUNT_ID;
  assert(accountPattern.test(accountId), 'Cloudflare account ID is required');
  const priorOffVersion = env.FLOWSAFE_STAGING_RETRY_OFF_VERSION;
  assert(
    priorOffVersion === undefined || versionPattern.test(priorOffVersion),
    'invalid prior off version',
  );
  return {
    port,
    expectedVersion,
    probeSecretPath,
    cfTokenPath,
    secret,
    token,
    accountId,
    priorOffVersion,
  };
}

function acquireDeploymentLock(accountId) {
  const directory = join('/tmp', `flowsafe-deploy-locks-${process.getuid()}`);
  mkdirSync(directory, { mode: 0o700, recursive: true });
  const directoryInfo = statSync(directory);
  assert(
    directoryInfo.isDirectory() &&
      directoryInfo.uid === process.getuid() &&
      (directoryInfo.mode & 0o077) === 0,
    'deployment lock directory is not private',
  );
  const path = join(directory, `${accountId}-${workerName}.lock`);
  const fd = openSync(
    path,
    constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW,
    0o600,
  );
  const info = fstatSync(fd);
  assert(
    info.isFile() && info.uid === process.getuid() && (info.mode & 0o077) === 0,
    'deployment lock file is not private',
  );
  const result = spawnSync('flock', ['--nonblock', '--exclusive', '3'], {
    stdio: ['ignore', 'pipe', 'pipe', fd],
  });
  assert(result.status === 0, 'deployment lock is unavailable');
  return fd;
}

function confirmedFence(current, baselineEpoch = 0) {
  assert(
    current.state === 'migration-locked',
    'staging fence relock is unconfirmed',
  );
  const sealed =
    current.mutationEpoch === baselineEpoch + 1 &&
    current.transitionRevision === baselineEpoch + 1 &&
    current.requireMutationEpoch === true;
  const relocked =
    current.mutationEpoch === baselineEpoch + 2 &&
    current.transitionRevision === baselineEpoch + 2 &&
    current.requireMutationEpoch === true;
  assert(sealed || relocked, 'staging fence metadata differs');
}

async function watchdog(stateFile) {
  const lockFd = 3;
  assert(fstatSync(lockFd).isFile(), 'inherited deployment lock is missing');
  const state = JSON.parse(privateFile(stateFile));
  const {
    port,
    expectedVersion,
    probeSecretPath,
    cfTokenPath,
    parentPid,
    deadlineAt,
    outdir,
    baselineEpoch,
  } = state;
  assert(
    baselineEpoch === 0 || baselineEpoch === 1,
    'invalid watchdog baseline',
  );
  const probe = localProbe({ port, secret: readSecretFile(probeSecretPath) });
  const cli = createWranglerControl({ token: privateFile(cfTokenPath) });
  assert(
    cli.currentVersion() === expectedVersion &&
      cli.proofEnabled(expectedVersion),
    'watchdog enabled version differs',
  );
  writeFileSync(`${stateFile}.ready`, '', { mode: 0o600, flag: 'wx' });
  while (Date.now() < deadlineAt && !existsSync(`${stateFile}.off-requested`)) {
    try {
      process.kill(parentPid, 0);
    } catch {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  await sealFence(probe, baselineEpoch);
  cli.deployOffIfCurrent(expectedVersion, outdir);
  confirmedFence(await probe('GET', '/admin/execution-fence'), baselineEpoch);
  writeFileSync(`${stateFile}.off-confirmed`, '', { mode: 0o600, flag: 'wx' });
}

async function controller() {
  const settings = settingsFromEnv(process.env);
  const lockFd = acquireDeploymentLock(settings.accountId);
  const cli = createWranglerControl({ token: settings.token });
  assert(
    cli.currentVersion() === settings.expectedVersion &&
      cli.proofEnabled(settings.expectedVersion),
    'enabled version preflight differs',
  );
  const probe = localProbe({ port: settings.port, secret: settings.secret });
  const baselineEpoch = settings.priorOffVersion === undefined ? 0 : 1;
  if (baselineEpoch === 1) {
    assert(
      settings.priorOffVersion !== settings.expectedVersion &&
        cli.priorOffVersion(settings.expectedVersion) ===
          settings.priorOffVersion,
      'retry prior off version is unconfirmed',
    );
    const fence = await probe('GET', '/admin/execution-fence');
    assert(
      Object.entries(sealedRetryFence).every(
        ([key, value]) => fence[key] === value,
      ) &&
        fence.proofKey === undefined &&
        fence.proofRunId === undefined,
      'retry fence is not the exact sealed baseline',
    );
  }
  const directory = join(
    tmpdir(),
    `flowsafe-notification-control-${process.pid}`,
  );
  mkdirSync(directory, { mode: 0o700 });
  const stateFile = join(directory, 'watchdog.json');
  const outdir = join(directory, 'off-deploy');
  const watcherLog = openSync(join(directory, 'watchdog.log'), 'wx', 0o600);
  writeFileSync(
    stateFile,
    JSON.stringify({
      port: settings.port,
      expectedVersion: settings.expectedVersion,
      probeSecretPath: settings.probeSecretPath,
      cfTokenPath: settings.cfTokenPath,
      parentPid: process.pid,
      deadlineAt: Date.now() + 900_000,
      outdir,
      baselineEpoch,
    }),
    { mode: 0o600, flag: 'wx' },
  );
  const watcher = spawn(
    process.execPath,
    [fileURLToPath(import.meta.url), '--watchdog', stateFile],
    {
      detached: true,
      stdio: ['ignore', watcherLog, watcherLog, lockFd],
      env: process.env,
    },
  );
  closeSync(watcherLog);
  watcher.unref();
  assert(Number.isInteger(watcher.pid), 'watchdog did not start');
  const readyDeadline = Date.now() + 20_000;
  while (!existsSync(`${stateFile}.ready`) && Date.now() < readyDeadline) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert(
    existsSync(`${stateFile}.ready`) &&
      watcher.exitCode === null &&
      watcher.signalCode === null,
    'watchdog readiness is unconfirmed',
  );
  process.kill(watcher.pid, 0);
  let offConfirmed = false;
  const requestOffDeploy = async () => {
    if (offConfirmed) return;
    writeFileSync(`${stateFile}.off-requested`, '', {
      mode: 0o600,
      flag: 'wx',
    });
    while (!existsSync(`${stateFile}.off-confirmed`)) {
      assert(
        watcher.exitCode === null && watcher.signalCode === null,
        'watchdog exited before off confirmation',
      );
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    offConfirmed = true;
  };
  try {
    const result = await runNotificationStagingControl({
      probe,
      ...trustedD1Checks((sql) => cli.query(sql)),
      expectedInitialFence: baselineEpoch === 1 ? sealedRetryFence : undefined,
      requestOffDeploy,
      onBeforeUnlock: async () => {
        assert(
          watcher.exitCode === null && watcher.signalCode === null,
          'watchdog exited before fence unlock',
        );
        process.kill(watcher.pid, 0);
      },
    });
    assert(offConfirmed, 'off deploy was not confirmed');
    return result;
  } catch (error) {
    appendFileSync(
      join(directory, 'watchdog.log'),
      `controller: ${privateDiagnostic(error, [settings.secret, settings.token])}\n`,
    );
    throw new Error('staging controller failed; inspect private watchdog log');
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const action = process.argv[2];
  const execute =
    action === '--watchdog'
      ? watchdog(process.argv[3])
      : action === '--run' && process.argv.length === 3
        ? controller()
        : Promise.reject(new Error('usage: --run'));
  execute.then(
    (result) => {
      if (result) process.stdout.write(`${JSON.stringify(result)}\n`);
    },
    (error) => {
      process.stderr.write(`${privateDiagnostic(error)}\n`);
      process.exitCode = 1;
    },
  );
}
