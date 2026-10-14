import { spawn, spawnSync } from 'node:child_process';
import {
  appendFileSync,
  closeSync,
  existsSync,
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
  trustedD1Checks,
} from './notification-staging-control.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const flowsafe = dirname(here);
const wrangler = join(flowsafe, '../../node_modules/.bin/wrangler');
const config = join(here, 'notification-staging.wrangler.jsonc');
const database = 'anchorage-flowsafe-remote-step-staging-db';
const versionPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

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
  function currentVersion() {
    const deployments = json([
      'deployments',
      'list',
      '--config',
      config,
      '--json',
    ]);
    assert(
      Array.isArray(deployments) && deployments.length > 0,
      'deployment list is empty',
    );
    const ordered = deployments
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
    const latest = ordered[0].deployment;
    assert(
      latest.versions?.length === 1 && latest.versions[0].percentage === 100,
      'deployment is not a single full version',
    );
    const id = latest.versions[0].version_id;
    assert(versionPattern.test(id), 'invalid deployment version');
    return id;
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

export async function relockIfOpen(probe, expected) {
  assert(
    expected?.mutationEpoch === 1 && expected?.transitionRevision === 1,
    'watchdog owned fence version is missing',
  );
  const current = await probe('GET', '/admin/execution-fence');
  if (current.state === 'migration-locked') return current;
  assert(current.state === 'open', 'watchdog found an unexpected fence state');
  assert(
    current.requireMutationEpoch === true &&
      current.mutationEpoch === expected.mutationEpoch &&
      current.transitionRevision === expected.transitionRevision,
    'watchdog fence metadata differs',
  );
  const locked = await probe('POST', '/admin/execution-fence', {
    expected: 'open',
    next: 'migration-locked',
    expectedMutationEpoch: current.mutationEpoch,
    expectedRevision: current.transitionRevision,
    advanceMutationEpoch: true,
  });
  assert(
    locked.state === 'migration-locked' &&
      locked.mutationEpoch === current.mutationEpoch + 1 &&
      locked.transitionRevision === current.transitionRevision + 1,
    'watchdog relock is unconfirmed',
  );
  return locked;
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
  return { port, expectedVersion, probeSecretPath, cfTokenPath, secret, token };
}

async function watchdog(stateFile) {
  const state = JSON.parse(privateFile(stateFile));
  const {
    port,
    expectedVersion,
    probeSecretPath,
    cfTokenPath,
    parentPid,
    deadlineAt,
    outdir,
  } = state;
  const probe = localProbe({ port, secret: readSecretFile(probeSecretPath) });
  const cli = createWranglerControl({ token: privateFile(cfTokenPath) });
  assert(
    cli.currentVersion() === expectedVersion &&
      cli.proofEnabled(expectedVersion),
    'watchdog enabled version differs',
  );
  writeFileSync(`${stateFile}.ready`, '', { mode: 0o600, flag: 'wx' });
  while (Date.now() < deadlineAt && !existsSync(`${stateFile}.done`)) {
    try {
      process.kill(parentPid, 0);
    } catch {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  if (existsSync(`${stateFile}.done`)) return;
  let failure;
  if (existsSync(`${stateFile}.unlock-attempted`)) {
    try {
      const owned = JSON.parse(privateFile(`${stateFile}.unlock-attempted`));
      await relockIfOpen(probe, owned);
    } catch (error) {
      failure = error;
    }
  }
  try {
    if (cli.currentVersion() === expectedVersion)
      cli.deployOffIfCurrent(expectedVersion, outdir);
  } catch (error) {
    failure = new AggregateError(
      failure ? [failure, error] : [error],
      'watchdog off deploy is unconfirmed',
    );
  }
  if (failure) throw failure;
}

async function controller() {
  const settings = settingsFromEnv(process.env);
  const cli = createWranglerControl({ token: settings.token });
  assert(
    cli.currentVersion() === settings.expectedVersion &&
      cli.proofEnabled(settings.expectedVersion),
    'enabled version preflight differs',
  );
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
    }),
    { mode: 0o600, flag: 'wx' },
  );
  const watcher = spawn(
    process.execPath,
    [fileURLToPath(import.meta.url), '--watchdog', stateFile],
    {
      detached: true,
      stdio: ['ignore', watcherLog, watcherLog],
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
  assert(existsSync(`${stateFile}.ready`), 'watchdog readiness is unconfirmed');
  const probe = localProbe({ port: settings.port, secret: settings.secret });
  let offConfirmed = false;
  const requestOffDeploy = async () => {
    if (offConfirmed) return;
    cli.deployOffIfCurrent(settings.expectedVersion, outdir);
    offConfirmed = true;
  };
  try {
    const result = await runNotificationStagingControl({
      probe,
      ...trustedD1Checks((sql) => cli.query(sql)),
      requestOffDeploy,
      onBeforeUnlock: async () => {
        writeFileSync(
          `${stateFile}.unlock-attempted`,
          JSON.stringify({
            mutationEpoch: 1,
            transitionRevision: 1,
          }),
          {
            mode: 0o600,
            flag: 'wx',
          },
        );
      },
    });
    assert(offConfirmed, 'off deploy was not confirmed');
    writeFileSync(`${stateFile}.done`, '', { mode: 0o600, flag: 'wx' });
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
