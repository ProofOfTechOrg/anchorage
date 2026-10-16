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
import { dirname, join, resolve as resolvePath } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import {
  decodeExecutionFenceMutationMetadata,
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_TABLE,
} from '#deployment-identity-protocol';
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
const trackedConfigPath = join(here, 'notification-staging.wrangler.jsonc');
const trackedConfig = JSON.parse(readFileSync(trackedConfigPath, 'utf8'));
const trackedProbeConfigPath = join(
  here,
  'notification-staging-probe.wrangler.jsonc',
);
const trackedProbeConfig = JSON.parse(
  readFileSync(trackedProbeConfigPath, 'utf8'),
);
const versionPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const accountPattern = /^[0-9a-f]{32}$/;
const workerNamePattern = /^[a-z0-9-]+$/;

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

function stagingConfig(configPath) {
  const source =
    configPath === trackedConfigPath
      ? readFileSync(configPath, 'utf8')
      : privateFile(configPath);
  const selected = JSON.parse(source);
  const expected = structuredClone(trackedConfig);
  if (configPath !== trackedConfigPath) {
    assert(
      workerNamePattern.test(selected.name) &&
        selected.name !== trackedConfig.name &&
        /^[a-z0-9-]+$/.test(selected.d1_databases?.[0]?.database_name) &&
        selected.d1_databases[0].database_name !==
          trackedConfig.d1_databases[0].database_name &&
        versionPattern.test(selected.d1_databases[0].database_id) &&
        selected.d1_databases[0].database_id !==
          trackedConfig.d1_databases[0].database_id &&
        /^[a-z0-9]{1,32}$/.test(selected.vars?.DEPLOYMENT_TENANT) &&
        selected.vars.DEPLOYMENT_TENANT !==
          trackedConfig.vars.DEPLOYMENT_TENANT,
      'private staging config resource identity differs',
    );
    expected.name = selected.name;
    expected.main = resolvePath(here, trackedConfig.main);
    expected.alias = Object.fromEntries(
      Object.entries(trackedConfig.alias).map(([name, path]) => [
        name,
        resolvePath(here, path),
      ]),
    );
    expected.d1_databases[0].database_name =
      selected.d1_databases[0].database_name;
    expected.d1_databases[0].database_id = selected.d1_databases[0].database_id;
    expected.vars.DEPLOYMENT_TENANT = selected.vars.DEPLOYMENT_TENANT;
  }
  assert(
    isDeepStrictEqual(selected, expected),
    'private staging config differs from the off template',
  );
  return {
    source,
    name: selected.name,
    database: selected.d1_databases[0].database_name,
    databaseId: selected.d1_databases[0].database_id,
    tenant: selected.vars.DEPLOYMENT_TENANT,
  };
}

function verifyProbeConfig(configPath, workerName, fresh) {
  assert(
    fresh === (configPath !== trackedProbeConfigPath),
    'private probe config is required for the selected Worker',
  );
  const selected = JSON.parse(
    configPath === trackedProbeConfigPath
      ? readFileSync(configPath, 'utf8')
      : privateFile(configPath),
  );
  const expected = structuredClone(trackedProbeConfig);
  if (fresh) {
    expected.main = resolvePath(here, trackedProbeConfig.main);
    expected.services[0].service = workerName;
  }
  assert(
    isDeepStrictEqual(selected, expected),
    'private probe config differs from the selected Worker',
  );
}

export function createWranglerControl({
  command = spawnSync,
  token,
  configPath = trackedConfigPath,
}) {
  const selected = stagingConfig(configPath);
  function run(args, timeout = 30_000) {
    assert(
      stagingConfig(configPath).source === selected.source,
      'staging config changed since preflight',
    );
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
    const list = json([
      'deployments',
      'list',
      '--config',
      configPath,
      '--json',
    ]);
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
  function proofEnabled(version, expectedMutationEpoch) {
    assert(versionPattern.test(version), 'invalid version');
    const detail = json([
      'versions',
      'view',
      version,
      '--config',
      configPath,
      '--json',
    ]);
    const bindings = detail.resources?.bindings;
    assert(Array.isArray(bindings), 'version bindings are unreadable');
    const flag = bindings.filter(
      (binding) => binding.name === 'NOTIFICATION_PROOF_ENABLED',
    );
    const d1 = bindings.filter((binding) => binding.name === 'DB');
    const tenant = bindings.filter(
      (binding) => binding.name === 'DEPLOYMENT_TENANT',
    );
    assert(
      flag.length === 1 && flag[0].type === 'plain_text',
      'proof flag binding differs',
    );
    assert(
      d1.length === 1 &&
        d1[0].type === 'd1' &&
        d1[0].database_id === selected.databaseId,
      'enabled D1 binding differs',
    );
    assert(
      tenant.length === 1 &&
        tenant[0].type === 'plain_text' &&
        tenant[0].text === selected.tenant,
      'enabled deployment tenant differs',
    );
    if (expectedMutationEpoch !== undefined) {
      assert(
        expectedMutationEpoch === 1 || expectedMutationEpoch === 2,
        'invalid enabled epoch',
      );
      const epoch = bindings.filter(
        (binding) => binding.name === 'STAGING_MUTATION_EPOCH',
      );
      assert(
        epoch.length === 1 &&
          epoch[0].type === 'plain_text' &&
          epoch[0].text === String(expectedMutationEpoch),
        'enabled epoch binding differs',
      );
    }
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
        selected.database,
        '--remote',
        '--yes',
        '--json',
        '--config',
        configPath,
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
    prepareOffBundle(directory) {
      assert(directory.startsWith('/'), 'absolute off bundle path required');
      mkdirSync(directory, { mode: 0o700 });
      run(
        ['deploy', '--dry-run', '--config', configPath, '--outdir', directory],
        180_000,
      );
      const bundlePath = join(directory, 'notification-staging.worker.js');
      assert(statSync(bundlePath).isFile(), 'off bundle is missing');
      run(
        [
          'deploy',
          bundlePath,
          '--dry-run',
          '--config',
          configPath,
          '--outdir',
          join(directory, 'bundle-rehearsal'),
        ],
        180_000,
      );
      return bundlePath;
    },
    deployOffIfCurrent(expectedVersion, outdir, bundlePath) {
      assert(versionPattern.test(expectedVersion), 'invalid expected version');
      assert(
        typeof bundlePath === 'string' &&
          bundlePath.startsWith('/') &&
          statSync(bundlePath).isFile(),
        'pinned off bundle is required',
      );
      assert(
        currentVersion() === expectedVersion,
        'enabled deployment changed',
      );
      assert(
        proofEnabled(expectedVersion),
        'expected deployment is not enabled',
      );
      run(
        ['deploy', bundlePath, '--config', configPath, '--outdir', outdir],
        180_000,
      );
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
  const configPath = env.FLOWSAFE_STAGING_CONFIG_FILE ?? trackedConfigPath;
  const selected = stagingConfig(configPath);
  verifyProbeConfig(
    env.FLOWSAFE_STAGING_PROBE_CONFIG_FILE ?? trackedProbeConfigPath,
    selected.name,
    configPath !== trackedConfigPath,
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
    configPath,
    selected,
  };
}

function acquireDeploymentLock(accountId, workerName) {
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
    configPath,
    workerName,
    bundlePath,
  } = state;
  assert(
    baselineEpoch === 0 || baselineEpoch === 1,
    'invalid watchdog baseline',
  );
  const secret = readSecretFile(probeSecretPath);
  const token = privateFile(cfTokenPath);
  const probe = localProbe({ port, secret });
  const selected = stagingConfig(configPath);
  assert(selected.name === workerName, 'watchdog Worker identity differs');
  assert(
    bundlePath ===
      join(
        dirname(stateFile),
        'pinned-off-bundle',
        'notification-staging.worker.js',
      ) && statSync(bundlePath).isFile(),
    'watchdog pinned off bundle is missing',
  );
  const cli = createWranglerControl({
    token,
    configPath,
  });
  assert(
    cli.currentVersion() === expectedVersion &&
      cli.proofEnabled(expectedVersion, baselineEpoch + 1),
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
  let sealFailure;
  try {
    await sealFence(probe, baselineEpoch);
  } catch (error) {
    sealFailure = error;
  }
  cli.deployOffIfCurrent(expectedVersion, outdir, bundlePath);
  try {
    const rows = cli.query(`SELECT * FROM ${EXECUTION_FENCE_TABLE}`);
    assert(
      rows.length === 1 && rows[0].id === EXECUTION_FENCE_ROW_ID,
      'authoritative fence row differs',
    );
    const row = rows[0];
    const metadata = decodeExecutionFenceMutationMetadata(row);
    assert(
      row.proof_key === null &&
        row.proof_run_id === null &&
        metadata.proofTablePrefix === null &&
        metadata.proofWorkflowId === null &&
        metadata.proofStartToken === null,
      'authoritative fence proof differs',
    );
    confirmedFence({ state: row.state, ...metadata }, baselineEpoch);
  } catch (error) {
    const failure = new AggregateError(
      sealFailure ? [sealFailure, error] : [error],
      'staging fence relock is unconfirmed',
    );
    throw new Error(privateDiagnostic(failure, [secret, token]));
  }
  writeFileSync(`${stateFile}.off-confirmed`, '', { mode: 0o600, flag: 'wx' });
}

async function controller() {
  const settings = settingsFromEnv(process.env);
  const lockFd = acquireDeploymentLock(
    settings.accountId,
    settings.selected.name,
  );
  const directory = join(
    tmpdir(),
    `flowsafe-notification-control-${process.pid}`,
  );
  mkdirSync(directory, { mode: 0o700 });
  const configPath =
    settings.configPath === trackedConfigPath
      ? trackedConfigPath
      : join(directory, 'off.wrangler.jsonc');
  if (configPath !== trackedConfigPath)
    writeFileSync(configPath, settings.selected.source, {
      mode: 0o600,
      flag: 'wx',
    });
  const stateFile = join(directory, 'watchdog.json');
  const outdir = join(directory, 'off-deploy');
  let stage = 'enabled-version-preflight';
  try {
    const cli = createWranglerControl({ token: settings.token, configPath });
    const baselineEpoch = settings.priorOffVersion === undefined ? 0 : 1;
    assert(
      cli.currentVersion() === settings.expectedVersion &&
        cli.proofEnabled(settings.expectedVersion, baselineEpoch + 1),
      'enabled version preflight differs',
    );
    const probe = localProbe({ port: settings.port, secret: settings.secret });
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
    stage = 'off-bundle-preparation';
    const bundlePath = cli.prepareOffBundle(
      join(directory, 'pinned-off-bundle'),
    );
    stage = 'watchdog-start';
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
        configPath,
        workerName: settings.selected.name,
        bundlePath,
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
    stage = 'proof';
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
        const identity = await probe('GET', '/identity');
        assert(
          identity.versionId === settings.expectedVersion &&
            identity.deploymentTenant === settings.selected.tenant,
          'authenticated probe subject differs from enabled Worker',
        );
      },
    });
    assert(offConfirmed, 'off deploy was not confirmed');
    return result;
  } catch (error) {
    appendFileSync(
      join(directory, 'watchdog.log'),
      `controller ${stage}: ${privateDiagnostic(error, [settings.secret, settings.token])}\n`,
      { mode: 0o600 },
    );
    throw new Error(
      `staging controller failed at ${stage}; inspect private watchdog log`,
    );
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
      process.stderr.write(`${error.message}\n`);
      process.exitCode = 1;
    },
  );
}
