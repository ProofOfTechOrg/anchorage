#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0

import { spawnSync } from 'node:child_process';
import { readFileSync, realpathSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  DEPLOYMENT_TAG_PATTERN,
  provisionDeploymentIdentityProtocol,
} from '#deployment-identity-protocol';

function usage(message) {
  const suffix = message ? `\n${message}` : '';
  return new Error(
    `Usage: flowsafe-provision --database <name-or-binding> --tag <tag> (--remote | --local | --preview) [--config <path>] [--persist-to <path>]${suffix}`,
  );
}

export function parseProvisioningArguments(argv) {
  const valueOptions = new Set([
    '--database',
    '--tag',
    '--config',
    '--persist-to',
  ]);
  const values = new Map();
  let target;
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (index === 0 && argument === '--') continue;
    if (['--remote', '--local', '--preview'].includes(argument)) {
      if (target) throw usage('choose exactly one execution target');
      target = argument;
      continue;
    }
    if (!valueOptions.has(argument))
      throw usage(`unknown option '${argument}'`);
    const value = argv[index + 1];
    if (!value || value.startsWith('--')) {
      throw usage(`option '${argument}' requires a value`);
    }
    if (values.has(argument)) throw usage(`option '${argument}' was repeated`);
    values.set(argument, value);
    index += 1;
  }
  const database = values.get('--database');
  const tag = values.get('--tag');
  if (!database || !tag || !target) throw usage();
  if (!DEPLOYMENT_TAG_PATTERN.test(tag)) {
    throw usage(`tag '${tag}' must match ${DEPLOYMENT_TAG_PATTERN}`);
  }
  const persistTo = values.get('--persist-to');
  if (persistTo && target !== '--local') {
    throw usage('--persist-to is valid only with --local');
  }
  return {
    database,
    tag,
    target,
    ...(values.has('--config') ? { config: values.get('--config') } : {}),
    ...(persistTo ? { persistTo } : {}),
  };
}

function resultRows(stdout) {
  let parsed;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw new Error('wrangler returned non-JSON output');
  }
  const envelopes = Array.isArray(parsed) ? parsed : [parsed];
  return envelopes.flatMap((entry) =>
    entry && Array.isArray(entry.results) ? entry.results : [],
  );
}

function wranglerEntrypoint() {
  const consumerRequire = createRequire(resolve(process.cwd(), 'package.json'));
  let packagePath;
  try {
    packagePath = consumerRequire.resolve('wrangler/package.json');
  } catch (cause) {
    throw new Error(
      'flowsafe-provision requires Wrangler 4 installed in the current project',
      { cause },
    );
  }

  let manifest;
  try {
    manifest = JSON.parse(readFileSync(packagePath, 'utf8'));
  } catch (cause) {
    throw new Error(
      'flowsafe-provision could not read the local Wrangler manifest',
      { cause },
    );
  }
  const version = manifest.version;
  if (typeof version !== 'string' || !/^4\./.test(version)) {
    throw new Error(
      `flowsafe-provision requires Wrangler major 4; found ${typeof version === 'string' ? version : 'an invalid version'}`,
    );
  }
  const relativeBin =
    typeof manifest.bin === 'string' ? manifest.bin : manifest.bin?.wrangler;
  if (typeof relativeBin !== 'string') {
    throw new Error(
      'flowsafe-provision found a Wrangler package without a bin',
    );
  }
  return resolve(dirname(packagePath), relativeBin);
}

function commandFailure(message, cause, result) {
  const error = new Error(message, { cause });
  error.stdout = typeof result.stdout === 'string' ? result.stdout : '';
  error.stderr = typeof result.stderr === 'string' ? result.stderr : '';
  return error;
}

function reportFailure(error) {
  if (error && typeof error === 'object') {
    if (typeof error.stdout === 'string' && error.stdout.length > 0) {
      process.stdout.write(error.stdout);
    }
    if (typeof error.stderr === 'string' && error.stderr.length > 0) {
      process.stderr.write(error.stderr);
    }
  }
  const messages = [];
  const seen = new Set();
  let current = error;
  while (current instanceof Error && !seen.has(current)) {
    seen.add(current);
    messages.push(current.message);
    current = current.cause;
  }
  if (messages.length === 0) messages.push(String(error));
  process.stderr.write(`${messages.join('\nCaused by: ')}\n`);
}

export function wranglerTargetArguments(target) {
  switch (target) {
    case '--local':
    case '--remote':
      return [target];
    case '--preview':
      return ['--remote', '--preview'];
    default:
      throw usage(`unknown execution target '${target}'`);
  }
}

function wranglerQuery(options, sql) {
  const args = [
    'd1',
    'execute',
    options.database,
    ...wranglerTargetArguments(options.target),
    '--yes',
    '--json',
    '--command',
    sql,
  ];
  if (options.config) args.push('--config', options.config);
  if (options.persistTo) args.push('--persist-to', options.persistTo);
  const result = spawnSync(process.execPath, [wranglerEntrypoint(), ...args], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  if (result.error) {
    throw commandFailure('failed to execute Wrangler 4', result.error, result);
  }
  if (result.status !== 0) {
    const outcome =
      result.signal === null
        ? `status ${result.status}`
        : `signal ${result.signal}`;
    throw commandFailure(
      'Wrangler failed while provisioning deployment identity',
      new Error(`Wrangler exited with ${outcome}`),
      result,
    );
  }
  if (result.stderr.length > 0) process.stderr.write(result.stderr);
  try {
    return resultRows(result.stdout);
  } catch (cause) {
    throw commandFailure(
      'Wrangler returned unusable JSON output',
      cause,
      result,
    );
  }
}

function sqlLiteral(value) {
  if (value === null) return 'NULL';
  if (typeof value === 'string') return `'${value.replaceAll("'", "''")}'`;
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  throw new Error(
    'deployment identity protocol produced an invalid SQL binding',
  );
}

function renderProtocolStatement(statement) {
  const segments = statement.sql.split('?');
  if (segments.length !== statement.bindings.length + 1) {
    throw new Error(
      'deployment identity protocol produced mismatched SQL bindings',
    );
  }
  return segments.reduce(
    (sql, segment, index) =>
      `${sql}${index === 0 ? '' : sqlLiteral(statement.bindings[index - 1])}${segment}`,
    '',
  );
}

export async function provisionDeploymentIdentity(options, execute) {
  const query = execute ?? ((sql) => wranglerQuery(options, sql));
  await provisionDeploymentIdentityProtocol(
    (statement) => query(renderProtocolStatement(statement)),
    options.tag,
    { caller: 'flowsafe-provision' },
  );
}

async function main() {
  const options = parseProvisioningArguments(process.argv.slice(2));
  await provisionDeploymentIdentity(options);
  process.stdout.write(
    `Deployment identity '${options.tag}' verified in ${options.database} (${options.target.slice(2)}).\n`,
  );
}

const isMain =
  process.argv[1] !== undefined &&
  realpathSync(process.argv[1]) ===
    realpathSync(fileURLToPath(import.meta.url));

if (isMain) {
  main().catch((error) => {
    reportFailure(error);
    process.exitCode = 1;
  });
}
