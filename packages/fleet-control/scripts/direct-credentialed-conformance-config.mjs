// SPDX-License-Identifier: Apache-2.0

import {
  DEPLOYMENT_TAG_PATTERN,
  isDeploymentEnvironment,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import { isPortablePathSegment } from '../src/export-file-name.ts';
import { cloneBoundedPlainData } from '../src/strict-plain-data.ts';

export const DIRECT_CONFORMANCE_CONTRACT_VERSION = 1;

const PREFIX = /^fc[a-f0-9]{24}$/u;
const DIGEST = /^[a-f0-9]{64}$/u;
const DATE = /^\d{4}-\d{2}-\d{2}$/u;
const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u;
const RUNTIME_KEYS = [
  'artifact',
  'compatibilityDate',
  'compatibilityFlags',
  'cpuLimitMs',
  'subrequestLimit',
];

function invalid(field) {
  return new Error(`direct conformance config has invalid ${field}`);
}

function object(value, keys, field) {
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Object.keys(value).length !== keys.length ||
    keys.some((key) => !Object.hasOwn(value, key))
  )
    throw invalid(field);
  return value;
}

function string(value, field) {
  if (typeof value !== 'string' || value.length === 0 || value.includes('\0'))
    throw invalid(field);
  return value;
}

function integer(value, maximum, field) {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum)
    throw invalid(field);
  return value;
}

function hostname(value, field) {
  const host = string(value, field);
  const labels = host.split('.');
  if (
    host.length > 253 ||
    labels.length < 2 ||
    labels.some((label) => !DNS_LABEL.test(label)) ||
    !/[a-z]/u.test(host)
  )
    throw invalid(field);
  let url;
  try {
    url = new URL(`https://${host}`);
  } catch {
    throw invalid(field);
  }
  if (url.hostname !== host) throw invalid(field);
  return host;
}

function artifact(value, field) {
  const keys = ['bundle', 'mainModule', 'sha256'];
  if (
    value &&
    typeof value === 'object' &&
    Object.hasOwn(value, 'auxiliaryWasm')
  )
    keys.push('auxiliaryWasm');
  const input = object(value, keys, field);
  const bundle = string(input.bundle, `${field}.bundle`);
  const mainModule = string(input.mainModule, `${field}.mainModule`);
  if (
    mainModule.length > 255 ||
    !isPortablePathSegment(mainModule) ||
    !/\.m?js$/u.test(mainModule)
  )
    throw invalid(`${field}.mainModule`);
  if (typeof input.sha256 !== 'string' || !DIGEST.test(input.sha256))
    throw invalid(`${field}.sha256`);
  let auxiliaryWasm;
  if (Object.hasOwn(input, 'auxiliaryWasm')) {
    if (!Array.isArray(input.auxiliaryWasm))
      throw invalid(`${field}.auxiliaryWasm`);
    const names = new Set([mainModule]);
    auxiliaryWasm = Object.freeze(
      input.auxiliaryWasm.map((value) => {
        const descriptor = object(
          value,
          ['file', 'name', 'sha256'],
          `${field}.auxiliaryWasm`,
        );
        const file = string(descriptor.file, `${field}.auxiliaryWasm.file`);
        const name = string(descriptor.name, `${field}.auxiliaryWasm.name`);
        if (
          name.length > 255 ||
          !isPortablePathSegment(name) ||
          !name.endsWith('.wasm') ||
          names.has(name)
        )
          throw invalid(`${field}.auxiliaryWasm.name`);
        if (
          typeof descriptor.sha256 !== 'string' ||
          !DIGEST.test(descriptor.sha256)
        )
          throw invalid(`${field}.auxiliaryWasm.sha256`);
        names.add(name);
        return Object.freeze({ file, name, sha256: descriptor.sha256 });
      }),
    );
  }
  return Object.freeze({
    bundle,
    mainModule,
    sha256: input.sha256,
    ...(auxiliaryWasm === undefined ? {} : { auxiliaryWasm }),
  });
}

function runtime(input, field, today) {
  const date = string(input.compatibilityDate, `${field}.compatibilityDate`);
  let actualDate;
  try {
    actualDate = new Date(`${date}T00:00:00.000Z`).toISOString().slice(0, 10);
  } catch {
    throw invalid(`${field}.compatibilityDate`);
  }
  if (
    !DATE.test(date) ||
    actualDate !== date ||
    date < '2026-08-04' ||
    date > today
  )
    throw invalid(`${field}.compatibilityDate`);
  const flags = input.compatibilityFlags;
  if (
    !Array.isArray(flags) ||
    !(
      flags.length === 0 ||
      (flags.length === 1 &&
        (flags[0] === 'nodejs_compat' ||
          flags[0] === 'global_fetch_strictly_public')) ||
      (flags.length === 2 &&
        flags[0] === 'nodejs_compat' &&
        flags[1] === 'global_fetch_strictly_public')
    )
  )
    throw invalid(`${field}.compatibilityFlags`);
  return {
    artifact: artifact(input.artifact, `${field}.artifact`),
    compatibilityDate: date,
    compatibilityFlags: Object.freeze([...flags]),
    cpuLimitMs: integer(input.cpuLimitMs, 300_000, `${field}.cpuLimitMs`),
    subrequestLimit: integer(
      input.subrequestLimit,
      10_000_000,
      `${field}.subrequestLimit`,
    ),
  };
}

export function deriveDirectConformanceNames(config) {
  const prefix = string(config.resourcePrefix, 'resourcePrefix');
  if (!PREFIX.test(prefix)) throw invalid('resourcePrefix');
  const ownedHostname = hostname(config.ownedHostname, 'ownedHostname');
  const roles = {};
  for (const [role, suffix] of [
    ['a', 'a'],
    ['b', 'b'],
    ['recovery', 'r'],
  ]) {
    const tenantTag = `${prefix}${suffix}`;
    if (!DEPLOYMENT_TAG_PATTERN.test(tenantTag))
      throw invalid('derived tenant tag');
    const name = `${prefix}-tenant-${role}`;
    roles[role] = Object.freeze({
      tenantTag,
      scriptName: name,
      databaseName: name,
      routeHostname: hostname(`${name}.${ownedHostname}`, 'derived hostname'),
    });
  }
  return Object.freeze({
    referenceWorker: `${prefix}-reference`,
    fleetDatabase: `${prefix}-fleet`,
    quotaDatabase: `${prefix}-quota`,
    exportBucket: `${prefix}-exports`,
    referenceHostname: hostname(
      `${prefix}-reference.${ownedHostname}`,
      'derived reference hostname',
    ),
    roles: Object.freeze(roles),
  });
}

export function validateDirectConformanceConfig(value, options = {}) {
  const input = object(
    cloneBoundedPlainData(value, {
      maxDepth: 8,
      maxNodes: 512,
      maxScalarBytes: 256 * 1024,
      maxSerializedBytes: 256 * 1024,
      error: () => invalid('plain JSON data'),
    }),
    [
      'contractVersion',
      'disposableAccount',
      'resourcePrefix',
      'environment',
      'ownedHostname',
      'referenceWorker',
      'deployment',
      'interruption',
    ],
    'root',
  );
  if (input.contractVersion !== DIRECT_CONFORMANCE_CONTRACT_VERSION)
    throw invalid('contractVersion');
  if (typeof input.disposableAccount !== 'boolean')
    throw invalid('disposableAccount');
  const environment = string(input.environment, 'environment');
  if (!isDeploymentEnvironment(environment)) throw invalid('environment');
  if (input.interruption !== 'after-migration-admission')
    throw invalid('interruption');
  let today;
  try {
    today = new Date(options.now ?? Date.now()).toISOString().slice(0, 10);
  } catch {
    throw invalid('clock');
  }
  const reference = object(
    input.referenceWorker,
    [
      ...RUNTIME_KEYS,
      'requestTimeoutMs',
      'invocationTimeoutMs',
      'maxProviderRequests',
      'maxInvocations',
    ],
    'referenceWorker',
  );
  const referenceRuntime = runtime(reference, 'referenceWorker', today);
  if (
    !referenceRuntime.compatibilityFlags.includes(
      'global_fetch_strictly_public',
    )
  )
    throw invalid('referenceWorker.compatibilityFlags');
  if (referenceRuntime.artifact.mainModule === 'direct-run-manifest.js')
    throw invalid('referenceWorker.artifact.mainModule');
  const maxProviderRequests = integer(
    reference.maxProviderRequests,
    1_000,
    'referenceWorker.maxProviderRequests',
  );
  if (maxProviderRequests < 9)
    throw invalid('referenceWorker.maxProviderRequests');
  const deployment = object(
    input.deployment,
    [...RUNTIME_KEYS, 'spec'],
    'deployment',
  );
  const spec = object(deployment.spec, ['fixtureVersion'], 'deployment.spec');
  if (spec.fixtureVersion !== 1)
    throw invalid('deployment.spec.fixtureVersion');
  const result = Object.freeze({
    contractVersion: DIRECT_CONFORMANCE_CONTRACT_VERSION,
    disposableAccount: input.disposableAccount,
    resourcePrefix: string(input.resourcePrefix, 'resourcePrefix'),
    environment,
    ownedHostname: hostname(input.ownedHostname, 'ownedHostname'),
    referenceWorker: Object.freeze({
      ...referenceRuntime,
      requestTimeoutMs: integer(
        reference.requestTimeoutMs,
        2_147_483_647,
        'referenceWorker.requestTimeoutMs',
      ),
      invocationTimeoutMs: integer(
        reference.invocationTimeoutMs,
        2_147_483_647,
        'referenceWorker.invocationTimeoutMs',
      ),
      maxProviderRequests,
      maxInvocations: integer(
        reference.maxInvocations,
        Number.MAX_SAFE_INTEGER,
        'referenceWorker.maxInvocations',
      ),
    }),
    deployment: Object.freeze({
      ...runtime(deployment, 'deployment', today),
      spec: Object.freeze({ fixtureVersion: 1 }),
    }),
    interruption: input.interruption,
  });
  deriveDirectConformanceNames(result);
  return result;
}
