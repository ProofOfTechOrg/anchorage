// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  deriveDirectConformanceNames,
  validateDirectConformanceConfig,
} from '../scripts/direct-credentialed-conformance-config.mjs';
import { isDeploymentScriptName } from '../src/deployment-context.js';
import { validateDeploymentSpec } from '../src/validation.js';
import { buildPlainWorkerSpec } from './fixtures/plain-worker-harnesses.js';

const EXAMPLE = new URL(
  '../scripts/direct-credentialed-conformance.example.json',
  import.meta.url,
);
const NOW = Date.parse('2026-09-09T12:00:00.000Z');

function input(): Record<string, unknown> {
  return JSON.parse(readFileSync(EXAMPLE, 'utf8')) as Record<string, unknown>;
}

function objectAt(value: Record<string, unknown>, path: readonly string[]) {
  let target = value;
  for (const part of path) target = target[part] as Record<string, unknown>;
  return target;
}

function changed(path: readonly string[], value: unknown) {
  const result = input();
  const key = path.at(-1);
  if (!key) throw new Error('test path needs a key');
  objectAt(result, path.slice(0, -1))[key] = value;
  return result;
}

function validate(value: unknown) {
  return validateDirectConformanceConfig(value, { now: NOW });
}

describe('direct conformance configuration', () => {
  it('validates the nonsecret example and keeps copied intent immutable', () => {
    const raw = input();
    const result = validate(raw);
    expect(result.referenceWorker.requestTimeoutMs).toBe(30_000);
    expect(result.referenceWorker.invocationTimeoutMs).toBe(600_000);
    expect(result.referenceWorker.subrequestLimit).toBe(10_000);
    expect(result.deployment.subrequestLimit).toBe(50);
    for (const value of [
      result,
      result.referenceWorker,
      result.referenceWorker.artifact,
      result.referenceWorker.compatibilityFlags,
      result.deployment,
      result.deployment.spec,
      result.deployment.artifact,
    ])
      expect(Object.isFrozen(value)).toBe(true);
    objectAt(raw, ['referenceWorker']).cpuLimitMs = 1;
    expect(result.referenceWorker.cpuLimitMs).toBe(30_000);
  });

  it('derives separate fixed-role resources that pass production spec validation', () => {
    const config = validate(input());
    const names = deriveDirectConformanceNames(config);
    expect(names.referenceWorker).toBe(`${config.resourcePrefix}-reference`);
    expect(names.exportBucket).toBe(`${config.resourcePrefix}-exports`);
    expect(Object.keys(names.roles)).toEqual(['a', 'b', 'recovery']);
    const scripts = new Set<string>();
    for (const [role, values] of Object.entries(names.roles)) {
      expect(Object.isFrozen(values)).toBe(true);
      expect(isDeploymentScriptName(values.scriptName)).toBe(true);
      expect(values.routeHostname).toBe(
        `${config.resourcePrefix}-${role}.${config.ownedHostname}`,
      );
      expect(() =>
        validateDeploymentSpec(
          buildPlainWorkerSpec({
            ...values,
            environment: config.environment,
          }),
        ),
      ).not.toThrow();
      scripts.add(values.scriptName);
    }
    expect(scripts.size).toBe(3);
    expect(Object.isFrozen(names.roles)).toBe(true);
  });

  it.each([
    [],
    ['referenceWorker'],
    ['referenceWorker', 'artifact'],
    ['deployment'],
    ['deployment', 'artifact'],
    ['deployment', 'spec'],
  ])('rejects unknown and missing keys at %j', (...path) => {
    const extra = input();
    objectAt(extra, path).unexpected = 'secret-sentinel';
    expect(() => validate(extra)).toThrow(/direct conformance config/);
    const missing = input();
    const target = objectAt(missing, path);
    const key = Object.keys(target)[0];
    if (!key) throw new Error('test object needs a key');
    delete target[key];
    expect(() => validate(missing)).toThrow(/direct conformance config/);
  });

  it.each([
    'dispatchNamespace',
    'hostRoutingKvId',
    'platformProfile',
    'sharedOutboundWorkerName',
    'maintenanceCapabilityPublicKey',
    'apiToken',
    'headers',
    'databaseId',
  ])('rejects %s without echoing supplied secret data', (key) => {
    const raw = input();
    raw[key] = 'secret-sentinel';
    let caught: unknown;
    try {
      validate(raw);
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(String(caught)).not.toContain('secret-sentinel');
  });

  it('rejects accessors before invoking them, symbols, classes and sparse arrays', () => {
    let reads = 0;
    const getter = input();
    Object.defineProperty(getter, 'environment', {
      enumerable: true,
      get() {
        reads += 1;
        return 'conformance';
      },
    });
    expect(() => validate(getter)).toThrow();
    expect(reads).toBe(0);
    const symbol = input();
    Object.defineProperty(symbol, Symbol('hidden'), { value: 1 });
    expect(() => validate(symbol)).toThrow();
    class Config {}
    expect(() => validate(Object.assign(new Config(), input()))).toThrow();
    expect(() =>
      validate(changed(['referenceWorker', 'compatibilityFlags'], Array(1))),
    ).toThrow();
  });

  it.each([
    [['contractVersion'], 2],
    [['disposableAccount'], 'true'],
    [['resourcePrefix'], 'fc-short'],
    [['resourcePrefix'], 'fc0123456789ABCDEF01234567'],
    [['environment'], '../production'],
    [['interruption'], 'before-admission'],
    [['deployment', 'spec', 'fixtureVersion'], 2],
    [['referenceWorker', 'artifact', 'mainModule'], 'direct-run-manifest.js'],
    [['deployment', 'artifact', 'mainModule'], '../worker.js'],
    [['deployment', 'artifact', 'mainModule'], 'CON.js'],
    [['deployment', 'artifact', 'sha256'], 'A'.repeat(64)],
    [['deployment', 'artifact', 'bundle'], ''],
  ] as const)('rejects invalid %j', (path, value) => {
    expect(() => validate(changed(path, value))).toThrow();
  });

  it.each([
    'https://example.test',
    'EXAMPLE.test',
    'example.test.',
    '*.example.test',
    'user@example.test',
    'example.test:443',
    'example.test/path',
    'example.test?query',
    'example.test#hash',
    '127.0.0.1',
    '[::1]',
    'bad..example',
    `${'a'.repeat(64)}.test`,
    `${'a'.repeat(63)}.${'b'.repeat(63)}.${'c'.repeat(63)}.${'d'.repeat(40)}`,
  ])('rejects noncanonical or overlong derived hostname %s', (host) => {
    expect(() => validate(changed(['ownedHostname'], host))).toThrow();
  });

  it('accepts parsed native JSON semantics, null prototypes and local paths with spaces', () => {
    const raw = Object.assign(Object.create(null), input());
    objectAt(raw, ['deployment', 'artifact']).bundle =
      './my artifacts/worker.mjs';
    raw.disposableAccount = false;
    const result = validate(raw);
    expect(result.disposableAccount).toBe(false);
    expect(result.deployment.artifact.bundle).toBe('./my artifacts/worker.mjs');
  });

  it.each([
    'referenceWorker',
    'deployment',
  ])('checks %s dates and compatibility flags', (target) => {
    for (const date of ['2026-08-03', '2026-09-31', '2026-09-10', '2026-8-06'])
      expect(() =>
        validate(changed([target, 'compatibilityDate'], date)),
      ).toThrow();
    for (const flags of [['unknown'], ['nodejs_compat', 'nodejs_compat'], null])
      expect(() =>
        validate(changed([target, 'compatibilityFlags'], flags)),
      ).toThrow();
    expect(
      validate(changed([target, 'compatibilityFlags'], ['nodejs_compat']))[
        target as 'referenceWorker' | 'deployment'
      ].compatibilityFlags,
    ).toEqual(['nodejs_compat']);
    const leap = changed([target, 'compatibilityDate'], '2028-02-29');
    expect(() =>
      validateDirectConformanceConfig(leap, {
        now: Date.parse('2028-03-01T00:00:00.000Z'),
      }),
    ).not.toThrow();
  });

  it.each([
    ['referenceWorker', 'cpuLimitMs', 300_000],
    ['deployment', 'cpuLimitMs', 300_000],
    ['referenceWorker', 'subrequestLimit', 10_000_000],
    ['deployment', 'subrequestLimit', 10_000_000],
    ['referenceWorker', 'requestTimeoutMs', 2_147_483_647],
    ['referenceWorker', 'invocationTimeoutMs', 2_147_483_647],
    ['referenceWorker', 'maxInvocations', Number.MAX_SAFE_INTEGER],
  ] as const)('checks %s.%s integer bounds', (target, key, maximum) => {
    for (const value of [0, -1, 1.5, NaN, Infinity, null, '50', maximum + 1])
      expect(() => validate(changed([target, key], value))).toThrow();
    expect(() => validate(changed([target, key], 1))).not.toThrow();
    expect(() => validate(changed([target, key], maximum))).not.toThrow();
  });

  it('uses the production inventory request-budget domain and a finite clock', () => {
    for (const value of [0, 8, 1001, 9.5])
      expect(() =>
        validate(changed(['referenceWorker', 'maxProviderRequests'], value)),
      ).toThrow();
    for (const value of [9, 1000])
      expect(() =>
        validate(changed(['referenceWorker', 'maxProviderRequests'], value)),
      ).not.toThrow();
    expect(() =>
      validateDirectConformanceConfig(input(), { now: NaN }),
    ).toThrow(/clock/);
  });

  it('runs as a native Node configuration module without constructing a provider', () => {
    const module = new URL(
      '../scripts/direct-credentialed-conformance-config.mjs',
      import.meta.url,
    );
    const code = `import {readFileSync} from 'node:fs'; import {validateDirectConformanceConfig} from ${JSON.stringify(module.href)}; const value=validateDirectConformanceConfig(JSON.parse(readFileSync(${JSON.stringify(fileURLToPath(EXAMPLE))},'utf8')),{now:${NOW}}); process.stdout.write(String(value.contractVersion));`;
    expect(
      execFileSync(process.execPath, ['--input-type=module', '-e', code], {
        encoding: 'utf8',
      }),
    ).toBe('1');
  });
});
