// SPDX-License-Identifier: Apache-2.0

// Records the golden baseline of `auditFleetDrift()` (src/fleet.ts, before its
// R4-B.2 decomposition into bounded stages): the exact findings array it
// returns AND the exact sequence of calls it makes onto its `store`,
// `backendFor`, `specFor`, and `maintenanceSecretFor` collaborators (the "op
// log"), for the hand-authored world in
// packages/fleet-control/test/fixtures/fleet-audit-world.ts.
//
// The baseline must be recorded from PRE-REWRITE code, so this script writes
// exactly one file — the generated literals — and never touches the world it
// drives. `--check` re-derives both values from the unchanged world and
// compares them STRUCTURALLY against the committed module's exports, so the
// compatibility gate never depends on formatter behavior; it writes nothing.
//
// This script is a re-recording aid, not a CI gate: the in-suite equivalence
// title in packages/fleet-control/test/fleet-audit-golden.test.ts is the
// automatic behavioral gate.
//
// Usage:
//   node scripts/record-audit-baseline.mjs            # write the baseline
//   node scripts/record-audit-baseline.mjs --check    # verify, exit 1 on drift

import { spawnSync } from 'node:child_process';
import { existsSync, writeFileSync } from 'node:fs';
import { register } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const REPOSITORY_ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const FIXTURE_DIRECTORY = join(
  REPOSITORY_ROOT,
  'packages',
  'fleet-control',
  'test',
  'fixtures',
);
const WORLD_MODULE = join(FIXTURE_DIRECTORY, 'fleet-audit-world.ts');
const BASELINE_FILE = join(FIXTURE_DIRECTORY, 'fleet-audit-baseline.ts');
const BASELINE_RELATIVE_PATH =
  'packages/fleet-control/test/fixtures/fleet-audit-baseline.ts';
// `pnpm exec` rather than a hard-coded node_modules/.bin path, matching
// record-drain-baseline.mjs and build-api-docs.mjs; the .bin shim location is
// a pnpm implementation detail.
const PNPM = process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm';

function usage(message) {
  process.stderr.write(
    `${message}\nusage: node scripts/record-audit-baseline.mjs [--check]\n`,
  );
  process.exit(2);
}

function parseArguments(argv) {
  let check = false;
  for (const argument of argv) {
    if (argument === '--check') check = true;
    else usage(`unknown argument '${argument}'`);
  }
  return { check };
}

// The fixture chain (and the audit function it drives) is TypeScript with
// parameter properties, which Node's default strip-only mode refuses, so the
// script re-executes itself once with full type transformation.
function reexecuteWithTypeTransform(argv) {
  const result = spawnSync(
    process.execPath,
    [
      '--experimental-transform-types',
      '--no-warnings',
      fileURLToPath(import.meta.url),
      ...argv,
    ],
    { stdio: 'inherit' },
  );
  if (result.error) throw result.error;
  process.exit(result.status ?? 1);
}

// Test sources import sibling modules with `.js` specifiers, which Node does
// not remap to the `.ts` files on disk.
function registerTypeScriptResolution() {
  const hook = `
    import { existsSync } from 'node:fs';
    import { fileURLToPath } from 'node:url';
    export async function resolve(specifier, context, next) {
      const relative = specifier.startsWith('.') || specifier.startsWith('/');
      if (relative && specifier.endsWith('.js')) {
        const target = new URL(specifier, context.parentURL);
        if (!existsSync(fileURLToPath(target))) {
          const candidate = new URL(\`\${target.href.slice(0, -3)}.ts\`);
          if (existsSync(fileURLToPath(candidate))) {
            return next(candidate.href, context);
          }
        }
      }
      return next(specifier, context);
    }
  `;
  register(`data:text/javascript,${encodeURIComponent(hook)}`);
}

function quoted(value) {
  const escaped = value
    .replaceAll('\\', '\\\\')
    .replaceAll("'", "\\'")
    .replaceAll('\n', '\\n')
    .replaceAll('\r', '\\r')
    .replaceAll('\t', '\\t');
  return `'${escaped}'`;
}

function primitive(value) {
  if (value === undefined) return 'undefined';
  if (value === null) return 'null';
  if (typeof value === 'string') return quoted(value);
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  throw new Error(`unsupported baseline value type '${typeof value}'`);
}

function isComposite(value) {
  return typeof value === 'object' && value !== null;
}

function propertyKey(key) {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/u.test(key) ? key : quoted(key);
}

// Emits readable TypeScript; `biome check --write` owns the final layout.
function render(value) {
  if (!isComposite(value)) return primitive(value);
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    return `[${value.map((item) => `${render(item)},`).join('\n')}]`;
  }
  const entries = Object.entries(value);
  if (entries.length === 0) return '{}';
  return `{${entries
    .map(([key, item]) => `${propertyKey(key)}: ${render(item)},`)
    .join('\n')}}`;
}

function baselineSource({ findings, ops }) {
  return `// SPDX-License-Identifier: Apache-2.0

/**
 * GENERATED FILE — DO NOT EDIT BY HAND.
 *
 * Written by \`scripts/record-audit-baseline.mjs\` from the hand-authored world
 * in \`fleet-audit-world.ts\`. It freezes the observable behavior of
 * \`auditFleetDrift()\` (src/fleet.ts) before it is decomposed into bounded
 * stages, so the decomposition can be proven byte-equivalent. Verify with
 * \`node scripts/record-audit-baseline.mjs --check\`; any required change to
 * these literals is a compatibility break, not a fixture update.
 */

import type { DriftFinding } from '../../src/fleet.js';
import type { AuditOpLogEntry } from './fleet-audit-world.js';

/** Every finding \`auditFleetDrift()\` returned, in order. */
export const AUDIT_BASELINE_FINDINGS = ${render(findings)} as const satisfies readonly DriftFinding[];

/**
 * Every \`withDeploymentLease\`/\`get\`/\`put\`/\`inspect\`/\`ensureMaintenance\`
 * call, every \`resolver:<kind>\` invocation, and every \`lease.assertOwned()\`
 * call \`auditFleetDrift()\` made, in order. \`list\`/\`renew\`/\`delete\` are in
 * \`AuditOpLogEntry\`'s vocabulary but never appear here (defensive, unused by
 * this pre-decomposition world).
 */
export const AUDIT_BASELINE_OPS = ${render(ops)} as const satisfies readonly AuditOpLogEntry[];
`;
}

function formatGeneratedFile() {
  const result = spawnSync(
    PNPM,
    ['exec', 'biome', 'check', '--write', BASELINE_FILE],
    { cwd: REPOSITORY_ROOT, stdio: ['ignore', 'ignore', 'inherit'] },
  );
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error('biome refused the generated baseline');
  }
}

function describeValue(value) {
  if (isComposite(value)) {
    return Array.isArray(value)
      ? `array(${value.length})`
      : `object{${Object.keys(value).join(',')}}`;
  }
  return primitive(value);
}

/**
 * Structural comparison: ordered arrays, ordered object keys, exact leaf
 * values. Formatting and quoting are deliberately outside the comparison.
 */
function structuralDifferences(committed, derived, path, differences) {
  if (isComposite(committed) !== isComposite(derived)) {
    differences.push(
      `${path}: committed ${describeValue(committed)} / derived ${describeValue(derived)}`,
    );
    return differences;
  }
  if (!isComposite(committed)) {
    if (committed !== derived) {
      differences.push(
        `${path}: committed ${primitive(committed)} / derived ${primitive(derived)}`,
      );
    }
    return differences;
  }
  if (Array.isArray(committed) !== Array.isArray(derived)) {
    differences.push(
      `${path}: committed ${describeValue(committed)} / derived ${describeValue(derived)}`,
    );
    return differences;
  }
  if (Array.isArray(committed)) {
    if (committed.length !== derived.length) {
      differences.push(
        `${path}: committed ${committed.length} item(s) / derived ${derived.length} item(s)`,
      );
    }
    const length = Math.max(committed.length, derived.length);
    for (let index = 0; index < length; index += 1) {
      const onlyDerived = index >= committed.length;
      if (onlyDerived || index >= derived.length) {
        const side = onlyDerived ? 'derived only' : 'committed only';
        const value = onlyDerived ? derived[index] : committed[index];
        differences.push(`${path}[${index}]: ${side} ${describeValue(value)}`);
        continue;
      }
      structuralDifferences(
        committed[index],
        derived[index],
        `${path}[${index}]`,
        differences,
      );
    }
    return differences;
  }
  const committedKeys = Object.keys(committed);
  const derivedKeys = Object.keys(derived);
  if (committedKeys.join(',') !== derivedKeys.join(',')) {
    differences.push(
      `${path}: committed keys [${committedKeys.join(', ')}] / derived keys [${derivedKeys.join(', ')}]`,
    );
  }
  for (const key of new Set([...committedKeys, ...derivedKeys])) {
    structuralDifferences(
      committed[key],
      derived[key],
      `${path}.${key}`,
      differences,
    );
  }
  return differences;
}

function summary(baseline) {
  const distinctKinds = new Set(
    baseline.findings.map((finding) => finding.kind),
  ).size;
  return (
    `${baseline.findings.length} findings (${distinctKinds} distinct kinds), ` +
    `${baseline.ops.length} ops`
  );
}

async function main(argv) {
  const { check } = parseArguments(argv);
  if (process.features.typescript !== 'transform') {
    reexecuteWithTypeTransform(argv);
  }
  registerTypeScriptResolution();
  const { runFleetAuditBaseline } = await import(WORLD_MODULE);
  const baseline = await runFleetAuditBaseline();

  if (!check) {
    writeFileSync(BASELINE_FILE, baselineSource(baseline));
    formatGeneratedFile();
    process.stdout.write(
      `wrote ${BASELINE_RELATIVE_PATH}: ${summary(baseline)}\n`,
    );
    return 0;
  }

  if (!existsSync(BASELINE_FILE)) {
    process.stderr.write(
      `audit baseline is missing: ${BASELINE_RELATIVE_PATH}\n` +
        'run `node scripts/record-audit-baseline.mjs` on the pre-rewrite tree\n',
    );
    return 1;
  }
  const committed = await import(BASELINE_FILE);
  const differences = [
    ...structuralDifferences(
      committed.AUDIT_BASELINE_FINDINGS,
      baseline.findings,
      'findings',
      [],
    ),
    ...structuralDifferences(
      committed.AUDIT_BASELINE_OPS,
      baseline.ops,
      'ops',
      [],
    ),
  ];
  if (differences.length === 0) {
    process.stdout.write(
      `audit baseline matches ${BASELINE_RELATIVE_PATH}: ${summary(baseline)}\n`,
    );
    return 0;
  }
  process.stderr.write(
    `audit baseline drifted from ${BASELINE_RELATIVE_PATH}\n` +
      `${differences.length} structural difference(s), committed vs re-derived from the unchanged world:\n` +
      `${differences.map((difference) => `  ${difference}`).join('\n')}\n`,
  );
  return 1;
}

const invokedPath = process.argv[1]
  ? pathToFileURL(resolve(process.argv[1])).href
  : undefined;
if (invokedPath === import.meta.url) {
  process.exit(await main(process.argv.slice(2)));
}
