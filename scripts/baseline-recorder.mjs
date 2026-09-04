// SPDX-License-Identifier: Apache-2.0

// The write/`--check` machinery every golden-baseline recorder shares.
//
// A golden baseline freezes the observable behavior of one function — the value
// it returns and the exact sequence of calls it makes onto its collaborators —
// as TypeScript literals recorded from a hand-authored deterministic world, so
// a later rewrite of that function can be proven behavior-equivalent. The
// mechanics are identical for every such baseline: parse `--check`, re-execute
// under Node's type transform, import the world, run it, and then either render
// the literals into exactly one generated file or compare the committed file
// against a fresh re-derivation. Only the DOMAIN differs, and each recorder
// supplies its domain as the config object documented on `runBaselineRecorder`.
//
// `--check` compares STRUCTURALLY — ordered arrays, ordered object keys, exact
// leaf values — so the compatibility gate never depends on formatter behavior,
// prints every difference, and exits non-zero without writing.
//
// This module is machinery, not a gate: each recorder's in-suite equivalence
// title is the automatic behavioral gate, and `--check` is the re-recording aid
// an author runs by hand.

import { spawnSync } from 'node:child_process';
import { existsSync, writeFileSync } from 'node:fs';
import { register } from 'node:module';
import { dirname, join, relative, resolve } from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const REPOSITORY_ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
// `pnpm exec` rather than a hard-coded node_modules/.bin path, matching
// build-api-docs.mjs; the .bin shim location is a pnpm implementation detail.
const PNPM = process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm';

// Every path a recorder names is repository-relative, resolved here against the
// one `REPOSITORY_ROOT` this module already computes for itself. A recorder
// therefore carries no path machinery, and — because the string it configures
// is BOTH what gets resolved and what gets printed — no message can ever name a
// file the run did not touch.
function repositoryPath(relativePath) {
  return join(REPOSITORY_ROOT, relativePath);
}

/** The recorder's own repository-relative path, as its messages print it. */
function scriptPath(config) {
  return relative(REPOSITORY_ROOT, fileURLToPath(config.scriptUrl));
}

function usage(config, message) {
  process.stderr.write(
    `${message}\nusage: node ${scriptPath(config)} [--check]\n`,
  );
  process.exit(2);
}

function parseArguments(config, argv) {
  let check = false;
  for (const argument of argv) {
    if (argument === '--check') check = true;
    else usage(config, `unknown argument '${argument}'`);
  }
  return { check };
}

// The fixture chain (and the function it drives) is TypeScript with parameter
// properties, which Node's default strip-only mode refuses, so the recorder
// re-executes itself once with full type transformation.
function reexecuteWithTypeTransform(config, argv) {
  const result = spawnSync(
    process.execPath,
    [
      '--experimental-transform-types',
      '--no-warnings',
      fileURLToPath(config.scriptUrl),
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

function baselineSource(config, baseline) {
  const declarations = config.exports.map(
    (declaration) =>
      `${declaration.jsDoc}\nexport const ${declaration.name} = ${render(
        baseline[declaration.key],
      )} as const satisfies ${declaration.satisfies};`,
  );
  return `// SPDX-License-Identifier: Apache-2.0

${config.header}

${config.imports}

${declarations.join('\n\n')}
`;
}

function formatGeneratedFile(baselineFile) {
  const result = spawnSync(
    PNPM,
    ['exec', 'biome', 'check', '--write', baselineFile],
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

async function main(config, argv) {
  const { check } = parseArguments(config, argv);
  if (process.features.typescript !== 'transform') {
    reexecuteWithTypeTransform(config, argv);
  }
  registerTypeScriptResolution();
  const baselineFile = repositoryPath(config.baselineFile);
  const baseline = await config.run(
    await import(repositoryPath(config.worldModule)),
  );

  if (!check) {
    writeFileSync(baselineFile, baselineSource(config, baseline));
    formatGeneratedFile(baselineFile);
    process.stdout.write(
      `wrote ${config.baselineFile}: ${config.summary(baseline)}\n`,
    );
    return 0;
  }

  if (!existsSync(baselineFile)) {
    process.stderr.write(
      `${config.noun} baseline is missing: ${config.baselineFile}\n` +
        `run \`node ${scriptPath(config)}\` on the pre-rewrite tree\n`,
    );
    return 1;
  }
  const committed = await import(baselineFile);
  const differences = config.exports.flatMap((declaration) =>
    structuralDifferences(
      committed[declaration.name],
      baseline[declaration.key],
      declaration.key,
      [],
    ),
  );
  if (differences.length === 0) {
    process.stdout.write(
      `${config.noun} baseline matches ${config.baselineFile}: ${config.summary(baseline)}\n`,
    );
    return 0;
  }
  process.stderr.write(
    `${config.noun} baseline drifted from ${config.baselineFile}\n` +
      `${differences.length} structural difference(s), committed vs re-derived from the unchanged world:\n` +
      `${differences.map((difference) => `  ${difference}`).join('\n')}\n`,
  );
  return 1;
}

/**
 * Runs one golden-baseline recorder, when its own file is the invoked entry.
 *
 * The config is the recorder's whole domain, and nothing else — no path
 * machinery, and no message-only path string that could drift out of step with
 * the file the run actually reads or writes:
 * - `scriptUrl` — the recorder's `import.meta.url`, which gates this call, names
 *   the file the type-transform re-execution re-runs, and yields the
 *   repository-relative path the usage line and the re-recording hint print.
 * - `noun` — the domain noun the `--check` messages read as
 *   "<noun> baseline is missing/matches/drifted from".
 * - `worldModule` — the hand-authored world, repository-relative.
 * - `baselineFile` — the one file write mode writes, repository-relative. The
 *   same string is resolved for every read and write AND printed in every
 *   message, so the two can never disagree.
 * - `run(worldModule)` — drives the world(s) and returns the baseline object
 *   the `exports` keys index.
 * - `header` / `imports` — the generated file's leading comment block and its
 *   import lines, verbatim.
 * - `exports` — one entry per generated export: `name`, the baseline `key` it
 *   renders (which also names it in `--check` differences), its `jsDoc` block,
 *   and the `satisfies` type expression that gates the literals.
 * - `summary(baseline)` — the one-line count the write and match messages
 *   report.
 */
export async function runBaselineRecorder(config) {
  const invokedPath = process.argv[1]
    ? pathToFileURL(resolve(process.argv[1])).href
    : undefined;
  if (invokedPath !== config.scriptUrl) return;
  process.exit(await main(config, process.argv.slice(2)));
}
