// SPDX-License-Identifier: Apache-2.0

import { spawnSync } from 'node:child_process';
import { existsSync, realpathSync, statSync, writeFileSync } from 'node:fs';
import { register } from 'node:module';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { isInvokedAsEntryPoint } from './entry-point.mjs';

const REPOSITORY_ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
// `pnpm exec` rather than a hard-coded node_modules/.bin path, matching
// build-api-docs.mjs; the .bin shim location is a pnpm implementation detail.
const PNPM = process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm';

function repositoryPath(relativePath, field) {
  if (
    typeof relativePath !== 'string' ||
    relativePath.length === 0 ||
    relativePath.includes('\0') ||
    isAbsolute(relativePath) ||
    /^[A-Za-z][A-Za-z0-9+.-]*:/u.test(relativePath)
  ) {
    throw new Error(`${field} must be a repository-relative filesystem path`);
  }
  const filePath = resolve(REPOSITORY_ROOT, relativePath);
  const fromRoot = relative(REPOSITORY_ROOT, filePath);
  if (
    fromRoot === '' ||
    fromRoot === '..' ||
    fromRoot.startsWith(`..${sep}`) ||
    isAbsolute(fromRoot)
  ) {
    throw new Error(`${field} must resolve to a path inside the repository`);
  }
  return filePath;
}

function scriptPath(config) {
  return relative(REPOSITORY_ROOT, fileURLToPath(config.scriptUrl))
    .split(sep)
    .join('/');
}

function usage(config, message) {
  process.stderr.write(
    `${message}\nusage: node ${scriptPath(config)} --check | --write\n`,
  );
}

function parseArguments(config, argv) {
  let mode;
  for (const argument of argv) {
    if (argument !== '--check' && argument !== '--write') {
      usage(config, `unknown argument '${argument}'`);
      return undefined;
    }
    if (mode && mode !== argument) {
      usage(config, 'conflicting modes: choose --check or --write');
      return undefined;
    }
    mode = argument;
  }
  if (!mode) usage(config, 'missing mode: choose --check or --write');
  return mode;
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
  return result.status ?? 1;
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
  const escaped = JSON.stringify(value)
    .slice(1, -1)
    .replaceAll('\\"', '"')
    .replaceAll("'", "\\'");
  return `'${escaped}'`;
}

function primitive(value) {
  if (value === undefined) return 'undefined';
  if (value === null) return 'null';
  if (typeof value === 'string') return quoted(value);
  if (Object.is(value, -0)) return '-0';
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  throw new Error(`unsupported baseline value type '${typeof value}'`);
}

function isComposite(value) {
  return typeof value === 'object' && value !== null;
}

function validateValue(value, path, ancestors = new Set()) {
  if (!isComposite(value)) {
    if (
      value === null ||
      value === undefined ||
      typeof value === 'string' ||
      typeof value === 'boolean' ||
      typeof value === 'number'
    )
      return;
    throw new Error(
      `${path}: unsupported baseline value type '${typeof value}'`,
    );
  }
  const array = Array.isArray(value);
  const prototype = Object.getPrototypeOf(value);
  if (
    array
      ? prototype !== Array.prototype
      : prototype !== Object.prototype && prototype !== null
  ) {
    throw new Error(`${path}: expected a plain map or ordinary array`);
  }
  if (ancestors.has(value)) throw new Error(`${path}: cyclic baseline value`);
  ancestors.add(value);
  const keys = Reflect.ownKeys(value).filter(
    (key) => !array || key !== 'length',
  );
  if (array && keys.length !== value.length) {
    throw new Error(`${path}: expected a dense array without extra properties`);
  }
  for (const [index, key] of keys.entries()) {
    if (typeof key !== 'string') {
      throw new Error(`${path}: symbol properties are unsupported`);
    }
    if (array && key !== String(index)) {
      throw new Error(
        `${path}: expected a dense array without extra properties`,
      );
    }
    const propertyPath = `${path}[${array ? key : JSON.stringify(key)}]`;
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new Error(`${propertyPath}: expected an enumerable data property`);
    }
    validateValue(descriptor.value, propertyPath, ancestors);
  }
  ancestors.delete(value);
}

function configuredValue(values, key, path) {
  if (!isComposite(values) || !Object.hasOwn(values, key)) {
    throw new Error(`${path}: missing own property`);
  }
  const descriptor = Object.getOwnPropertyDescriptor(values, key);
  if (!descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
    throw new Error(`${path}: expected an enumerable data property`);
  }
  validateValue(descriptor.value, path);
  return descriptor.value;
}

function propertyKey(key) {
  if (key === '__proto__') return "['__proto__']";
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
  const declarations = config.exports.map((declaration) => {
    const value = baseline[declaration.key];
    // TypeScript restricts const assertions to literal expressions.
    const assertion =
      value == null || (typeof value === 'number' && !Number.isFinite(value))
        ? ''
        : ' as const';
    return `${declaration.jsDoc ? `${declaration.jsDoc}\n` : ''}export const ${declaration.name} = ${render(value)}${assertion} satisfies ${declaration.satisfies};`;
  });
  return `// SPDX-License-Identifier: Apache-2.0

/**
 * GENERATED FILE. DO NOT EDIT BY HAND.
 */

${config.imports}

${declarations.join('\n\n')}
`;
}

function formatGeneratedFile(baselineFilePath) {
  const result = spawnSync(
    PNPM,
    ['exec', 'biome', 'check', '--write', baselineFilePath],
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

function structuralDifferences(committed, derived, path, differences) {
  if (isComposite(committed) !== isComposite(derived)) {
    differences.push(
      `${path}: committed ${describeValue(committed)} / derived ${describeValue(derived)}`,
    );
    return differences;
  }
  if (!isComposite(committed)) {
    if (!Object.is(committed, derived)) {
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
  if (
    committedKeys.length !== derivedKeys.length ||
    committedKeys.some((key, index) => key !== derivedKeys[index])
  ) {
    differences.push(
      `${path}: committed keys ${JSON.stringify(committedKeys)} / derived keys ${JSON.stringify(derivedKeys)}`,
    );
  }
  for (const key of new Set([...committedKeys, ...derivedKeys])) {
    if (!Object.hasOwn(committed, key) || !Object.hasOwn(derived, key)) {
      const onlyDerived = !Object.hasOwn(committed, key);
      const side = onlyDerived ? 'derived only' : 'committed only';
      const value = onlyDerived ? derived[key] : committed[key];
      differences.push(`${path}.${key}: ${side} ${describeValue(value)}`);
      continue;
    }
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
  const mode = parseArguments(config, argv);
  if (!mode) return 2;
  if (process.features.typescript !== 'transform') {
    return reexecuteWithTypeTransform(config, argv);
  }
  registerTypeScriptResolution();
  const baselineFilePath = repositoryPath(config.baselineFile, 'baselineFile');
  const worldModulePath = repositoryPath(config.worldModule, 'worldModule');
  if (!existsSync(worldModulePath)) {
    process.stderr.write(
      `${config.noun} world is missing: ${config.worldModule}\n`,
    );
    return 1;
  }
  const world = await import(pathToFileURL(worldModulePath).href);
  const baseline = await config.run(world);
  for (const declaration of config.exports) {
    configuredValue(
      baseline,
      declaration.key,
      `derived key '${declaration.key}'`,
    );
  }

  if (mode === '--write') {
    writeFileSync(baselineFilePath, baselineSource(config, baseline));
    formatGeneratedFile(baselineFilePath);
    process.stdout.write(
      `wrote ${config.baselineFile}: ${config.summary(baseline)}\n`,
    );
    return 0;
  }

  if (!existsSync(baselineFilePath)) {
    process.stderr.write(
      `${config.noun} baseline is missing: ${config.baselineFile}\n` +
        `run \`node ${scriptPath(config)} --write\` to record the baseline\n`,
    );
    return 1;
  }
  // A committed export with no matching `config.exports` declaration is
  // compared against nothing.
  const committed = await import(pathToFileURL(baselineFilePath).href);
  const differences = config.exports.flatMap((declaration) =>
    structuralDifferences(
      configuredValue(
        committed,
        declaration.name,
        `committed export '${declaration.name}'`,
      ),
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
 * Runs a recorder entry with explicit `--check` or `--write`.
 *
 * Configure `scriptUrl: import.meta.url`, repository-relative `worldModule`
 * and `baselineFile` paths, `run(world)`, `noun`, `summary(baseline)`, and
 * generated `imports`. Each `exports` declaration supplies `name`, a derived
 * `key`, a `satisfies` type expression, and optional `jsDoc`.
 *
 * Selected values support undefined, null, strings, booleans, numbers, dense
 * ordinary arrays, and plain or null-prototype maps with enumerable string
 * data properties. Map prototypes and shared references are not preserved;
 * key order and undefined-valued key presence are significant.
 */
export async function runBaselineRecorder(config) {
  // An entry-less process has nothing to compare, so it returns ahead of the
  // scriptUrl validation rather than throwing on a caller that configured none.
  if (!process.argv[1]) return;
  let scriptUrl;
  try {
    scriptUrl = new URL(config.scriptUrl);
  } catch {
    throw new Error('recorder scriptUrl must be a file URL');
  }
  if (scriptUrl.protocol !== 'file:') {
    throw new Error('recorder scriptUrl must be a file URL');
  }
  const recorderFilePath = realpathSync(fileURLToPath(scriptUrl));
  if (!statSync(recorderFilePath).isFile()) {
    throw new Error('recorder scriptUrl must name a file');
  }
  if (!isInvokedAsEntryPoint(scriptUrl)) return;
  process.exitCode = await main(config, process.argv.slice(2));
}
