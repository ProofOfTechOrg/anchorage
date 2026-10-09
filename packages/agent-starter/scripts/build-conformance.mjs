// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import {
  basename,
  dirname,
  isAbsolute,
  join,
  relative,
  resolve,
} from 'node:path';
import { fileURLToPath } from 'node:url';

import { buildConformanceConfig } from './emit-conformance-config.mjs';

const packageRoot = fileURLToPath(new URL('..', import.meta.url));
const outputDirectory = join(packageRoot, 'dist', 'conformance');
const generatedConfigPath = join(
  outputDirectory,
  'anchorage-starter.conformance.json',
);
const wasmModules = new Map();

const contract = JSON.parse(
  readFileSync(
    join(packageRoot, 'src', 'conformance', 'contract.json'),
    'utf8',
  ),
);
const stateV1Classes = [
  ...contract.durableObjectBindings.map((binding) => binding.className),
  contract.auditProxyClassName,
];

/**
 * `exports` is the class set the artifact must expose. Cloudflare rejects an
 * upload whose migrations name a class the module does not export, and the
 * operator config's migrations are rendered from this same contract.json — so
 * checking both ends here is what stops an artifact and its migration history
 * from drifting apart between now and the paid run.
 */
const ARTIFACTS = [
  {
    config: 'wrangler.candidate.jsonc',
    output: 'candidate.mjs',
    // An external candidate may not own a Durable Object class.
    exports: [],
  },
  {
    config: 'wrangler.state-v1.jsonc',
    output: 'trusted-state-v1.mjs',
    exports: stateV1Classes,
  },
  {
    config: 'wrangler.state-v2.jsonc',
    output: 'trusted-state-v2.mjs',
    exports: [...stateV1Classes, contract.newDurableObjectBinding.className],
  },
];

/** The exact suffix the gate appends to make the candidate's release two. */
const RELEASE_SUFFIX = '\n// conformance-release:2\n';

function buildOne({ config, output }) {
  const stagingDirectory = join(outputDirectory, `.staging-${output}`);
  const configPath = join(packageRoot, 'conformance', config);
  const metafilePath = join(stagingDirectory, 'metafile.json');
  rmSync(stagingDirectory, { recursive: true, force: true });
  try {
    execFileSync(
      'pnpm',
      [
        'exec',
        'wrangler',
        'deploy',
        '--dry-run',
        '--outdir',
        stagingDirectory,
        '--metafile',
        metafilePath,
        '--config',
        configPath,
      ],
      { cwd: packageRoot, stdio: 'inherit' },
    );

    const emitted = readdirSync(stagingDirectory).filter((name) =>
      name.endsWith('.js'),
    );
    if (emitted.length !== 1) {
      throw new Error(
        `${config} emitted ${emitted.length} JavaScript modules (${emitted.join(', ')}); the gate uploads one main module`,
      );
    }
    const mainPath = join(stagingDirectory, emitted[0]);
    const metadata = JSON.parse(readFileSync(metafilePath, 'utf8'));
    const entries = Object.entries(metadata.outputs ?? {}).filter(
      ([path, candidate]) =>
        resolve(dirname(configPath), path) === mainPath &&
        typeof candidate.entryPoint === 'string',
    );
    const bundle = readFileSync(mainPath);
    const entry = entries[0]?.[1];
    if (
      entries.length !== 1 ||
      entry.bytes !== bundle.length ||
      !Array.isArray(entry.imports)
    ) {
      throw new Error(`${config} has invalid Wrangler main output metadata`);
    }
    const wasmNames = new Set();
    for (const imported of entry.imports) {
      if (!imported || typeof imported.path !== 'string') {
        throw new Error(`${config} has invalid Wrangler import metadata`);
      }
      const path = imported.path;
      if (!path.startsWith('.') && !isAbsolute(path)) continue;
      if (
        !/^\.\/[A-Za-z0-9][A-Za-z0-9._-]*\.wasm$/u.test(path) ||
        imported.kind !== 'import-statement' ||
        imported.external !== true
      ) {
        throw new Error(`${config} has an unsupported output import: ${path}`);
      }
      wasmNames.add(path.slice(2));
    }
    const auxiliaryWasm = [...wasmNames].sort().map((name) => {
      const bytes = readFileSync(join(stagingDirectory, name));
      if (!WebAssembly.validate(bytes)) {
        throw new Error(`${config} emitted invalid Wasm: ${name}`);
      }
      const previous = wasmModules.get(name);
      if (previous && !previous.equals(bytes)) {
        throw new Error(`${config} emitted conflicting Wasm bytes: ${name}`);
      }
      wasmModules.set(name, bytes);
      const file = join(outputDirectory, name);
      writeFileSync(file, bytes);
      return {
        name,
        file: relative(join(packageRoot, '../fleet-control'), file),
        sha256: createHash('sha256').update(bytes).digest('hex'),
      };
    });
    writeFileSync(join(outputDirectory, output), bundle);
    return { output, bytes: bundle.length, auxiliaryWasm };
  } finally {
    rmSync(stagingDirectory, { recursive: true, force: true });
  }
}

function assertSurvivesReleaseSuffix(path) {
  // A real ES-module parse with no evaluation: `node --check` on an .mjs file
  // rejects exactly what a broken append would produce — an unterminated block
  // comment or template literal — without needing Worker globals to exist.
  const probe = `${path}.release-two-probe.mjs`;
  writeFileSync(probe, `${readFileSync(path, 'utf8')}${RELEASE_SUFFIX}`);
  try {
    execFileSync(process.execPath, ['--check', probe], { stdio: 'pipe' });
  } catch (error) {
    throw new Error(
      `${path} does not survive the gate's release-two suffix: ${String(error.stderr ?? error)}`,
    );
  } finally {
    rmSync(probe, { force: true });
  }
}

/**
 * The bundler emits one terminal `export { ... }` list. Everything before
 * `as` is the local name; the exported name is what Cloudflare resolves a
 * migration's class against.
 */
function exportedNames(path) {
  const bundle = readFileSync(path, 'utf8');
  const blocks = [...bundle.matchAll(/^export \{$([\s\S]*?)^\};$/gmu)];
  const last = blocks.at(-1);
  // Failing closed matters most for the candidate, whose expectation is the
  // EMPTY set: a regex that silently found nothing would satisfy "owns no
  // Durable Object class" without having looked.
  if (!last) {
    throw new Error(
      `${path} has no terminal export list; the bundler's output shape changed`,
    );
  }
  return last[1]
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => entry.split(/\s+as\s+/u).at(-1) ?? entry);
}

function assertExportedClasses(path, expected) {
  const actual = exportedNames(path).filter((name) => name !== 'default');
  const missing = expected.filter((name) => !actual.includes(name));
  const unexpected = actual.filter((name) => !expected.includes(name));
  if (missing.length > 0 || unexpected.length > 0) {
    throw new Error(
      `${path} exports [${actual.join(', ')}] but its migrations declare [${expected.join(', ')}]`,
    );
  }
}

mkdirSync(outputDirectory, { recursive: true });
rmSync(generatedConfigPath, { force: true });
const built = ARTIFACTS.map(buildOne);
for (const artifact of ARTIFACTS) {
  assertExportedClasses(
    join(outputDirectory, artifact.output),
    artifact.exports,
  );
}
assertSurvivesReleaseSuffix(join(outputDirectory, 'candidate.mjs'));
const operatorConfig = buildConformanceConfig();
function wasmFor(bundle) {
  const artifact = built.find((item) => item.output === basename(bundle));
  if (!artifact) throw new Error(`No built conformance artifact for ${bundle}`);
  return artifact.auxiliaryWasm;
}
operatorConfig.auxiliaryWasm = wasmFor(operatorConfig.workerBundle);
for (const profile of operatorConfig.platformProfile.stateProfiles) {
  profile.stateWorker.auxiliaryWasm = wasmFor(profile.stateWorker.bundle);
}
writeFileSync(
  generatedConfigPath,
  `${JSON.stringify(operatorConfig, null, 2)}\n`,
);
for (const artifact of built) {
  console.log(`${artifact.output}: ${artifact.bytes} bytes`);
}
