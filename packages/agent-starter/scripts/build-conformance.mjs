// SPDX-License-Identifier: Apache-2.0

/**
 * Build the three artifacts `pnpm fleet-control:credentialed` uploads: one
 * external candidate and two trusted state versions.
 *
 * Each must be ONE self-contained ES module. The gate reads the file as raw
 * bytes and uploads it as a single module named by the operator config, and it
 * manufactures release two of the candidate by appending
 * `\n// conformance-release:2\n` to those exact bytes
 * (packages/fleet-control/scripts/credentialed-conformance.mjs). This script
 * therefore verifies, not just builds: one emitted file, and still parseable
 * with that comment appended.
 */

import { execFileSync } from 'node:child_process';
import {
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = fileURLToPath(new URL('..', import.meta.url));
const outputDirectory = join(packageRoot, 'dist', 'conformance');

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
  rmSync(stagingDirectory, { recursive: true, force: true });
  execFileSync(
    'pnpm',
    [
      'exec',
      'wrangler',
      'deploy',
      '--dry-run',
      '--outdir',
      stagingDirectory,
      '--config',
      join(packageRoot, 'conformance', config),
    ],
    { cwd: packageRoot, stdio: 'inherit' },
  );

  const emitted = readdirSync(stagingDirectory).filter((name) =>
    name.endsWith('.js'),
  );
  if (emitted.length !== 1) {
    throw new Error(
      `${config} emitted ${emitted.length} JavaScript modules (${emitted.join(', ')}); the gate uploads exactly one`,
    );
  }
  const [only] = emitted;
  const bundle = readFileSync(join(stagingDirectory, only), 'utf8');
  writeFileSync(join(outputDirectory, output), bundle);
  rmSync(stagingDirectory, { recursive: true, force: true });
  return { output, bytes: Buffer.byteLength(bundle) };
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
const built = ARTIFACTS.map(buildOne);
for (const artifact of ARTIFACTS) {
  assertExportedClasses(
    join(outputDirectory, artifact.output),
    artifact.exports,
  );
}
assertSurvivesReleaseSuffix(join(outputDirectory, 'candidate.mjs'));
for (const artifact of built) {
  console.log(`${artifact.output}: ${artifact.bytes} bytes`);
}
