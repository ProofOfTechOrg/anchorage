// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  chmod,
  lstat,
  mkdir,
  open,
  readdir,
  readFile,
  writeFile,
} from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import {
  DIRECT_MANIFEST_MODULE,
  DIRECT_MAX_UPLOAD_BYTES,
  preflightDirectConformance,
  readDirectConformanceConfig,
} from './direct-credentialed-conformance-preflight.mjs';

function invalid(field) {
  return new Error(`direct conformance artifact build has invalid ${field}`);
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function writePrivate(path, bytes) {
  return writeFile(path, bytes, { flag: 'wx', mode: 0o600 });
}

function writeJson(path, value) {
  return writePrivate(path, `${JSON.stringify(value, null, 2)}\n`);
}

async function restrictBuildFiles(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      await chmod(path, 0o700);
      await restrictBuildFiles(path);
    } else if (entry.isFile()) {
      await chmod(path, 0o600);
    } else {
      throw invalid('build output file');
    }
  }
}

async function readModule(path) {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.size < 1 || stat.size > DIRECT_MAX_UPLOAD_BYTES)
    throw invalid('module file');
  const bytes = await readFile(path);
  if (bytes.length !== stat.size) throw invalid('module file size');
  return bytes;
}

async function resolveWrangler() {
  const require = createRequire(import.meta.url);
  const manifestPath = require.resolve('wrangler/package.json');
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
  const bin =
    typeof manifest.bin === 'string' ? manifest.bin : manifest.bin?.wrangler;
  if (typeof bin !== 'string' || !bin || typeof manifest.version !== 'string')
    throw invalid('Wrangler package');
  return {
    manifestPath,
    version: manifest.version,
    bin: resolve(dirname(manifestPath), bin),
  };
}

async function runWrangler(directory, wrangler, args) {
  const logPath = join(directory, 'dry-run.log');
  const log = await open(logPath, 'wx', 0o600);
  const startedAtMs = Date.now();
  const environment = {
    PATH: process.env.PATH ?? '',
    WRANGLER_SEND_METRICS: 'false',
  };
  let outcome;
  try {
    outcome = await new Promise((fulfill) => {
      const child = spawn(process.execPath, [wrangler.bin, ...args], {
        cwd: directory,
        env: environment,
        stdio: ['ignore', log.fd, log.fd],
      });
      let error;
      child.once('error', (cause) => {
        error = cause.message;
      });
      child.once('close', (status, signal) => {
        fulfill({ status, signal, ...(error ? { error } : {}) });
      });
    });
  } finally {
    await log.close();
  }
  await writeJson(join(directory, 'command.json'), {
    executable: process.execPath,
    args: [wrangler.bin, ...args],
    cwd: directory,
    wranglerManifestPath: wrangler.manifestPath,
    wranglerVersion: wrangler.version,
    nodeVersion: process.version,
    environmentKeys: Object.keys(environment),
    metrics: false,
    startedAtMs,
    completedAtMs: Date.now(),
    ...outcome,
  });
  if (outcome.status !== 0 || outcome.error)
    throw new Error(`direct conformance Wrangler build failed; see ${logPath}`);
}

function relativeArtifactPath(outputDirectory, path) {
  return relative(outputDirectory, path).split(sep).join('/');
}

async function inspectBuild(directory, outputDirectory, role, mainModule) {
  const metafilePath = join(directory, 'metafile.json');
  const metadata = JSON.parse(await readFile(metafilePath, 'utf8'));
  if (
    !metadata.outputs ||
    typeof metadata.outputs !== 'object' ||
    Array.isArray(metadata.outputs)
  )
    throw invalid('Wrangler outputs');
  const mainPath = join(directory, 'out', `${role}.js`);
  const entryPath = join(directory, 'src', `${role}.ts`);
  const entries = Object.entries(metadata.outputs).filter(
    ([, output]) =>
      output &&
      typeof output.entryPoint === 'string' &&
      resolve(directory, output.entryPoint) === entryPath,
  );
  const entry = entries[0];
  if (
    entries.length !== 1 ||
    resolve(directory, entry[0]) !== mainPath ||
    !Array.isArray(entry[1].imports)
  )
    throw invalid('Wrangler main output');
  const bytes = await readModule(mainPath);
  if (entry[1].bytes !== bytes.length)
    throw invalid('Wrangler main output size');
  const wasmNames = new Set();
  for (const imported of entry[1].imports) {
    if (!imported || typeof imported.path !== 'string')
      throw invalid('Wrangler output import');
    const path = imported.path;
    if (role === 'reference' && path === `./${DIRECT_MANIFEST_MODULE}`)
      continue;
    if (!path.startsWith('.') && !isAbsolute(path)) continue;
    if (
      !/^\.\/[A-Za-z0-9][A-Za-z0-9._-]*\.wasm$/u.test(path) ||
      imported.kind !== 'import-statement' ||
      imported.external !== true
    )
      throw invalid('flat relative output import');
    wasmNames.add(path.slice(2));
  }
  const auxiliaryWasm = [];
  for (const name of [...wasmNames].sort()) {
    const path = join(directory, 'out', name);
    auxiliaryWasm.push({
      file: relativeArtifactPath(outputDirectory, path),
      name,
      sha256: sha256(await readModule(path)),
    });
  }
  const digest = sha256(bytes);
  return {
    artifact: {
      bundle: relativeArtifactPath(outputDirectory, mainPath),
      mainModule,
      sha256: digest,
      ...(auxiliaryWasm.length > 0 ? { auxiliaryWasm } : {}),
    },
    measurements: Object.freeze({
      metafilePath,
      rawBytes: bytes.length,
      gzipBytes: gzipSync(bytes).length,
      sha256: digest,
    }),
  };
}

async function buildWorker(outputDirectory, role, intent, wrangler) {
  const directory = join(outputDirectory, role);
  const sourceDirectory = join(directory, 'src');
  const bundleDirectory = join(directory, 'out');
  await mkdir(directory, { mode: 0o700 });
  await mkdir(sourceDirectory, { mode: 0o700 });
  await mkdir(bundleDirectory, { mode: 0o700 });
  const reference = role === 'reference';
  const sourcePath = fileURLToPath(
    new URL(
      reference
        ? './direct-reference-worker.ts'
        : './direct-credentialed-tenant.ts',
      import.meta.url,
    ),
  );
  await writePrivate(
    join(sourceDirectory, `${role}.ts`),
    reference
      ? `import manifest from './${DIRECT_MANIFEST_MODULE}';\nimport { createDirectReferenceWorker } from ${JSON.stringify(sourcePath)};\nexport default createDirectReferenceWorker(manifest);\n`
      : `export { default, Maintenance, Runner } from ${JSON.stringify(sourcePath)};\n`,
  );
  if (reference)
    await writePrivate(
      join(sourceDirectory, DIRECT_MANIFEST_MODULE),
      "export default { buildPlaceholder: 'direct-conformance-manifest' };\n",
    );
  const wranglerConfigPath = join(directory, 'wrangler.json');
  await writeJson(wranglerConfigPath, {
    name: `direct-conformance-${role}-build`,
    main: `src/${role}.ts`,
    base_dir: 'src',
    compatibility_date: intent.compatibilityDate,
    compatibility_flags: intent.compatibilityFlags,
    limits: {
      cpu_ms: intent.cpuLimitMs,
      subrequests: intent.subrequestLimit,
    },
    ...(reference
      ? {
          rules: [
            {
              type: 'ESModule',
              globs: [`**/${DIRECT_MANIFEST_MODULE}`],
              fallthrough: false,
            },
          ],
          find_additional_modules: true,
        }
      : {}),
  });
  try {
    await runWrangler(directory, wrangler, [
      'deploy',
      '--config',
      wranglerConfigPath,
      '--dry-run',
      '--outdir',
      bundleDirectory,
      '--metafile',
      join(directory, 'metafile.json'),
      '--outfile',
      join(directory, 'upload.bundle'),
    ]);
  } finally {
    await restrictBuildFiles(directory);
  }
  return inspectBuild(
    directory,
    outputDirectory,
    role,
    intent.artifact.mainModule,
  );
}

export async function buildDirectConformanceArtifacts(input) {
  const loaded = await readDirectConformanceConfig(input);
  if (
    typeof input.outputDirectory !== 'string' ||
    !input.outputDirectory ||
    input.outputDirectory.includes('\0')
  )
    throw invalid('output directory');
  const outputDirectory = resolve(input.outputDirectory);
  const wrangler = await resolveWrangler();
  await mkdir(outputDirectory, { mode: 0o700 });
  await writePrivate(
    join(outputDirectory, 'input-conformance.json'),
    loaded.configBytes,
  );
  const reference = await buildWorker(
    outputDirectory,
    'reference',
    loaded.config.referenceWorker,
    wrangler,
  );
  const tenant = await buildWorker(
    outputDirectory,
    'tenant',
    loaded.config.deployment,
    wrangler,
  );
  const configPath = join(outputDirectory, 'conformance.json');
  await writeJson(configPath, {
    ...loaded.config,
    referenceWorker: {
      ...loaded.config.referenceWorker,
      artifact: reference.artifact,
    },
    deployment: {
      ...loaded.config.deployment,
      artifact: tenant.artifact,
    },
  });
  const prepared = await preflightDirectConformance({
    configPath,
    now: input.now,
  });
  return Object.freeze({
    configPath,
    prepared,
    builds: Object.freeze({
      reference: reference.measurements,
      tenant: tenant.measurements,
    }),
  });
}
