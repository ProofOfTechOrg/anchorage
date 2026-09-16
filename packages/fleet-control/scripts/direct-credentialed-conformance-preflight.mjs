// SPDX-License-Identifier: Apache-2.0

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import ts from 'typescript';
import {
  deriveDirectConformanceNames,
  validateDirectConformanceConfig,
} from './direct-credentialed-conformance-config.mjs';
import { DIRECT_MAX_UPLOAD_BYTES } from './direct-credentialed-conformance-limits.mjs';

export { DIRECT_MAX_UPLOAD_BYTES } from './direct-credentialed-conformance-limits.mjs';

export const DIRECT_MANIFEST_MODULE = 'direct-run-manifest.js';

const REFERENCE_CORE_MODULES = new Set(['crypto', 'async_hooks', 'buffer']);
const TENANT_CORE_MODULES = new Set([
  'stream',
  'child_process',
  'fs',
  'path',
  'crypto',
  'os',
  'fs/promises',
  'module',
  'stream/web',
  'events',
  'async_hooks',
  'url',
  'path/posix',
  'string_decoder',
]);

function invalid(field) {
  return new Error(`direct conformance preflight has invalid ${field}`);
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

async function readBoundedFile(path, maximum, field) {
  try {
    const handle = await open(path, constants.O_RDONLY | constants.O_NONBLOCK);
    try {
      const stat = await handle.stat();
      if (!stat.isFile() || stat.size < 1 || stat.size > maximum)
        throw invalid(field);
      const bytes = Buffer.alloc(stat.size);
      let offset = 0;
      while (offset < bytes.length) {
        const { bytesRead } = await handle.read(
          bytes,
          offset,
          bytes.length - offset,
          offset,
        );
        if (bytesRead === 0) throw invalid(field);
        offset += bytesRead;
      }
      const extra = await handle.read(Buffer.alloc(1), 0, 1, offset);
      if (extra.bytesRead !== 0) throw invalid(field);
      return bytes;
    } finally {
      await handle.close();
    }
  } catch {
    throw invalid(`${field} file`);
  }
}

function utf8(bytes, field) {
  try {
    return new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(
      bytes,
    );
  } catch {
    throw invalid(`${field} UTF-8`);
  }
}

/**
 * The bracket-nesting ceiling both parsers below run under. Recursive-descent
 * depth is what exhausts a stack, and the host's stack size is what decides
 * when; bounding the depth here decides it for every host alike, so one
 * artifact gets one verdict.
 */
const DIRECT_MAX_NESTING_DEPTH = 512;

// A lexical count over the whole text, literal and comment content included: an
// unbalanced bracket inside a string raises the reading and refuses the
// artifact, which is the direction a gate fails in.
function inspectNesting(text, field) {
  let depth = 0;
  for (const character of text) {
    if (character === '(' || character === '[' || character === '{') {
      depth += 1;
      if (depth > DIRECT_MAX_NESTING_DEPTH) throw invalid(`${field} nesting`);
    } else if (
      (character === ')' || character === ']' || character === '}') &&
      depth > 0
    )
      depth -= 1;
  }
}

function inspectModule(text, reference, field, wasm) {
  inspectNesting(text, field);
  const syntax = spawnSync(
    process.execPath,
    ['--input-type=module', '--check'],
    {
      input: text,
      env: {},
      stdio: ['pipe', 'ignore', 'ignore'],
      timeout: 30_000,
    },
  );
  if (syntax.status !== 0) throw invalid(`${field} JavaScript`);
  try {
    inspectModuleStructure(text, reference, field, wasm);
  } catch {
    throw invalid(`${field} module inspection`);
  }
}

function inspectModuleStructure(text, reference, field, wasm) {
  const fileName = '/direct-artifact.js';
  const source = ts.createSourceFile(
    fileName,
    text,
    ts.ScriptTarget.ESNext,
    true,
    ts.ScriptKind.JS,
  );
  const host = {
    getSourceFile: (name) => (name === fileName ? source : undefined),
    getDefaultLibFileName: () => '/lib.d.ts',
    writeFile: () => {},
    getCurrentDirectory: () => '/',
    getDirectories: () => [],
    fileExists: (name) => name === fileName,
    readFile: (name) => (name === fileName ? text : undefined),
    getCanonicalFileName: (name) => name,
    useCaseSensitiveFileNames: () => true,
    getNewLine: () => '\n',
  };
  const program = ts.createProgram(
    [fileName],
    { allowJs: true, noResolve: true, noLib: true },
    host,
  );
  if (program.getSyntacticDiagnostics(source).length !== 0)
    throw invalid(`${field} syntax`);
  const checker = program.getTypeChecker();
  const symbol = checker.getSymbolAtLocation(source);
  const exports = symbol
    ? checker
        .getExportsOfModule(symbol)
        .map((entry) => entry.name)
        .sort()
    : [];
  const expected = reference
    ? ['default']
    : ['Maintenance', 'Runner', 'default'];
  if (JSON.stringify(exports) !== JSON.stringify(expected))
    throw invalid(`${field} exports`);

  let manifests = 0;
  const wasmPaths = new Set(wasm.map(({ name }) => `./${name}`));
  const coreModules = reference ? REFERENCE_CORE_MODULES : TENANT_CORE_MODULES;
  const inspectImport = (specifier, declaration) => {
    if (!specifier || !ts.isStringLiteralLike(specifier))
      throw invalid(`${field} module dependency`);
    if (reference && specifier.text === `./${DIRECT_MANIFEST_MODULE}`) {
      if (
        !declaration ||
        !ts.isImportDeclaration(declaration) ||
        !declaration.importClause?.name ||
        declaration.importClause.namedBindings ||
        declaration.attributes
      )
        throw invalid(`${field} manifest import`);
      manifests += 1;
    } else if (wasmPaths.has(specifier.text)) {
      if (
        !declaration ||
        !ts.isImportDeclaration(declaration) ||
        !declaration.importClause?.name ||
        declaration.importClause.namedBindings ||
        declaration.attributes
      )
        throw invalid(`${field} Wasm import`);
    } else if (
      specifier.text !== 'cloudflare:workers' &&
      !coreModules.has(specifier.text.replace(/^node:/, ''))
    ) {
      throw invalid(`${field} module dependency`);
    }
  };
  const pending = [source];
  for (let node = pending.pop(); node; node = pending.pop()) {
    if (ts.isExportDeclaration(node) && !node.exportClause)
      throw invalid(`${field} star export`);
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      if (node.moduleSpecifier) inspectImport(node.moduleSpecifier, node);
    } else if (
      ts.isCallExpression(node) &&
      node.expression.kind === ts.SyntaxKind.ImportKeyword
    ) {
      if (node.arguments.length !== 1)
        throw invalid(`${field} module dependency`);
      if (ts.isStringLiteralLike(node.arguments[0]) || reference)
        inspectImport(node.arguments[0]);
    }
    ts.forEachChild(node, (child) => {
      pending.push(child);
    });
  }
  if (reference && manifests !== 1) throw invalid(`${field} manifest import`);
}

function moduleSnapshot(name, source) {
  return Object.freeze({
    name,
    source,
    contentType: 'application/javascript+module',
    byteLength: Buffer.byteLength(source),
    sha256: sha256(source),
  });
}

async function readArtifact(configDirectory, intent, reference, field) {
  const bytes = await readBoundedFile(
    resolve(configDirectory, intent.bundle),
    DIRECT_MAX_UPLOAD_BYTES,
    field,
  );
  if (sha256(bytes) !== intent.sha256) throw invalid(`${field} digest`);
  const source = utf8(bytes, field);
  const wasm = [];
  let byteLength = bytes.byteLength;
  for (const descriptor of intent.auxiliaryWasm ?? []) {
    const binary = await readBoundedFile(
      resolve(configDirectory, descriptor.file),
      DIRECT_MAX_UPLOAD_BYTES - byteLength,
      `${field} Wasm`,
    );
    if (sha256(binary) !== descriptor.sha256)
      throw invalid(`${field} Wasm digest`);
    if (!WebAssembly.validate(binary)) throw invalid(`${field} Wasm format`);
    byteLength += binary.byteLength;
    wasm.push(
      Object.freeze({
        name: descriptor.name,
        base64: binary.toString('base64'),
        contentType: 'application/wasm',
        byteLength: binary.byteLength,
        sha256: descriptor.sha256,
      }),
    );
  }
  inspectModule(source, reference, field, wasm);
  return Object.freeze({
    main: moduleSnapshot(intent.mainModule, source),
    wasm: Object.freeze(wasm),
  });
}

function runtimeFields(runtime) {
  return {
    compatibilityDate: runtime.compatibilityDate,
    compatibilityFlags: runtime.compatibilityFlags,
    cpuLimitMs: runtime.cpuLimitMs,
    subrequestLimit: runtime.subrequestLimit,
  };
}

export async function readDirectConformanceConfig(input) {
  let configPath;
  try {
    configPath = resolve(input.configPath);
  } catch {
    throw invalid('config path');
  }
  const configBytes = await readBoundedFile(configPath, 256 * 1024, 'config');
  let parsed;
  try {
    parsed = JSON.parse(utf8(configBytes, 'config'));
  } catch {
    throw invalid('config JSON');
  }
  const config = validateDirectConformanceConfig(parsed, { now: input.now });
  return Object.freeze({ configPath, configBytes, config });
}

export async function preflightDirectConformance(input) {
  const { configPath, configBytes, config } =
    await readDirectConformanceConfig(input);
  const names = deriveDirectConformanceNames(config);
  const configDirectory = dirname(configPath);
  const reference = await readArtifact(
    configDirectory,
    config.referenceWorker.artifact,
    true,
    'reference artifact',
  );
  const tenant = await readArtifact(
    configDirectory,
    config.deployment.artifact,
    false,
    'tenant artifact',
  );
  const configSha256 = sha256(configBytes);
  const manifest = Object.freeze({
    contractVersion: config.contractVersion,
    configSha256,
    resourcePrefix: config.resourcePrefix,
    environment: config.environment,
    names,
    referenceRuntime: Object.freeze({
      ...runtimeFields(config.referenceWorker),
      requestTimeoutMs: config.referenceWorker.requestTimeoutMs,
      invocationTimeoutMs: config.referenceWorker.invocationTimeoutMs,
      maxProviderRequests: config.referenceWorker.maxProviderRequests,
      maxInvocations: config.referenceWorker.maxInvocations,
    }),
    deploymentRuntime: Object.freeze(runtimeFields(config.deployment)),
    tenantModule: tenant.main,
    tenantWasm: tenant.wasm,
    fixtureVersion: config.deployment.spec.fixtureVersion,
    interruption: config.interruption,
  });
  const manifestModule = moduleSnapshot(
    DIRECT_MANIFEST_MODULE,
    `export default ${JSON.stringify(manifest)};\n`,
  );
  const referenceModules = Object.freeze([
    reference.main,
    manifestModule,
    ...reference.wasm,
  ]);
  const referenceUploadBytes = referenceModules.reduce(
    (sum, module) => sum + module.byteLength,
    0,
  );
  if (referenceUploadBytes > DIRECT_MAX_UPLOAD_BYTES)
    throw invalid('reference upload size');
  const moduleTable = referenceModules.map(
    ({ name, contentType, byteLength, sha256 }) => ({
      name,
      contentType,
      byteLength,
      sha256,
    }),
  );
  return Object.freeze({
    config,
    names,
    configSha256,
    manifest,
    referenceModules,
    referenceUploadBytes,
    referenceModuleSetSha256: sha256(JSON.stringify(moduleTable)),
  });
}
