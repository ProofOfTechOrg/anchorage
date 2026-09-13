// SPDX-License-Identifier: Apache-2.0

import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  mkdir,
  mkdtemp,
  open,
  readFile,
  rm,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  DIRECT_MANIFEST_MODULE,
  DIRECT_MAX_UPLOAD_BYTES,
  preflightDirectConformance,
  readDirectConformanceConfig,
} from '../scripts/direct-credentialed-conformance-preflight.mjs';

// TypeScript's CommonJS namespace exposes createSourceFile as a non-configurable
// getter, so the compiler seam is reachable only by mocking the module.
const compiler = vi.hoisted(() => ({ failOnCall: 0 }));

vi.mock('typescript', async (importOriginal) => {
  const actual = await importOriginal<{
    default: typeof import('typescript');
  }>();
  const createSourceFile = (
    ...args: Parameters<typeof actual.default.createSourceFile>
  ) => {
    if (compiler.failOnCall > 0) {
      compiler.failOnCall -= 1;
      if (compiler.failOnCall === 0)
        throw new RangeError('Maximum call stack size exceeded');
    }
    return actual.default.createSourceFile(...args);
  };
  return {
    ...actual,
    createSourceFile,
    default: { ...actual.default, createSourceFile },
  };
});

const NOW = Date.parse('2026-09-09T12:00:00.000Z');
const REFERENCE = `import manifest from './direct-run-manifest.js';
export default {fetch() {return Response.json(manifest.contractVersion)}};`;
const TENANT = `export class Maintenance {}
export class Runner {}
export default {fetch() {return new Response('fixture')}};`;
const directories: string[] = [];

function digest(value: string | Uint8Array) {
  return createHash('sha256').update(value).digest('hex');
}

async function fixture(reference = REFERENCE, tenant = TENANT) {
  const directory = await mkdtemp(join(tmpdir(), 'fleet-direct-preflight-'));
  directories.push(directory);
  const config = JSON.parse(
    await readFile(
      new URL(
        '../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
      'utf8',
    ),
  );
  const configPath = join(directory, 'config.json');
  const referencePath = join(directory, 'reference artifact.mjs');
  const tenantPath = join(directory, 'tenant artifact.mjs');
  config.referenceWorker.artifact.bundle = './reference artifact.mjs';
  config.referenceWorker.artifact.sha256 = digest(reference);
  config.deployment.artifact.bundle = './tenant artifact.mjs';
  config.deployment.artifact.sha256 = digest(tenant);
  await writeFile(referencePath, reference);
  await writeFile(tenantPath, tenant);
  const save = () => writeFile(configPath, JSON.stringify(config));
  await save();
  return { directory, configPath, referencePath, tenantPath, config, save };
}

function prepare(configPath: string) {
  return preflightDirectConformance({ configPath, now: NOW });
}

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  await Promise.all(
    directories
      .splice(0)
      .map((path) => rm(path, { recursive: true, force: true })),
  );
});

describe('direct artifact preflight', () => {
  it('reads validated config before artifact creation while preflight still requires the bytes', async () => {
    const f = await fixture();
    await rm(f.referencePath);
    const result = await readDirectConformanceConfig({
      configPath: f.configPath,
      now: NOW,
    });
    expect(result.configPath).toBe(f.configPath);
    expect(result.configBytes).toEqual(await readFile(f.configPath));
    expect(result.config).toEqual(f.config);
    expect(Object.isFrozen(result)).toBe(true);
    await expect(prepare(f.configPath)).rejects.toThrow(
      'reference artifact file',
    );
  });

  it('accepts literal-template compatibility imports through the ordinary dependency check', async () => {
    const f = await fixture(
      `${REFERENCE}\nimport(\`node:buffer\`);`,
      `${TENANT}\nimport(\`node:crypto\`);`,
    );
    await expect(prepare(f.configPath)).resolves.toBeDefined();
  });

  it.each([
    './missing.js',
    'undeclared-package',
  ])('rejects a literal template tenant dependency %s', async (dependency) => {
    const f = await fixture(REFERENCE, `${TENANT}\nimport(\`${dependency}\`);`);
    await expect(prepare(f.configPath)).rejects.toThrow(
      /tenant artifact module inspection/,
    );
  });

  it.each([
    'reference',
    'tenant',
  ] as const)('rejects unresolved star exports for the %s role', async (role) => {
    for (const external of [
      'node:crypto',
      'node:async_hooks',
      'cloudflare:workers',
    ]) {
      const suffix = `\nexport * from '${external}';`;
      const f = await fixture(
        REFERENCE + (role === 'reference' ? suffix : ''),
        TENANT + (role === 'tenant' ? suffix : ''),
      );
      await expect(prepare(f.configPath)).rejects.toThrow(/artifact/);
    }
  });

  it.each([
    'reference',
    'tenant',
  ] as const)('inspects a valid deep addition expression for the %s role', async (role) => {
    const suffix = `\nconst sum=${'1+'.repeat(12_000)}1;`;
    const f = await fixture(
      REFERENCE + (role === 'reference' ? suffix : ''),
      TENANT + (role === 'tenant' ? suffix : ''),
    );
    await expect(prepare(f.configPath)).resolves.toMatchObject({
      manifest: { fixtureVersion: 1 },
    });
  });

  it.each([
    'reference',
    'tenant',
  ] as const)('contains compiler failures for the %s role', async (role) => {
    compiler.failOnCall = role === 'reference' ? 1 : 2;
    const f = await fixture();
    await expect(prepare(f.configPath)).rejects.toThrow(
      `direct conformance preflight has invalid ${role} artifact module inspection`,
    );
  });

  it('rejects a JSON import attribute for the generated JavaScript manifest', async () => {
    const f = await fixture(
      "import manifest from './direct-run-manifest.js' with {type:'json'}; export default {};",
    );
    await expect(prepare(f.configPath)).rejects.toThrow(/reference artifact/);
  });

  it('retains immutable exact UTF-8 snapshots and binds the generated upload module table', async () => {
    const tenant = `\uFEFF${TENANT}\n// café 🦀`;
    const f = await fixture(REFERENCE, tenant);
    const result = await prepare(f.configPath);
    expect(result.configSha256).toBe(digest(await readFile(f.configPath)));
    expect(result.manifest.tenantModule.source).toBe(tenant);
    expect(result.manifest.tenantModule.sha256).toBe(digest(tenant));
    expect(result.referenceModules.map((module) => module.name)).toEqual([
      'worker.js',
      DIRECT_MANIFEST_MODULE,
    ]);
    const moduleTable = result.referenceModules.map((module) => {
      const { name, contentType, byteLength, sha256 } = module;
      const bytes =
        'source' in module
          ? Buffer.from(module.source)
          : Buffer.from(module.base64, 'base64');
      expect(byteLength).toBe(bytes.byteLength);
      expect(sha256).toBe(digest(bytes));
      expect(Object.isFrozen(result.referenceModules)).toBe(true);
      return { name, contentType, byteLength, sha256 };
    });
    expect(result.referenceModuleSetSha256).toBe(
      digest(JSON.stringify(moduleTable)),
    );
    expect(result.referenceUploadBytes).toBe(
      moduleTable.reduce((sum, module) => sum + module.byteLength, 0),
    );
    const generated = result.referenceModules[1].source;
    const manifest = JSON.parse(generated.slice('export default '.length, -2));
    expect(manifest).toEqual(result.manifest);
    expect(generated).not.toContain(f.directory);
    expect(generated).not.toContain('artifact.mjs');
    expect(generated).not.toContain(result.referenceModuleSetSha256);
    const assertFrozen = (value: unknown): void => {
      if (!value || typeof value !== 'object') return;
      expect(Object.isFrozen(value)).toBe(true);
      for (const child of Object.values(value)) assertFrozen(child);
    };
    assertFrozen(result);
    await writeFile(f.referencePath, 'changed');
    await writeFile(f.tenantPath, 'changed');
    expect(result.referenceModules[0].source).toBe(REFERENCE);
    expect(result.manifest.tenantModule.source).toBe(tenant);
  });

  it('retains checked auxiliary Wasm bytes for both uploads and their manifest identities', async () => {
    const binary = Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]);
    const dependency = "import binary from './fixture.wasm';\n";
    const f = await fixture(dependency + REFERENCE, dependency + TENANT);
    await writeFile(join(f.directory, 'fixture.wasm'), binary);
    for (const role of ['referenceWorker', 'deployment'])
      f.config[role].artifact.auxiliaryWasm = [
        {
          file: './fixture.wasm',
          name: 'fixture.wasm',
          sha256: digest(binary),
        },
      ];
    await f.save();
    const result = await prepare(f.configPath);
    const expected = {
      name: 'fixture.wasm',
      base64: binary.toString('base64'),
      contentType: 'application/wasm',
      byteLength: 8,
      sha256: digest(binary),
    };
    expect(result.referenceModules[2]).toEqual(expected);
    expect(result.manifest.tenantWasm).toEqual([expected]);
    expect(Object.isFrozen(result.manifest.tenantWasm)).toBe(true);
    expect(Object.isFrozen(result.referenceModules[2])).toBe(true);
    const table = result.referenceModules.map(
      ({ name, contentType, byteLength, sha256 }) => ({
        name,
        contentType,
        byteLength,
        sha256,
      }),
    );
    expect(result.referenceModuleSetSha256).toBe(digest(JSON.stringify(table)));
    expect(result.referenceUploadBytes).toBe(
      table.reduce((sum, module) => sum + module.byteLength, 0),
    );
    await writeFile(join(f.directory, 'fixture.wasm'), 'changed');
    expect(
      Buffer.from(result.manifest.tenantWasm[0]?.base64 ?? '', 'base64'),
    ).toEqual(binary);
  });

  it.each([
    'missing',
    'digest',
    'format',
    'aggregate-size',
  ] as const)('rejects %s auxiliary Wasm', async (kind) => {
    const f = await fixture();
    const binary = Buffer.from(
      kind === 'format' ? 'not a module' : [0, 97, 115, 109, 1, 0, 0, 0],
    );
    const path = join(f.directory, 'fixture.wasm');
    if (kind !== 'missing') await writeFile(path, binary);
    if (kind === 'aggregate-size') {
      const handle = await open(path, 'w');
      try {
        await handle.truncate(DIRECT_MAX_UPLOAD_BYTES);
      } finally {
        await handle.close();
      }
    }
    f.config.deployment.artifact.auxiliaryWasm = [
      {
        file: './fixture.wasm',
        name: 'fixture.wasm',
        sha256: kind === 'digest' ? '0'.repeat(64) : digest(binary),
      },
    ];
    await f.save();
    await expect(prepare(f.configPath)).rejects.toThrow(/tenant artifact Wasm/);
  });

  it('rejects an unrecorded Wasm dependency', async () => {
    const f = await fixture(
      REFERENCE,
      `import binary from './fixture.wasm';\n${TENANT}`,
    );
    await expect(prepare(f.configPath)).rejects.toThrow(
      /tenant artifact module inspection/,
    );
  });

  it('keeps reference and tenant Node dependency contracts separate', async () => {
    const tenant = await fixture(
      REFERENCE,
      `import 'node:fs'; import 'stream/web';\n${TENANT}`,
    );
    await expect(prepare(tenant.configPath)).resolves.toBeDefined();
    const reference = await fixture(`import 'node:fs';\n${REFERENCE}`, TENANT);
    await expect(prepare(reference.configPath)).rejects.toThrow(
      /reference artifact module inspection/,
    );
  });

  it('leaves optional computed tenant imports to runtime acceptance while retaining the reference contract', async () => {
    const optional =
      "async function optionalDependency(){try{const name='optional-package';return await import(name)}catch{return null}}";
    const tenant = await fixture(REFERENCE, `${TENANT}\n${optional}`);
    await expect(prepare(tenant.configPath)).resolves.toBeDefined();
    const reference = await fixture(`${REFERENCE}\n${optional}`, TENANT);
    await expect(prepare(reference.configPath)).rejects.toThrow(
      /reference artifact module inspection/,
    );
  });

  it('accepts symlinked regular input files and config-relative paths with spaces', async () => {
    const f = await fixture();
    const link = join(f.directory, 'config link.json');
    await symlink(f.configPath, link);
    await symlink(f.tenantPath, join(f.directory, 'tenant link.mjs'));
    f.config.deployment.artifact.bundle = './tenant link.mjs';
    await f.save();
    expect((await prepare(link)).manifest.tenantModule.source).toBe(TENANT);
  });

  it('never evaluates artifact code or inherits Node preload options for its syntax check', async () => {
    const network = vi
      .spyOn(globalThis, 'fetch')
      .mockRejectedValue(new Error('unexpected network'));
    const f = await fixture(
      `${REFERENCE}\nthrow new Error('artifact evaluated');`,
      `${TENANT}\nthrow new Error('artifact evaluated');`,
    );
    vi.stubEnv(
      'NODE_OPTIONS',
      '--require /nonexistent-direct-preflight-preload.cjs',
    );
    await expect(prepare(f.configPath)).resolves.toMatchObject({
      manifest: { fixtureVersion: 1 },
    });
    expect(network).not.toHaveBeenCalled();
  });

  it.each([
    'referenceWorker',
    'deployment',
  ])('rejects a mismatched %s digest', async (role) => {
    const f = await fixture();
    f.config[role].artifact.sha256 = 'f'.repeat(64);
    await f.save();
    await expect(prepare(f.configPath)).rejects.toThrow(/artifact digest/);
  });

  it.each([
    'missing',
    'directory',
    'empty',
    'oversized',
    'invalid-utf8',
  ])('rejects %s artifact input', async (kind) => {
    const f = await fixture();
    if (kind === 'missing') await rm(f.tenantPath);
    if (kind === 'directory') {
      await rm(f.tenantPath);
      await mkdir(f.tenantPath);
    }
    if (kind === 'empty') await writeFile(f.tenantPath, '');
    if (kind === 'oversized') {
      const handle = await open(f.tenantPath, 'w');
      try {
        await handle.truncate(DIRECT_MAX_UPLOAD_BYTES + 1);
      } finally {
        await handle.close();
      }
    }
    if (kind === 'invalid-utf8') {
      const bytes = Buffer.from([0xc3, 0x28]);
      await writeFile(f.tenantPath, bytes);
      f.config.deployment.artifact.sha256 = digest(bytes);
      await f.save();
    }
    await expect(prepare(f.configPath)).rejects.toThrow(/tenant artifact/);
  });

  it('rejects a FIFO without waiting for a writer', async () => {
    const f = await fixture();
    await rm(f.tenantPath);
    execFileSync('mkfifo', [f.tenantPath]);
    await expect(prepare(f.configPath)).rejects.toThrow(/tenant artifact file/);
  });

  it.each([
    'oversized',
    'invalid-utf8',
    'invalid-json',
    'invalid-schema',
  ])('rejects %s config before reading artifacts', async (kind) => {
    const f = await fixture();
    await rm(f.referencePath);
    if (kind === 'oversized')
      await writeFile(f.configPath, ' '.repeat(256 * 1024 + 1));
    if (kind === 'invalid-utf8')
      await writeFile(f.configPath, Buffer.from([0xc3, 0x28]));
    if (kind === 'invalid-json')
      await writeFile(f.configPath, '{"secret-sentinel"');
    if (kind === 'invalid-schema')
      await writeFile(f.configPath, '{"secret-sentinel":true}');
    await expect(prepare(f.configPath)).rejects.toThrow(/config/);
    await expect(prepare(f.configPath)).rejects.not.toThrow(
      /secret-sentinel|reference artifact/,
    );
  });

  it.each([
    'export default {',
    'const value: number = 1; export default value;',
    'export default {}; export default {};',
  ])('rejects invalid JavaScript without leaking diagnostics', async (source) => {
    const f = await fixture(`${source}\n// secret-sentinel`);
    await expect(prepare(f.configPath)).rejects.toThrow(
      /reference artifact JavaScript/,
    );
    await expect(prepare(f.configPath)).rejects.not.toThrow(/secret-sentinel/);
  });

  it.each([
    'export default {};',
    `${REFERENCE}\nexport const extra = 1;`,
    `import './direct-run-manifest.js'; export default {};`,
    `import * as manifest from './direct-run-manifest.js'; export default {};`,
    `${REFERENCE}\nimport second from './direct-run-manifest.js';`,
    `${REFERENCE}\nimport './missing.js';`,
    `${REFERENCE}\nexport {value} from './missing.js';`,
    `${REFERENCE}\nimport 'node:fs';`,
    `${REFERENCE}\nconst path='./missing.js'; import(path);`,
    `${REFERENCE}\nimport('./direct-run-manifest.js');`,
  ])('rejects an incompatible reference module contract', async (source) => {
    const f = await fixture(source);
    await expect(prepare(f.configPath)).rejects.toThrow(/reference artifact/);
  });

  it.each([
    'export default {};',
    TENANT.replace('export class Runner', 'class Runner'),
    `${TENANT}\nimport './direct-run-manifest.js';`,
    `${TENANT}\nimport 'undeclared-package';`,
  ])('rejects an incompatible tenant module contract', async (source) => {
    const f = await fixture(REFERENCE, source);
    await expect(prepare(f.configPath)).rejects.toThrow(/tenant artifact/);
  });

  it('accepts named export aliases and the fixed emitted external modules', async () => {
    const f = await fixture(
      `import manifest from './direct-run-manifest.js'; import 'node:crypto'; const entry={fetch(){}}; export {entry as default};`,
      `import 'cloudflare:workers'; import 'node:async_hooks'; class A {} class B {} const entry={}; export {A as Maintenance, B as Runner, entry as default};`,
    );
    await expect(prepare(f.configPath)).resolves.toMatchObject({
      manifest: { fixtureVersion: 1 },
    });
  });

  it('checks generated manifest expansion against the combined upload ceiling', async () => {
    const f = await fixture(
      REFERENCE,
      `${TENANT}\n/*${'\\'.repeat(33 * 1024 * 1024)}*/`,
    );
    await expect(prepare(f.configPath)).rejects.toThrow(
      /reference upload size/,
    );
  }, 30_000);
});
