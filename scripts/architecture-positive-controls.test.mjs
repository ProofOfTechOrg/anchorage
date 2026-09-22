import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  existsSync,
  globSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { builtinModules, createRequire, isBuiltin } from 'node:module';
import { tmpdir } from 'node:os';
import { basename, dirname, join, relative } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const config = require('../.dependency-cruiser.cjs');
const architectureRules = [...config.forbidden, ...config.required];
const cli = fileURLToPath(
  new URL(
    '../node_modules/dependency-cruiser/bin/dependency-cruise.mjs',
    import.meta.url,
  ),
);
const configPath = fileURLToPath(
  new URL('../.dependency-cruiser.cjs', import.meta.url),
);
const erasedDependencyTypes = new Set(['type-only', 'type-import']);
const decommissionAdvance =
  'packages/fleet-control/src/decommission-advance.ts';
const decommissionDatabase =
  'packages/fleet-control/src/decommission-database.ts';
const backendSwitch = 'packages/fleet-control/src/backend-switch.ts';
const switchProvider =
  'packages/fleet-control/src/workers-for-platforms-backend-switch-provider.ts';
const databaseExportStore =
  'packages/fleet-control/src/database-export-store.ts';
const strictPlainData = 'packages/fleet-control/src/strict-plain-data.ts';
const auditAdvance = 'packages/fleet-control/src/fleet-audit-advance.ts';
const migrationAdvance =
  'packages/fleet-control/src/fleet-migration-advance.ts';
const fleet = 'packages/fleet-control/src/fleet.ts';
const cloudflareClient = 'packages/fleet-control/src/cloudflare-client.ts';
const d1Database = 'packages/fleet-control/src/d1-fleet-state-database.ts';
const fleetRequire = createRequire(
  new URL('../packages/fleet-control/package.json', import.meta.url),
);
const ts = fleetRequire('typescript');
const root = fileURLToPath(new URL('..', import.meta.url));
const directScenarioProjects = [
  'packages/fleet-control/vitest.direct-scenario.config.ts',
  'packages/fleet-control/vitest.direct-scenario-seams.config.ts',
];
const fleetControlProject = 'packages/fleet-control/vitest.config.ts';
const rootProjectPaths = [
  ...globSync('vitest.*.config.*', { cwd: root }).map((match) =>
    match.split('\\').join('/'),
  ),
  ...directScenarioProjects,
];
const packageProjectPaths = readdirSync(join(root, 'packages'), {
  withFileTypes: true,
})
  .filter((entry) => entry.isDirectory())
  .map((entry) => `packages/${entry.name}/vitest.config.ts`)
  .filter((projectPath) => existsSync(join(root, projectPath)));
const rootProjectNames = {
  [directScenarioProjects[0]]: 'fleet-control-direct-scenario',
  [directScenarioProjects[1]]: 'fleet-control-direct-scenario-seams',
  'vitest.breakwater-workers.config.mts': 'breakwater-workers',
  'vitest.flowsafe-harness.config.ts': 'flowsafe-harness',
  'vitest.flowsafe-workers.config.ts': 'flowsafe-workers',
  'vitest.workerd-lifecycle.config.ts': 'workerd-lifecycle',
};

function adjacencyOf(report, keep) {
  return new Map(
    report.modules.map((module) => [
      module.source,
      module.dependencies
        .filter(keep)
        .map((dependency) => dependency.resolved)
        .filter((resolved) => typeof resolved === 'string')
        .sort(),
    ]),
  );
}

function runtimeAdjacency(report) {
  return adjacencyOf(
    report,
    (dependency) =>
      !dependency.dependencyTypes.some((type) =>
        erasedDependencyTypes.has(type),
      ),
  );
}

function fullAdjacency(report) {
  return adjacencyOf(report, () => true);
}

function reaches(adjacency, source, target) {
  const visited = new Set();
  const pending = [source];
  while (pending.length > 0) {
    const current = pending.shift();
    if (current === target) return true;
    if (current === undefined || visited.has(current)) continue;
    visited.add(current);
    pending.push(...(adjacency.get(current) ?? []));
  }
  return false;
}

function reachableFrom(adjacency, source) {
  const reachable = new Set();
  const pending = [...(adjacency.get(source) ?? [])];
  while (pending.length > 0) {
    const current = pending.shift();
    if (current === undefined || reachable.has(current)) continue;
    reachable.add(current);
    pending.push(...(adjacency.get(current) ?? []));
  }
  return [...reachable].sort();
}

function hasCycleThrough(adjacency, source) {
  return (adjacency.get(source) ?? []).some((target) =>
    reaches(adjacency, target, source),
  );
}

const controls = {
  'fleet-control-worker-entry-avoids-node-host-adapters':
    'scripts/architecture-fixtures/control-plane-imports-node-host.ts',
  'fleet-control-worker-entry-limits-core-imports':
    'scripts/architecture-fixtures/control-plane-imports-forbidden-core.ts',
  'flowsafe-public-entry-no-agent-host':
    'scripts/architecture-fixtures/public-entry-imports-agent-host.ts',
  'flowsafe-public-entry-no-breakwater':
    'scripts/architecture-fixtures/public-entry-imports-breakwater.ts',
  'do-runner-approval-api-leaves-only':
    'scripts/architecture-fixtures/do-runner-imports-approval-router.ts',
  'host-kit-no-durable-agent':
    'scripts/architecture-fixtures/host-kit-imports-durable-agent.ts',
  'host-kit-no-breakwater':
    'scripts/architecture-fixtures/host-kit-imports-breakwater.ts',
  'flowsafe-architecture-resolves':
    'scripts/architecture-fixtures/unresolved-import.ts',
  'agent-starter-no-private-bare-entrypoints':
    'scripts/architecture-fixtures/starter-imports-private-entrypoint.ts',
  'agent-starter-no-relative-package-reaches':
    'scripts/architecture-fixtures/starter-reaches-flowsafe-source.ts',
  'fleet-control-is-control-plane-only':
    'scripts/architecture-fixtures/data-plane-imports-fleet-control.ts',
  'no-new-architecture-cycles': 'scripts/architecture-fixtures/cycle-a.ts',
  'host-kit-reaches-approval-bridge':
    'scripts/architecture-fixtures/host-kit-misses-approval-bridge.ts',
  'host-kit-reaches-approval-shapes':
    'scripts/architecture-fixtures/host-kit-misses-approval-shapes.ts',
  'fleet-control-client-layers-are-one-way':
    'scripts/architecture-fixtures/fleet-control-leaf-imports-client.ts',
  'fleet-control-decommission-state-does-not-reach-provider':
    'scripts/architecture-fixtures/decommission-state-imports-provider.ts',
  'fleet-control-cleanup-state-does-not-reach-provider':
    'scripts/architecture-fixtures/cleanup-state-imports-provider.ts',
  'fleet-control-inventory-state-does-not-reach-provider':
    'scripts/architecture-fixtures/inventory-state-imports-provider.ts',
  'fleet-control-operation-state-does-not-reach-provider':
    'scripts/architecture-fixtures/operation-state-imports-provider.ts',
  'fleet-control-decommission-advance-is-transport-neutral':
    'scripts/architecture-fixtures/decommission-advance-imports-provider.ts',
  'fleet-control-inventory-advance-is-transport-neutral':
    'scripts/architecture-fixtures/inventory-advance-imports-provider.ts',
  'fleet-control-operation-advance-avoids-concrete-transports':
    'scripts/architecture-fixtures/operation-advance-imports-provider.ts',
  'fleet-control-runtime-sdk-stays-in-provider-modules':
    'scripts/architecture-fixtures/runtime-sdk-import.ts',
  'fleet-control-cleanup-advance-is-transport-neutral':
    'scripts/architecture-fixtures/cleanup-advance-imports-provider.ts',
  'fleet-control-decommission-database-is-provider-neutral':
    'scripts/architecture-fixtures/decommission-database-imports-provider.ts',
  'fleet-control-backend-switch-does-not-reach-its-provider':
    'scripts/architecture-fixtures/decommission-database-imports-provider.ts',
  'fleet-control-strict-plain-data-is-import-free':
    'scripts/architecture-fixtures/decommission-state-imports-provider.ts',
  'fleet-control-ports-do-not-reach-d1-adapter':
    'scripts/architecture-fixtures/fleet-control-port-imports-d1-adapter.ts',
  'fleet-control-worker-reachable-modules-avoid-node-builtins':
    'scripts/architecture-fixtures/fleet-control-worker-reachable-imports-node-builtin.ts',
  'fleet-control-client-does-not-reach-its-consumers':
    'scripts/architecture-fixtures/fleet-control-client-imports-consumer.ts',
  'fleet-control-export-port-does-not-reach-adapters':
    'scripts/architecture-fixtures/fleet-control-export-port-imports-adapter.ts',
};

const extraEntries = {
  'fleet-control-decommission-advance-is-transport-neutral': [
    decommissionAdvance,
  ],
  'fleet-control-decommission-database-is-provider-neutral': [
    decommissionAdvance,
    decommissionDatabase,
    backendSwitch,
  ],
  'fleet-control-backend-switch-does-not-reach-its-provider': [
    decommissionAdvance,
    decommissionDatabase,
    backendSwitch,
  ],
  'fleet-control-operation-advance-avoids-concrete-transports': [
    auditAdvance,
    migrationAdvance,
  ],
};

const followedImports = new Map([
  [decommissionAdvance, decommissionDatabase],
  [decommissionDatabase, databaseExportStore],
  [backendSwitch, decommissionAdvance],
  [switchProvider, backendSwitch],
  [auditAdvance, fleet],
  [migrationAdvance, fleet],
]);

function assertFollowedImports(adjacency, sources) {
  for (const source of sources) {
    const target = followedImports.get(source);
    assert.ok(target, `no followed-import control for ${source}`);
    assert.ok(
      adjacency.get(source)?.includes(target),
      `${source} did not follow its import of ${target}`,
    );
    assert.ok(
      adjacency.get(target)?.length > 0,
      `${target}, imported by ${source}, has no followed dependencies`,
    );
  }
}

test('production transport class implementations are forbidden operation targets', () => {
  const sourceRoot = fileURLToPath(
    new URL('../packages/fleet-control/src/', import.meta.url),
  );
  const parsed = ts.getParsedCommandLineOfConfigFile(
    fileURLToPath(
      new URL('../packages/fleet-control/tsconfig.build.json', import.meta.url),
    ),
    { noEmit: true },
    {
      ...ts.sys,
      onUnRecoverableConfigFileDiagnostic(diagnostic) {
        assert.fail(
          ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'),
        );
      },
    },
  );
  assert.deepEqual(parsed.errors, []);
  const program = ts.createProgram(parsed.fileNames, parsed.options);
  const checker = program.getTypeChecker();
  const portSources = {
    ProvisioningBackend: 'types.ts',
    PlainWorkerProvisioningApi: 'types.ts',
    BackendSwitchProvider: 'backend-switch.ts',
    DurableDatabaseExportStore: 'database-export-store.ts',
    FleetStateDatabase: 'state-store.ts',
  };
  const ports = Object.entries(portSources).map(([name, file]) => {
    const source = program.getSourceFile(`${sourceRoot}${file}`);
    assert.ok(source, `missing port source ${file}`);
    const module = checker.getSymbolAtLocation(source);
    assert.ok(module, `missing module symbol for ${file}`);
    const exported = checker
      .getExportsOfModule(module)
      .find((candidate) => candidate.name === name);
    assert.ok(exported, `missing port ${name} in ${file}`);
    const symbol =
      exported.flags & ts.SymbolFlags.Alias
        ? checker.getAliasedSymbol(exported)
        : exported;
    const type = checker.getDeclaredTypeOfSymbol(symbol);
    assert.ok(
      type.getProperties().length > 0,
      `${name} has no resolved members`,
    );
    return { name, type };
  });
  const implementations = [];
  for (const source of program.getSourceFiles()) {
    if (source.isDeclarationFile || !source.fileName.startsWith(sourceRoot))
      continue;
    function visit(node) {
      if (ts.isClassDeclaration(node) || ts.isClassExpression(node)) {
        const symbol = checker.getTypeAtLocation(node).getSymbol();
        assert.ok(symbol, `unresolved class symbol in ${source.fileName}`);
        const type = checker.getDeclaredTypeOfSymbol(symbol);
        const implemented = ports.filter((port) =>
          checker.isTypeAssignableTo(type, port.type),
        );
        if (implemented.length > 0) {
          implementations.push({
            file: relative(root, source.fileName).split('\\').join('/'),
            class: node.name?.text ?? '<anonymous>',
            ports: implemented.map((port) => port.name),
          });
        }
      }
      ts.forEachChild(node, visit);
    }
    visit(source);
  }
  for (const { name } of ports) {
    assert.ok(
      implementations.some((implementation) =>
        implementation.ports.includes(name),
      ),
      `no production class implementation found for ${name}`,
    );
  }
  for (const name of [
    'CloudflareApiPlainWorkerBackend',
    'WranglerLoopBackend',
  ]) {
    assert.ok(
      implementations.some(
        (implementation) =>
          implementation.class === name &&
          implementation.ports.includes('ProvisioningBackend'),
      ),
      `inherited backend ${name} was not classified`,
    );
  }
  const forbidden = new RegExp(
    architectureRules.find(
      (rule) =>
        rule.name ===
        'fleet-control-operation-advance-avoids-concrete-transports',
    ).to.path,
  );
  assert.deepEqual(
    implementations.filter(
      (implementation) => !forbidden.test(implementation.file),
    ),
    [],
    'production transport class implementations missing from the operation rule',
  );
});

test('Worker control-plane core policy admits crypto and async_hooks', () => {
  const rule = architectureRules.find(
    (candidate) =>
      candidate.name === 'fleet-control-worker-entry-limits-core-imports',
  );
  assert.ok(rule);
  const entry = 'packages/fleet-control/src/cloudflare-control-plane.ts';
  for (const name of [
    'fleet-control-worker-entry-limits-core-imports',
    'fleet-control-worker-entry-avoids-node-host-adapters',
  ]) {
    const entryRule = architectureRules.find(
      (candidate) => candidate.name === name,
    );
    assert.ok(
      entryRule.from.path.some((pattern) => new RegExp(pattern).test(entry)),
      name,
    );
  }
  const layer = architectureRules.find(
    (candidate) => candidate.name === 'fleet-control-client-layers-are-one-way',
  );
  const reverse = architectureRules.find(
    (candidate) =>
      candidate.name === 'fleet-control-client-does-not-reach-its-consumers',
  );
  assert.equal(new RegExp(layer.from.pathNot).test(entry), true);
  assert.equal(new RegExp(reverse.to.path).test(entry), true);
  const forbidden = new RegExp(rule.to.path);
  for (const name of [
    'node:test',
    'node:test/reporters',
    'node:sea',
    'node:sqlite',
  ]) {
    assert.equal(isBuiltin(name), true, name);
    assert.equal(forbidden.test(name), true, name);
  }
  for (const name of ['node:crypto', 'node:async_hooks']) {
    assert.equal(forbidden.test(name), false, name);
  }
  for (const raw of builtinModules) {
    const name = raw.replace(/^node:/, '');
    assert.equal(
      forbidden.test(name),
      !['crypto', 'async_hooks'].includes(name),
      name,
    );
  }
});

test('every architecture rule has an executable positive control', () => {
  const ruleNames = architectureRules.map((rule) => rule.name).sort();
  assert.deepEqual(Object.keys(controls).sort(), ruleNames);
});

test('every extra cruise entry is keyed by an architecture rule', () => {
  const ruleNames = new Set(architectureRules.map((rule) => rule.name));
  for (const ruleName of Object.keys(extraEntries)) {
    assert.ok(
      ruleNames.has(ruleName),
      `extraEntries names '${ruleName}', which is not an architecture rule`,
    );
  }
});

// These controls read the config sources instead of loading the projects
// through vitest, which would pull the Workers pool and workerd into this
// process. Resolution is node's `globSync`, assumed to answer a `projects`
// entry and an `include` entry the way vitest's and tsc's own resolvers do;
// what the membership control below adds over vitest's startup error is the
// `deepEqual` over the resolved entries, which names a config no entry
// reaches.
const parse = (projectPath) => {
  const fileName = join(root, projectPath);
  return ts.createSourceFile(
    fileName,
    readFileSync(fileName, 'utf8'),
    ts.ScriptTarget.Latest,
    true,
  );
};
const initializerOf = (
  source,
  property,
  { recursive = true, required = true, label } = {},
) => {
  const found = [];
  const collect = (node) => {
    if (
      ts.isPropertyAssignment(node) &&
      ts.isIdentifier(node.name) &&
      node.name.text === property
    ) {
      found.push(node.initializer);
    }
  };
  const visit = (node) => {
    collect(node);
    ts.forEachChild(node, visit);
  };
  if (recursive) visit(source);
  else ts.forEachChild(source, collect);
  const sourceLabel =
    label ??
    relative(root, source.getSourceFile?.().fileName ?? source.fileName);
  if (required)
    assert.equal(found.length, 1, `${sourceLabel} sets '${property}' once`);
  else
    assert.ok(
      found.length <= 1,
      `${sourceLabel} sets '${property}' at most once`,
    );
  return found[0];
};
// The array element that is not a string literal and still leaves the parse
// complete: vitest's own default exclude list, spread into a package `exclude`.
const readableSpread = 'configDefaults.exclude';
const stringsOf = (source, property) => {
  const label = relative(root, source.fileName);
  const initializer = initializerOf(source, property);
  assert.ok(
    ts.isArrayLiteralExpression(initializer),
    `${label} sets '${property}' to an array literal`,
  );
  const strings = [];
  for (const element of initializer.elements) {
    if (ts.isStringLiteralLike(element)) {
      strings.push(element.text);
      continue;
    }
    assert.ok(
      ts.isSpreadElement(element) &&
        element.expression.getText(source) === readableSpread,
      `${label} sets '${property}' with an element this control cannot read: ${element.getText(source)}`,
    );
  }
  return strings;
};

test('the config parse refuses an array element it cannot read', () => {
  const synthetic = (elements) =>
    ts.createSourceFile(
      join(root, 'vitest.synthetic.config.ts'),
      `export default { test: { projects: [${elements}] } };`,
      ts.ScriptTarget.Latest,
      true,
    );
  for (const elements of [
    "...['packages/missing-*/vitest.config.ts']",
    "{ test: { name: 'inline' } }",
    'declaredElsewhere',
  ]) {
    assert.throws(
      () => stringsOf(synthetic(elements), 'projects'),
      /cannot read/,
      elements,
    );
  }
  assert.deepEqual(
    stringsOf(
      synthetic("...configDefaults.exclude, 'packages/*/vitest.config.ts'"),
      'projects',
    ),
    ['packages/*/vitest.config.ts'],
  );
});

test('the root vitest projects resolve to exactly the config files the repository holds', () => {
  const entries = stringsOf(parse('vitest.config.ts'), 'projects');
  const resolved = new Set();
  for (const entry of entries) {
    const matches = globSync(entry, { cwd: root });
    assert.ok(matches.length > 0, `root project '${entry}' resolves no file`);
    for (const match of matches) {
      resolved.add(match.split('\\').join('/'));
    }
  }
  assert.deepEqual(
    [...resolved].sort(),
    [...packageProjectPaths, ...rootProjectPaths].sort(),
  );
});

test('every root vitest project declares its expected name', () => {
  assert.deepEqual(
    Object.keys(rootProjectNames).sort(),
    [...rootProjectPaths].sort(),
  );
  for (const projectPath of rootProjectPaths) {
    assert.equal(
      explicitProjectName(projectPath),
      rootProjectNames[projectPath],
      projectPath,
    );
  }
});

test('the direct-scenario suites are declared by their projects and excluded from the package project', () => {
  const expectedIncludes = new Map([
    [
      directScenarioProjects[0],
      [
        'test/direct-credentialed-scenario.test.ts',
        'test/direct-reference-fence.harness.test.ts',
      ],
    ],
    [
      directScenarioProjects[1],
      ['test/direct-credentialed-scenario.seams.test.ts'],
    ],
  ]);
  const packageExclude = stringsOf(parse(fleetControlProject), 'exclude');
  for (const [project, expectedInclude] of expectedIncludes) {
    const directInclude = stringsOf(parse(project), 'include');
    assert.deepEqual([...directInclude].sort(), [...expectedInclude].sort());
    for (const entry of directInclude) {
      assert.ok(
        packageExclude.includes(entry),
        `the package project does not exclude '${entry}'`,
      );
    }
  }
});

test('every root vitest project include entry resolves to a file', () => {
  for (const projectPath of rootProjectPaths) {
    const projectRoot = dirname(join(root, projectPath));
    for (const entry of stringsOf(parse(projectPath), 'include')) {
      assert.ok(
        globSync(entry, { cwd: projectRoot }).length > 0,
        `${projectPath} include '${entry}' resolves no file`,
      );
    }
  }
});

test('every tsconfig.harness.json include entry resolves to a file', () => {
  const harnessPath = join(root, 'tsconfig.harness.json');
  const { config: harness, error } = ts.readConfigFile(harnessPath, (path) =>
    readFileSync(path, 'utf8'),
  );
  assert.equal(
    error,
    undefined,
    error && ts.flattenDiagnosticMessageText(error.messageText, '\n'),
  );
  assert.ok(
    Array.isArray(harness.include) && harness.include.length > 0,
    'tsconfig.harness.json declares no include entry',
  );
  for (const entry of harness.include) {
    assert.ok(
      globSync(entry, { cwd: root }).length > 0,
      `tsconfig.harness.json include '${entry}' resolves no file`,
    );
  }
});

const noShadowProbe = `const value = 1;
function probe() {
  const value = 2;
  return value;
}
console.log(value, probe());
`;

const trackedJavaScriptPaths = spawnSync(
  'git',
  [
    'ls-files',
    '-z',
    '--',
    '*.ts',
    '*.tsx',
    '*.mts',
    '*.cts',
    '*.mjs',
    '*.cjs',
    '*.js',
    '*.jsx',
  ],
  { cwd: root, encoding: 'utf8' },
)
  .stdout.split('\0')
  .filter(Boolean);

const trackedBiomeConfigs = spawnSync(
  'git',
  [
    'ls-files',
    '-z',
    '--',
    'biome.json',
    'biome.jsonc',
    '.biome.json',
    '.biome.jsonc',
    ':(glob)**/biome.json',
    ':(glob)**/biome.jsonc',
    ':(glob)**/.biome.json',
    ':(glob)**/.biome.jsonc',
  ],
  { cwd: root, encoding: 'utf8' },
)
  .stdout.split('\0')
  .filter((path) => path !== '' && path !== 'biome.json');

function assertNoShadowEnforced(
  biomeConfig,
  { extraConfigs = new Map(), label = 'repository config' } = {},
) {
  assert.equal(
    biomeConfig.linter?.enabled,
    true,
    `${label}: root linter.enabled is true`,
  );
  assert.equal(
    biomeConfig.linter?.rules?.suspicious?.noShadow,
    'error',
    `${label}: root suspicious.noShadow is error`,
  );
  assert.equal(
    biomeConfig.files?.maxSize,
    undefined,
    `${label}: root files.maxSize is absent`,
  );
  for (const [index, override] of (biomeConfig.overrides ?? []).entries()) {
    assert.equal(
      override.files?.maxSize,
      undefined,
      `${label}: override ${index} files.maxSize is absent`,
    );
  }

  const scratch = mkdtempSync(join(tmpdir(), 'anchorage-noshadow-'));
  try {
    writeFileSync(
      join(scratch, 'biome.json'),
      `${JSON.stringify(biomeConfig, null, 2)}\n`,
    );
    writeFileSync(
      join(scratch, '.gitignore'),
      readFileSync(join(root, '.gitignore')),
    );
    for (const trackedConfigPath of trackedBiomeConfigs) {
      const target = join(scratch, trackedConfigPath);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, readFileSync(join(root, trackedConfigPath)));
    }
    for (const [customConfigPath, contents] of extraConfigs) {
      const target = join(scratch, customConfigPath);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, `${JSON.stringify(contents, null, 2)}\n`);
    }
    for (const sourcePath of trackedJavaScriptPaths) {
      const target = join(scratch, sourcePath);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, noShadowProbe);
    }

    const biomeArguments = [
      'lint',
      '--reporter=json',
      '--max-diagnostics=none',
    ];
    biomeArguments.push('.');
    const run = spawnSync(
      join(root, 'node_modules/.bin/biome'),
      biomeArguments,
      { cwd: scratch, encoding: 'utf8' },
    );
    assert.equal(run.error, undefined, `${label}: Biome starts`);
    let report;
    assert.doesNotThrow(() => {
      report = JSON.parse(run.stdout);
    }, `${label}: Biome emits its JSON report`);
    const errorPaths = new Set(
      report.diagnostics
        .filter(
          (diagnostic) =>
            diagnostic.category === 'lint/suspicious/noShadow' &&
            diagnostic.severity === 'error',
        )
        .map((diagnostic) => diagnostic.location.path),
    );
    for (const sourcePath of trackedJavaScriptPaths) {
      assert.ok(
        errorPaths.has(sourcePath),
        `${label}: noShadow is an error for ${sourcePath}`,
      );
    }
  } finally {
    rmSync(scratch, { force: true, recursive: true });
  }
}

test('Biome enforces noShadow without an override bypass', () => {
  const biomeConfig = JSON.parse(
    readFileSync(join(root, 'biome.json'), 'utf8'),
  );
  assertNoShadowEnforced(biomeConfig);

  const exactPath = trackedJavaScriptPaths[0];
  const nestedDirectory = dirname(
    trackedJavaScriptPaths.find((path) => path.includes('/')),
  );
  for (const configName of [
    'biome.json',
    'biome.jsonc',
    '.biome.json',
    '.biome.jsonc',
  ]) {
    assertNoShadowEnforced(biomeConfig, {
      extraConfigs: new Map([
        [
          `${nestedDirectory}/${configName}`,
          {
            root: false,
            linter: { rules: { suspicious: { noShadow: 'off' } } },
          },
        ],
      ]),
      label: `root lint ignores nested ${configName} downgrade`,
    });
  }
  const bypasses = [
    {
      label: 'root linter disabled',
      mutate: (candidate) => {
        candidate.linter.enabled = false;
      },
      message: /root linter disabled: root linter\.enabled is true/u,
    },
    {
      label: 'root noShadow warning',
      mutate: (candidate) => {
        candidate.linter.rules.suspicious.noShadow = 'warn';
      },
      message: /root noShadow warning: root suspicious\.noShadow is error/u,
    },
    ...['on', 'warn', 'info', 'off'].map((severity) => ({
      label: `override suspicious ${severity}`,
      mutate: (candidate) => {
        candidate.overrides = [
          ...(candidate.overrides ?? []),
          {
            includes: ['**'],
            linter: { rules: { suspicious: severity } },
          },
        ];
      },
      message: new RegExp(
        `override suspicious ${severity}: noShadow is an error for`,
        'u',
      ),
    })),
    {
      label: 'override noShadow replaced',
      mutate: (candidate) => {
        candidate.overrides = [
          ...(candidate.overrides ?? []),
          {
            includes: ['**'],
            linter: { rules: { suspicious: { noShadow: 'off' } } },
          },
        ];
      },
      message: /override noShadow replaced: noShadow is an error for/u,
    },
    {
      label: 'override linter disabled',
      mutate: (candidate) => {
        candidate.overrides = [
          ...(candidate.overrides ?? []),
          { includes: ['**'], linter: { enabled: false } },
        ];
      },
      message: /override linter disabled: noShadow is an error for/u,
    },
    {
      label: 'override JavaScript linter disabled',
      mutate: (candidate) => {
        candidate.overrides = [
          ...(candidate.overrides ?? []),
          { includes: ['**'], javascript: { linter: { enabled: false } } },
        ];
      },
      message: /override JavaScript linter disabled: noShadow is an error for/u,
    },
    {
      label: 'exact filename override',
      mutate: (candidate) => {
        candidate.overrides = [
          ...(candidate.overrides ?? []),
          {
            includes: [exactPath],
            linter: { rules: { suspicious: { noShadow: 'off' } } },
          },
        ];
      },
      message: new RegExp(
        `exact filename override: noShadow is an error for ${exactPath.replaceAll('.', '\\.')}`,
        'u',
      ),
    },
    {
      label: 'root files includes narrowed',
      mutate: (candidate) => {
        candidate.files.includes = ['packages/**'];
      },
      message: /root files includes narrowed: noShadow is an error for/u,
    },
    {
      label: 'root linter includes narrowed',
      mutate: (candidate) => {
        candidate.linter.includes = ['packages/**'];
      },
      message: /root linter includes narrowed: noShadow is an error for/u,
    },
    {
      label: 'root files maxSize',
      mutate: (candidate) => {
        candidate.files.maxSize = 1;
      },
      message: /root files maxSize: root files\.maxSize is absent/u,
    },
    {
      label: 'override files maxSize',
      mutate: (candidate) => {
        candidate.overrides = [
          ...(candidate.overrides ?? []),
          { includes: ['**'], files: { maxSize: 1 } },
        ];
      },
      message: /override files maxSize: override \d+ files\.maxSize is absent/u,
    },
  ];
  assert.deepEqual(
    bypasses.map(({ label }) => label).sort(),
    [
      'exact filename override',
      'override JavaScript linter disabled',
      'override files maxSize',
      'override linter disabled',
      'override noShadow replaced',
      'override suspicious info',
      'override suspicious off',
      'override suspicious on',
      'override suspicious warn',
      'root files includes narrowed',
      'root files maxSize',
      'root linter disabled',
      'root linter includes narrowed',
      'root noShadow warning',
    ],
    'the noShadow bypass set is complete',
  );
  for (const { extraConfigs, label, message, mutate } of bypasses) {
    const candidateConfig = structuredClone(biomeConfig);
    mutate(candidateConfig);
    let bypassError;
    try {
      assertNoShadowEnforced(candidateConfig, {
        extraConfigs,
        label,
      });
    } catch (error) {
      bypassError = error;
    }
    assert.ok(bypassError, `${label}: bypass is rejected`);
    assert.match(
      bypassError.message,
      message,
      `${label}: bypass reports its probe failure`,
    );
  }
});

function explicitProjectName(projectPath, source = parse(projectPath)) {
  const project = initializerOf(source, 'test');
  assert.ok(
    ts.isObjectLiteralExpression(project),
    `${projectPath} sets 'test' to an object literal`,
  );
  const name = initializerOf(project, 'name', {
    label: projectPath,
    recursive: false,
    required: false,
  });
  if (name === undefined) return undefined;
  assert.ok(
    ts.isStringLiteralLike(name),
    `${projectPath} names its project with a string literal`,
  );
  return name.text;
}

function resolvedProjectName(projectPath) {
  const explicit = explicitProjectName(projectPath);
  if (explicit !== undefined) return explicit;
  const projectDirectory = dirname(join(root, projectPath));
  const manifestPath = join(projectDirectory, 'package.json');
  if (existsSync(manifestPath)) {
    const manifestName = JSON.parse(readFileSync(manifestPath, 'utf8')).name;
    if (typeof manifestName === 'string' && manifestName.length > 0) {
      return manifestName;
    }
  }
  return basename(projectDirectory);
}

const projectSelector =
  /(?:^|\s)--project(?:=|\s+)(?:"([^"]+)"|'([^']+)'|([^\s]+))/gu;

function selectedProjects(scripts) {
  const selections = [];
  for (const [script, command] of Object.entries(scripts)) {
    for (const match of command.matchAll(projectSelector)) {
      const selector = match
        .slice(1)
        .find((candidate) => candidate !== undefined);
      selections.push([script, selector.replace(/^!/u, '')]);
    }
  }
  return selections;
}

function assertProjectSelections(scripts, declared) {
  const selections = selectedProjects(scripts);
  assert.ok(
    selections.length > 0,
    'no root script selects a vitest project by name',
  );
  for (const [script, selectedProject] of selections) {
    const selected = selectedProject.endsWith('*')
      ? [...declared].some((project) =>
          project.startsWith(selectedProject.slice(0, -1)),
        )
      : declared.has(selectedProject);
    assert.ok(
      selected,
      `script '${script}' selects '${selectedProject}', absent from the discovered Vitest projects`,
    );
  }
  return selections;
}

test('root --project selections name discovered vitest projects', () => {
  const { scripts } = JSON.parse(
    readFileSync(join(root, 'package.json'), 'utf8'),
  );
  const declared = new Set(
    [...packageProjectPaths, ...rootProjectPaths].map(resolvedProjectName),
  );
  const fixtures = {
    double: 'vitest run --project="flowsafe-harness"',
    equals: 'vitest run --project=fleet-control-direct-scenario',
    negated: "vitest run --project '!fleet-control-direct-scenario'",
    package: "vitest run --project '@proofoftech/breakwater'",
  };
  const synthetic = ts.createSourceFile(
    join(root, 'vitest.synthetic.config.ts'),
    "export default { test: { plugins: [{ name: 'not-a-project' }] } };",
    ts.ScriptTarget.Latest,
    true,
  );
  assert.equal(
    explicitProjectName('vitest.synthetic.config.ts', synthetic),
    undefined,
  );

  assert.deepEqual(assertProjectSelections(fixtures, declared), [
    ['double', 'flowsafe-harness'],
    ['equals', 'fleet-control-direct-scenario'],
    ['negated', 'fleet-control-direct-scenario'],
    ['package', '@proofoftech/breakwater'],
  ]);
  assertProjectSelections(scripts, declared);
  for (const command of [
    'vitest run --project=missing-project',
    "vitest run --project '@proofoftech/missing'",
  ]) {
    assert.throws(
      () => assertProjectSelections({ invalid: command }, declared),
      /absent from the discovered Vitest projects/u,
    );
  }
});

for (const [directory, section] of [
  ['scripts', '## Contents'],
  ['packages/breakwater/src/agent', '# Guarded agent navigation'],
  ['packages/breakwater/src/audit', '# Audit navigation'],
]) {
  test(`the ${directory} index lists exactly the files beside it`, () => {
    const indexPath = `${directory}/CLAUDE.md`;
    const lines = readFileSync(join(root, indexPath), 'utf8').split('\n');
    const heading = lines.indexOf(section);
    assert.notEqual(heading, -1, `${indexPath} holds no '${section}' section`);
    const rest = lines.slice(heading + 1);
    const next = rest.findIndex((line) => line.startsWith('## '));
    const listed = rest
      .slice(0, next === -1 ? rest.length : next)
      .filter((line) => line.startsWith('- '))
      .map((line) => {
        const named = /^- \[?`([^`]+)`/u.exec(line);
        assert.ok(named, `${indexPath} lists an entry without a name: ${line}`);
        return named[1];
      });
    // The index covers files beside it, excluding subdirectories.
    const present = readdirSync(join(root, directory), { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name !== 'CLAUDE.md')
      .map((entry) => entry.name);
    assert.deepEqual([...listed].sort(), [...present].sort());
  });
}

for (const [ruleName, fixture] of Object.entries(controls)) {
  test(`${ruleName} rejects its positive control`, () => {
    const entries = [fixture, ...(extraEntries[ruleName] ?? [])];
    const args = [
      cli,
      '--config',
      configPath,
      '--output-type',
      'json',
      ...entries,
    ];
    const result = spawnSync(process.execPath, args, {
      cwd: root,
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
    });
    const command = [process.execPath, ...args].join(' ');
    if (result.error) {
      throw new Error(
        `failed to spawn ${JSON.stringify(command)}: ${result.error.message}`,
      );
    }
    if (result.stdout.trim() === '') {
      throw new Error(
        `${JSON.stringify(command)} produced no JSON output (status=${String(result.status)}, signal=${String(result.signal)}); stderr=${JSON.stringify(result.stderr.slice(0, 300))}`,
      );
    }

    assert.equal(result.signal, null, result.stderr);
    assert.equal(result.status, 0, result.stderr);
    const report = JSON.parse(result.stdout);
    assert.ok(report.summary.error > 0, `${fixture} unexpectedly passed`);
    const violations = report.summary.violations.map(
      (violation) => violation.rule.name,
    );
    assert.ok(
      violations.includes(ruleName),
      `${fixture} did not trigger ${ruleName}; got ${violations.join(', ')}`,
    );
    const adjacency = fullAdjacency(report);
    assertFollowedImports(adjacency, extraEntries[ruleName] ?? []);
    if (
      [
        'fleet-control-decommission-advance-is-transport-neutral',
        'fleet-control-inventory-advance-is-transport-neutral',
        'fleet-control-operation-advance-avoids-concrete-transports',
        'fleet-control-cleanup-advance-is-transport-neutral',
      ].includes(ruleName)
    ) {
      assert.ok(
        report.summary.violations.some(
          (violation) =>
            violation.rule.name === ruleName &&
            violation.from === fixture &&
            violation.to === d1Database,
        ),
        `${fixture} did not reject the concrete D1 database adapter`,
      );
      assert.equal(reaches(adjacency, fixture, d1Database), true);
    }
    if (
      ruleName === 'fleet-control-decommission-state-does-not-reach-provider'
    ) {
      assert.ok(
        report.summary.violations.some(
          (violation) =>
            violation.rule.name === ruleName && violation.to === 'cloudflare',
        ),
        'decommission state control did not reject a direct Cloudflare SDK import',
      );
    }
    if (
      ruleName === 'fleet-control-decommission-advance-is-transport-neutral'
    ) {
      assert.deepEqual([...new Set(violations)].sort(), [ruleName]);
      assert.ok(
        report.summary.violations.some(
          (violation) =>
            violation.rule.name === ruleName && violation.to === switchProvider,
        ),
        'decommission advance control did not reject the concrete switch provider',
      );
      assert.equal(reaches(adjacency, fixture, backendSwitch), true);
      assert.equal(reaches(adjacency, fixture, switchProvider), true);
      assertFollowedImports(adjacency, [backendSwitch, switchProvider]);
      assert.equal(
        reaches(adjacency, decommissionAdvance, backendSwitch),
        false,
      );
      assert.equal(
        reaches(adjacency, decommissionAdvance, switchProvider),
        false,
      );
    }
    if (
      ruleName === 'fleet-control-decommission-database-is-provider-neutral'
    ) {
      assert.deepEqual([...new Set(violations)].sort(), [
        'fleet-control-backend-switch-does-not-reach-its-provider',
        'fleet-control-decommission-database-is-provider-neutral',
      ]);
      for (const target of [
        'packages/fleet-control/src/cloudflare-client.ts',
        switchProvider,
      ]) {
        assert.ok(
          report.summary.violations.some(
            (violation) =>
              violation.rule.name === ruleName && violation.to === target,
          ),
          `decommission database control did not reject ${target}`,
        );
      }
      assert.equal(adjacency.get(fixture)?.includes(backendSwitch), true);
      const runtime = runtimeAdjacency(report);
      assert.deepEqual(reachableFrom(runtime, decommissionDatabase), [
        databaseExportStore,
        strictPlainData,
      ]);
      assert.equal(
        runtime.get(fixture)?.includes(backendSwitch) ?? false,
        false,
        'erased fixture edge entered the runtime adjacency map',
      );
    }
    if (
      ruleName === 'fleet-control-backend-switch-does-not-reach-its-provider'
    ) {
      assert.deepEqual([...new Set(violations)].sort(), [
        'fleet-control-backend-switch-does-not-reach-its-provider',
        'fleet-control-decommission-database-is-provider-neutral',
      ]);
      assert.equal(reaches(adjacency, fixture, backendSwitch), true);
      assert.equal(reaches(adjacency, fixture, switchProvider), true);
      assertFollowedImports(adjacency, [switchProvider]);
      assert.equal(reaches(adjacency, backendSwitch, switchProvider), false);
      for (const source of [
        decommissionAdvance,
        decommissionDatabase,
        backendSwitch,
        switchProvider,
      ]) {
        assert.equal(
          hasCycleThrough(adjacency, source),
          false,
          `${source} entered a type-inclusive dependency cycle`,
        );
      }
    }
    if (
      ruleName === 'fleet-control-operation-advance-avoids-concrete-transports'
    ) {
      assert.deepEqual([...new Set(violations)].sort(), [ruleName]);
      assert.ok(
        report.summary.violations.some(
          (violation) =>
            violation.rule.name === ruleName &&
            violation.to === cloudflareClient,
        ),
        'operation advance control did not reject the concrete provider client',
      );
      assert.equal(reaches(adjacency, fixture, cloudflareClient), true);
      const forbidden = new RegExp(
        architectureRules.find((rule) => rule.name === ruleName).to.path,
      );
      for (const source of [auditAdvance, migrationAdvance]) {
        assert.deepEqual(
          reachableFrom(adjacency, source).filter((module) =>
            forbidden.test(module),
          ),
          [],
          `${source} reached a forbidden target`,
        );
      }
    }
    if (ruleName === 'fleet-control-strict-plain-data-is-import-free') {
      for (const target of ['cloudflare', 'crypto']) {
        assert.ok(
          report.summary.violations.some(
            (violation) =>
              violation.rule.name === ruleName && violation.to === target,
          ),
          `strict plain-data control did not reject ${target}`,
        );
      }
    }
    if (ruleName === 'fleet-control-worker-entry-limits-core-imports') {
      for (const target of [
        'fs/promises',
        'buffer',
        'node:test',
        'node:test/reporters',
        'node:sea',
        'node:sqlite',
      ]) {
        assert.ok(
          report.summary.violations.some(
            (violation) =>
              violation.rule.name === ruleName && violation.to === target,
          ),
        );
      }
      for (const allowed of ['crypto', 'async_hooks']) {
        assert.equal(
          report.summary.violations.some(
            (violation) =>
              violation.rule.name === ruleName && violation.to === allowed,
          ),
          false,
        );
      }
    }
    if (ruleName === 'fleet-control-worker-entry-avoids-node-host-adapters') {
      assert.ok(
        report.summary.violations.some(
          (violation) =>
            violation.rule.name === ruleName &&
            violation.to === 'packages/fleet-control/src/export-store.ts',
        ),
      );
    }
    if (ruleName === 'flowsafe-public-entry-no-breakwater') {
      const entry = report.modules.find((module) => module.source === fixture);
      assert.deepEqual(
        entry.dependencies.map((dependency) => dependency.module).sort(),
        ['@proofoftech/breakwater/agent', '@proofoftech/breakwater/rbac'],
      );
    }
  });
}
