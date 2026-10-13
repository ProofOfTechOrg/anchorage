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
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
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
const flowsafeRequire = createRequire(
  new URL('../packages/flowsafe/package.json', import.meta.url),
);
const ts = flowsafeRequire('typescript');
const root = fileURLToPath(new URL('..', import.meta.url));
const rootProjectPaths = [
  ...globSync('vitest.*.config.*', { cwd: root }).map((match) =>
    match.split('\\').join('/'),
  ),
];
const packageProjectPaths = readdirSync(join(root, 'packages'), {
  withFileTypes: true,
})
  .filter((entry) => entry.isDirectory())
  .map((entry) => `packages/${entry.name}/vitest.config.ts`)
  .filter((projectPath) => existsSync(join(root, projectPath)));
const controls = {
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
  'no-new-architecture-cycles': 'scripts/architecture-fixtures/cycle-a.ts',
  'host-kit-reaches-approval-bridge':
    'scripts/architecture-fixtures/host-kit-misses-approval-bridge.ts',
  'host-kit-reaches-approval-shapes':
    'scripts/architecture-fixtures/host-kit-misses-approval-shapes.ts',
};

test('every architecture rule has an executable positive control', () => {
  const ruleNames = architectureRules.map((rule) => rule.name).sort();
  assert.deepEqual(Object.keys(controls).sort(), ruleNames);
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

for (const [ruleName, fixture] of Object.entries(controls)) {
  test(`${ruleName} rejects its positive control`, () => {
    const entries = [fixture];
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
    if (ruleName === 'flowsafe-public-entry-no-breakwater') {
      const entry = report.modules.find((module) => module.source === fixture);
      assert.deepEqual(
        entry.dependencies.map((dependency) => dependency.module).sort(),
        ['@proofoftech/breakwater/agent', '@proofoftech/breakwater/rbac'],
      );
    }
  });
}
