import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
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
const cloudflareClient = 'packages/fleet-control/src/cloudflare-client.ts';

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

/** Adjacency over runtime edges only; type-only edges are erased. */
function runtimeAdjacency(report) {
  return adjacencyOf(
    report,
    (dependency) =>
      !dependency.dependencyTypes.some((type) =>
        erasedDependencyTypes.has(type),
      ),
  );
}

/** Adjacency over every edge, which is the graph a reachable rule walks. */
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

/**
 * Real modules cruised alongside a fixture, for two reasons. A reachable rule
 * is evaluated over the graph it guards rather than over the fixture alone;
 * and a block's own assertions get the modules they walk, which is why the
 * decommission-database entry lists decommissionDatabase — that rule is a
 * direct-edge rule, not a reachable one, so the module is there for the
 * block's exact-set assertion rather than for the rule.
 */
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
  'fleet-control-operation-advance-avoids-concrete-transports': [auditAdvance],
};

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
      cwd: fileURLToPath(new URL('..', import.meta.url)),
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
    // An extra entry the cruise report does not contain has an empty
    // adjacency entry, which satisfies the negative reachability assertions
    // below without proving anything.
    for (const entry of extraEntries[ruleName] ?? []) {
      assert.ok(
        report.modules.some((module) => module.source === entry),
        `extraEntries lists ${entry} for ${ruleName}, which the cruise report does not contain`,
      );
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
      const adjacency = fullAdjacency(report);
      assert.equal(
        reaches(adjacency, decommissionAdvance, backendSwitch),
        false,
      );
      assert.equal(
        reaches(adjacency, decommissionAdvance, switchProvider),
        false,
      );
      assert.equal(reaches(adjacency, fixture, backendSwitch), true);
      assert.equal(reaches(adjacency, fixture, switchProvider), true);
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
      const adjacency = runtimeAdjacency(report);
      assert.deepEqual(reachableFrom(adjacency, decommissionDatabase), [
        databaseExportStore,
        strictPlainData,
      ]);
      assert.equal(
        adjacency.get(fixture)?.includes(backendSwitch) ?? false,
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
      assert.equal(
        reaches(fullAdjacency(report), backendSwitch, switchProvider),
        false,
      );
      const adjacency = runtimeAdjacency(report);
      for (const source of [
        decommissionAdvance,
        decommissionDatabase,
        backendSwitch,
        switchProvider,
      ]) {
        assert.equal(
          hasCycleThrough(adjacency, source),
          false,
          `${source} entered a runtime cycle`,
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
      // Exhaustive over the rule's own to-set rather than over one member, so
      // a real-module reach into any other forbidden target cannot hide behind
      // this fixture's violations under the same rule name. The walk is over
      // the unfiltered graph, type-only edges included, because that is the
      // graph this reachable rule itself walks under tsPreCompilationDeps.
      const forbidden = new RegExp(
        architectureRules.find((rule) => rule.name === ruleName).to.path,
      );
      assert.deepEqual(
        reachableFrom(fullAdjacency(report), auditAdvance).filter((module) =>
          forbidden.test(module),
        ),
        [],
        'the real operation-advance coordinator reached a forbidden target',
      );
      assert.equal(
        reaches(runtimeAdjacency(report), fixture, cloudflareClient),
        true,
      );
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
    if (ruleName === 'flowsafe-public-entry-no-breakwater') {
      const entry = report.modules.find((module) => module.source === fixture);
      assert.deepEqual(
        entry.dependencies.map((dependency) => dependency.module).sort(),
        ['@proofoftech/breakwater/agent', '@proofoftech/breakwater/rbac'],
      );
    }
  });
}
