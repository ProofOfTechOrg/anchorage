// SPDX-License-Identifier: Apache-2.0

/**
 * Prove that the operator configuration `packages/agent-starter` ships is one
 * `pnpm fleet-control:credentialed` will actually accept.
 *
 * This lives at the repository root rather than inside either package because
 * `.dependency-cruiser.cjs`'s `fleet-control-is-control-plane-only` rule
 * forbids anything under `packages/` from reaching fleet control, tests
 * included. Root scripts are outside that boundary, so this is the only place
 * both sides can be compared against fleet control's own validators.
 *
 * It covers two distinct failure modes the gate would otherwise only find
 * against a paid account:
 *   - structural: `validateConformanceConfig`, the runner's stage one
 *   - operational: `validateDeploymentSpec`, the runner's stage two, which
 *     rejects tenant-tag shapes stage one never inspects
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  CONFORMANCE_CONFIG_PATH,
  conformanceConfigIsCurrent,
} from '../packages/agent-starter/scripts/emit-conformance-config.mjs';
import { validateConformanceConfig } from '../packages/fleet-control/scripts/credentialed-conformance-config.mjs';

const config = JSON.parse(readFileSync(CONFORMANCE_CONFIG_PATH, 'utf8'));
const contract = JSON.parse(
  readFileSync(
    fileURLToPath(
      new URL(
        '../packages/agent-starter/src/conformance/contract.json',
        import.meta.url,
      ),
    ),
    'utf8',
  ),
);

test('the committed operator config matches the contract it is rendered from', () => {
  assert.ok(
    conformanceConfigIsCurrent(readFileSync(CONFORMANCE_CONFIG_PATH, 'utf8')),
    'run pnpm --filter anchorage-agent-starter conformance:config',
  );
});

test('fleet control accepts the shipped operator config', () => {
  assert.deepEqual(validateConformanceConfig(structuredClone(config)), config);
});

test('the artifacts and the config name the same bindings', () => {
  assert.equal(config.mainModule, contract.candidateMainModule);
  assert.equal(
    config.applicationSecretBinding,
    contract.applicationSecretBinding,
  );
  assert.equal(config.conformance.httpPath, contract.httpPath);
  assert.equal(config.conformance.webSocketPath, contract.webSocketPath);
  assert.deepEqual(
    config.application.r2Buckets.map((bucket) => bucket.name),
    [contract.applicationR2Binding],
  );
  assert.deepEqual(
    config.durableObjectBindings,
    contract.durableObjectBindings,
  );
  assert.deepEqual(
    config.conformance.newDurableObjectBinding,
    contract.newDurableObjectBinding,
  );
  for (const profile of config.platformProfile.stateProfiles) {
    assert.equal(profile.stateWorker.mainModule, contract.stateMainModule);
  }
});

test('every Durable Object class the state artifacts export is created by a migration', () => {
  const [v1, v2] = config.platformProfile.stateProfiles;
  const created = (profile) =>
    profile.stateDurableObjectMigrations.flatMap((migration) => [
      ...(migration.newClasses ?? []),
      ...(migration.newSqliteClasses ?? []),
    ]);
  const v1Classes = created(v1);
  assert.ok(v1Classes.includes(contract.auditProxyClassName));
  for (const binding of contract.durableObjectBindings) {
    assert.ok(
      v1Classes.includes(binding.className),
      `v1 migrations do not create ${binding.className}`,
    );
  }
  assert.deepEqual(created(v2), [
    ...v1Classes,
    contract.newDurableObjectBinding.className,
  ]);
});

/**
 * A comment-and-string-aware stripper, not a regex: the wrangler configurations
 * carry `//` comments containing quotes and paths, and a naive strip corrupts
 * them silently. Anything it gets wrong surfaces immediately as a JSON.parse
 * failure rather than as a passing check over garbage.
 */
function parseJsonc(text) {
  let out = '';
  let index = 0;
  while (index < text.length) {
    const char = text[index];
    if (char === '"') {
      out += char;
      index += 1;
      while (index < text.length) {
        out += text[index];
        if (text[index] === '\\') {
          out += text[index + 1] ?? '';
          index += 2;
          continue;
        }
        if (text[index] === '"') {
          index += 1;
          break;
        }
        index += 1;
      }
      continue;
    }
    if (char === '/' && text[index + 1] === '/') {
      while (index < text.length && text[index] !== '\n') index += 1;
      continue;
    }
    if (char === '/' && text[index + 1] === '*') {
      const close = text.indexOf('*/', index + 2);
      index = close === -1 ? text.length : close + 2;
      continue;
    }
    out += char;
    index += 1;
  }
  // Trailing commas are legal jsonc — Cloudflare's own wrangler samples use
  // them — and JSON.parse is not. Stripped after comment removal so a comment
  // sitting between a comma and its closing brace cannot hide one.
  return JSON.parse(out.replace(/,(\s*[}\]])/gu, '$1'));
}

function wranglerConfig(name) {
  return parseJsonc(
    readFileSync(
      fileURLToPath(
        new URL(
          `../packages/agent-starter/conformance/${name}`,
          import.meta.url,
        ),
      ),
      'utf8',
    ),
  );
}

function migrationClasses(migrations) {
  return migrations.flatMap((migration) => [
    ...(migration.new_classes ?? []),
    ...(migration.new_sqlite_classes ?? []),
  ]);
}

/**
 * The wrangler configurations are what the local harness actually runs, and
 * their class lists are hand-written copies of the contract's. Without this the
 * harness can drift away from the artifact it exists to prove, and the first
 * signal would be a paid run.
 */
test('the harness wrangler configurations match the contract', () => {
  const stateClasses = [
    ...contract.durableObjectBindings.map((binding) => binding.className),
    contract.auditProxyClassName,
  ];

  for (const [name, expected] of [
    ['wrangler.state-v1.jsonc', stateClasses],
    [
      'wrangler.state-v2.jsonc',
      [...stateClasses, contract.newDurableObjectBinding.className],
    ],
  ]) {
    const config = wranglerConfig(name);
    assert.deepEqual(
      migrationClasses(config.migrations).sort(),
      [...expected].sort(),
      `${name} migrations do not match the contract`,
    );
    assert.deepEqual(
      config.durable_objects.bindings
        .map((binding) => binding.class_name)
        .sort(),
      [...expected].sort(),
      `${name} Durable Object bindings do not match the contract`,
    );
    assert.equal(config.compatibility_date, contract.compatibilityDate);
  }

  // The candidate owns no class and binds only into the state script; release
  // two adds exactly the one binding the contract's appended migration creates.
  const candidate = wranglerConfig('wrangler.candidate.jsonc');
  const candidateV2 = wranglerConfig('wrangler.candidate-v2.jsonc');
  assert.equal(candidate.migrations, undefined);
  assert.equal(candidateV2.migrations, undefined);
  const candidateBindings = candidate.durable_objects.bindings.map(
    (binding) => binding.name,
  );
  assert.deepEqual(
    candidateV2.durable_objects.bindings
      .map((binding) => binding.name)
      .filter((name) => !candidateBindings.includes(name)),
    [contract.newDurableObjectBinding.name],
  );
  for (const binding of candidateV2.durable_objects.bindings) {
    assert.ok(
      binding.script_name,
      `candidate binding ${binding.name} must resolve to the state script`,
    );
  }
  // Pin the harness candidate's binding set to what ConformanceCandidateEnv
  // declares. Without this, an artifact that grew a reach into a binding the
  // candidate is not supposed to use — RUNNER, say — would still run green
  // locally, because the harness config simply would not bind it.
  const candidateInterface = readFileSync(
    fileURLToPath(
      new URL(
        '../packages/agent-starter/src/conformance/env.ts',
        import.meta.url,
      ),
    ),
    'utf8',
  ).split('export interface ConformanceCandidateEnv {')[1];
  assert.ok(
    candidateInterface,
    'ConformanceCandidateEnv is no longer declared',
  );
  const declaredCandidateBindings = [
    ...candidateInterface
      .split('\n}')[0]
      .matchAll(/readonly ([A-Z][A-Z0-9_]*)\??: NamespaceLike/gu),
  ].map((match) => match[1]);
  assert.ok(
    declaredCandidateBindings.length > 0,
    'no candidate Durable Object bindings were found to compare',
  );
  assert.deepEqual(
    candidateV2.durable_objects.bindings.map((binding) => binding.name).sort(),
    [...declaredCandidateBindings].sort(),
    'the harness candidate binds a different set than ConformanceCandidateEnv declares',
  );

  assert.equal(candidate.r2_buckets[0].binding, contract.applicationR2Binding);
  assert.equal(
    candidate.vars[contract.applicationVariableName],
    contract.applicationVariableValue,
  );
});

/**
 * The candidate signs its Durable Object calls with `DEPLOYMENT_IDENTITY_SECRET`
 * and the trusted state verifies with its own copy, so the four harness
 * configurations must carry byte-equal fixtures. Passing them with `--var`
 * cannot enforce it — multi-worker `wrangler dev` spreads those flags into the
 * primary configuration only — so the equality is asserted here, where all four
 * files are already parsed.
 */
test('the harness fixtures are equal across every configuration', () => {
  const configs = [
    'wrangler.candidate.jsonc',
    'wrangler.candidate-v2.jsonc',
    'wrangler.state-v1.jsonc',
    'wrangler.state-v2.jsonc',
  ].map((name) => [name, wranglerConfig(name)]);

  for (const field of ['DEPLOYMENT_IDENTITY_SECRET', 'DEPLOYMENT_TENANT']) {
    const values = configs
      .filter(([, config]) => config.vars?.[field] !== undefined)
      .map(([name, config]) => [name, config.vars[field]]);
    assert.ok(
      values.length === configs.length,
      `${field} is absent from ${configs.length - values.length} configuration(s)`,
    );
    const [[, expected]] = values;
    for (const [name, value] of values) {
      assert.equal(value, expected, `${name} carries a different ${field}`);
    }
  }
});

/**
 * Stage two runs production validators over specs the runner derives from this
 * config. A tenant tag with a hyphen passes stage one and is rejected here, so
 * the shape check has to happen against the real validator, not a copy of the
 * pattern.
 */
test('the derived deployment specs survive production validation', async () => {
  const { validateDeploymentSpec } = await import(
    '../packages/fleet-control/dist/validation.js'
  );
  for (const tenantTag of config.tenantTags) {
    const scriptName = `conformance-${tenantTag}-abc123`;
    validateDeploymentSpec({
      tenantTag,
      environment: config.environment,
      scriptName,
      databaseName: scriptName,
      compatibilityDate: config.compatibilityDate,
      compatibilityFlags: config.compatibilityFlags,
      mainModule: config.mainModule,
      modules: [
        {
          name: config.mainModule,
          content: 'export default {}',
          contentType: 'application/javascript+module',
        },
      ],
      authoredBy: 'external',
      schemaVersion: config.schemaVersion,
      migrations: config.migrations,
      durableObjectMigrations: [],
      durableObjectBindings: config.durableObjectBindings,
      application: {
        vars: config.application.vars,
        secrets: [
          {
            name: config.applicationSecretBinding,
            valueSha256: 'a'.repeat(64),
          },
        ],
        r2Buckets: config.application.r2Buckets,
      },
      queueProducer: {
        binding: 'AUDIT_QUEUE',
        queueName: config.auditQueueName,
      },
      maintenanceBaseUrl: config.maintenanceBaseUrls[tenantTag],
      routeHostname: config.routeHostnames[tenantTag],
      cpuLimitMs: config.cpuLimitMs,
      subrequestLimit: config.subrequestLimit,
    });
  }
});
