// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { isDeepStrictEqual } from 'node:util';
import { DirectInvocationError } from './direct-credentialed-invocation.mjs';
import {
  observeDirectWorkerVersion,
  readDirectSettlementEffects,
  verifyDirectDecommissionExport,
} from './direct-credentialed-observations.mjs';
import {
  actionSummary,
  DIRECT_SCENARIO_ARRAY_MAXIMA,
  DIRECT_SCENARIO_FAILURE_DETAILS,
  DIRECT_SCENARIO_FAILURES,
  DirectRunStateError,
  isForceIdentity,
} from './direct-credentialed-run-state.mjs';
import {
  DIRECT_SCENARIO_MIN_INVOCATIONS,
  DIRECT_SCENARIO_PHASES,
} from './direct-credentialed-scenario-budget.mjs';
import {
  changedBy,
  checkFootprint,
  checkInterruptedItems,
  checkInterruptionWitness,
  checkInvocationHeadroom,
  checkItemConvergence,
  checkTrafficDistribution,
  equal,
  expectedVersion,
  hash,
  jsonHash,
  migrationInterruptSettled,
  migrationStartSettled,
  NORMAL_ROLES,
  operationFacts,
  parse,
  recordFacts,
  requireFact,
  SCENARIO_ROLES,
  zeroAttempts,
} from './direct-credentialed-scenario-checks.mjs';
import { DIRECT_TENANT_OBJECT_BODY } from './direct-credentialed-tenant-object.mjs';

const busy = new WeakSet();
const objectDigest = hash(DIRECT_TENANT_OBJECT_BODY);
const objectSize = Buffer.byteLength(DIRECT_TENANT_OBJECT_BODY);
const failureCodes = new Set(DIRECT_SCENARIO_FAILURES);
const failureDetails = new Set(DIRECT_SCENARIO_FAILURE_DETAILS);

async function processIdentity() {
  const stat = await readFile('/proc/self/stat', 'utf8');
  return {
    pid: process.pid,
    startTicks: stat.slice(stat.lastIndexOf(')') + 2).split(' ')[19],
    bootId: (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim(),
  };
}
function initialState(ordinal) {
  return {
    version: 1,
    phase: 'provision-a',
    startedOrdinal: ordinal,
    callCount: 0,
    phaseCalls: Object.fromEntries(
      DIRECT_SCENARIO_PHASES.map((phase) => [phase, 0]),
    ),
    attempts: zeroAttempts(),
    sdkRequests: 0,
    inventoryCalls: { before: 0, after: 0 },
    lastCall: null,
    mutation: null,
    reconciledOrdinal: ordinal,
    operations: [],
    records: [],
    failure: null,
    proofs: {
      initial: { a: null, b: null, recovery: null },
      candidate: { a: null, b: null },
      final: { a: null, b: null },
      objects: { a: null, b: null },
      objectDeletions: { a: null, b: null },
      recoveryExportAbsent: { beforeOrdinal: null, afterOrdinal: null },
      health: [],
      inventories: { before: null, after: null },
      audits: { before: null, after: null },
      fence: {
        drain: { a: null, b: null },
        sweeps: { a: null, b: null },
        reopen: { a: null, b: null },
        probes: { a: null, b: null },
      },
      restart: null,
      steps: [],
      effects: [],
      cleanup: null,
      exports: { a: null, b: null },
      exportVerifications: [],
      decommission: { a: null, b: null },
      terminalForce: { a: null },
      force: null,
      residual: null,
    },
  };
}
export async function runDirectCredentialedScenario(input) {
  const { prepared, journal, invocation, apiToken } = input;
  let state;
  let control;
  let acquired = false;
  const persist = async () => {
    await journal.recordScenario(state);
    state = structuredClone(journal.snapshot().scenario);
  };
  const advancePhase = async () => {
    await sync();
    state.phase =
      DIRECT_SCENARIO_PHASES[DIRECT_SCENARIO_PHASES.indexOf(state.phase) + 1];
    state.mutation = null;
    await persist();
  };
  const invoke = async (action, mutates = false, migration = null) => {
    const snapshot = journal.snapshot();
    requireFact(
      snapshot.lastInvocation?.state !== 'pending' &&
        !snapshot.bootstrap?.pending,
      'outcome-unknown',
    );
    const remaining =
      snapshot.binding.maxInvocations - snapshot.invocationCount;
    checkInvocationHeadroom(state.phase, state.phaseCalls, remaining);
    const call = {
      ordinal: snapshot.invocationCount + 1,
      action: actionSummary(action),
      outcome: 'prepared',
      attempts: null,
      migration,
    };
    state.lastCall = call;
    if (mutates) state.mutation = call;
    await persist();
    let response;
    let error;
    try {
      response = await invocation.invoke(action);
    } catch (caught) {
      error = caught;
    }
    const detail =
      error instanceof DirectInvocationError && failureDetails.has(error.detail)
        ? error.detail
        : undefined;
    const after = journal.snapshot();
    requireFact(
      after.invocationCount === call.ordinal &&
        after.lastInvocation?.state === 'settled',
      'outcome-unknown',
      detail,
    );
    if (error)
      requireFact(
        error instanceof DirectInvocationError &&
          error.attempts &&
          ['injected-response-loss', 'reference-refused'].includes(error.code),
        'outcome-unknown',
        detail ?? 'non-contract-answer',
      );
    const attempts = error ? error.attempts : response.attempts;
    const settled = {
      ...call,
      outcome: error ? error.code : 'returned',
      attempts,
    };
    if (action.kind === 'force-terminal') {
      const before = error ? null : response.result?.before;
      requireFact(isForceIdentity(before), 'observation-mismatch');
      settled.before = before;
    }
    state.lastCall = settled;
    if (mutates) state.mutation = settled;
    state.callCount++;
    state.phaseCalls[state.phase]++;
    for (const key of Object.keys(attempts))
      state.attempts[key] += attempts[key];
    if (
      action.kind === 'inventory-start' ||
      action.kind === 'inventory-continue'
    )
      state.inventoryCalls[
        action.slot === 'inventory-before' ? 'before' : 'after'
      ]++;
    await persist();
    if (error) throw error;
    return response.result;
  };
  const sync = async () => {
    const fresh = await invoke({ kind: 'control-read' });
    const snapshot = journal.snapshot();
    const bootstrap = snapshot.bootstrap;
    equal(fresh.binding, {
      version: 1,
      accountId: snapshot.binding.accountId,
      fleetDatabaseId: bootstrap.fleet.uuid,
      quotaDatabaseId: bootstrap.quota.uuid,
      exportBucketName: bootstrap.exports.name,
      referenceModuleSetSha256: prepared.referenceModuleSetSha256,
      accountWorkersDevSubdomain: bootstrap.context.accountWorkersDevSubdomain,
    });
    requireFact(
      Array.isArray(fresh.operations) && Array.isArray(fresh.records),
    );
    const allowed = changedBy(
      state.mutation?.outcome === 'prepared' ||
        (state.mutation?.ordinal ?? 0) <= state.reconciledOrdinal
        ? null
        : state.mutation?.action,
    );
    const operations = fresh.operations.map(operationFacts);
    requireFact(
      new Set(operations.map((entry) => entry.slot)).size === operations.length,
    );
    for (const known of state.operations) {
      const remote = operations.find((entry) => entry.slot === known.slot);
      requireFact(remote);
      equal(remote.inputSha256, known.inputSha256);
      if (known.operationId !== null)
        equal(remote.operationId, known.operationId);
      if (!allowed.slots.includes(known.slot)) equal(remote, known);
      else
        requireFact((remote.tokenRevision ?? 0) >= (known.tokenRevision ?? 0));
    }
    for (const remote of operations)
      requireFact(
        state.operations.some((known) => known.slot === remote.slot) ||
          allowed.slots.includes(remote.slot),
      );
    const records = fresh.records.map(recordFacts);
    equal(
      records.map((record) => record.role),
      [...SCENARIO_ROLES],
    );
    for (const remote of records) {
      const known = state.records.find((record) => record.role === remote.role);
      if (!known)
        requireFact(!remote.present || allowed.roles.includes(remote.role));
      else if (!allowed.roles.includes(remote.role)) equal(remote, known);
    }
    if (!state.proofs.restart && state.phase !== 'migration-interrupt')
      equal(fresh.interruption, null);
    if (
      DIRECT_SCENARIO_PHASES.indexOf(state.phase) <
      DIRECT_SCENARIO_PHASES.indexOf('force-recovery')
    ) {
      requireFact(!fresh.forceBefore && !fresh.forceAfter);
    }
    state.operations = operations;
    state.records = records;
    if (state.proofs.restart)
      equal(hash(fresh.interruption), state.proofs.restart.witnessSha256);
    if (state.proofs.force)
      equal(parse(fresh.forceAfter.identityJson), state.proofs.force);
    state.reconciledOrdinal = journal.snapshot().invocationCount;
    await persist();
    control = fresh;
    return fresh;
  };
  const mutate = async (action, migration = null) => {
    await sync();
    return invoke(action, true, migration);
  };
  const fenceTransition = async (role, operation) => {
    const before = state.proofs.fence[operation][role].before;
    const result = await mutate({
      kind: 'tenant-fence',
      role,
      operation,
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
    });
    const next = operation === 'drain' ? 'draining' : 'open';
    const epoch = before.mutationEpoch + (operation === 'drain' ? 1 : 0);
    let after;
    if (result.ok === true) {
      after = result.after;
      requireFact(after.transitionRevision === before.transitionRevision + 1);
    } else {
      requireFact(
        result.ok === false && result.reason?.code === 'FENCE_CAS_CONFLICT',
      );
      const reason = result.reason;
      after = {
        state: reason.state,
        mutationEpoch: reason.mutationEpoch,
        requireMutationEpoch: reason.requireMutationEpoch,
        transitionRevision: reason.transitionRevision,
      };
    }
    requireFact(
      after.state === next &&
        after.mutationEpoch === epoch &&
        after.requireMutationEpoch === true,
    );
    state.proofs.fence[operation][role].after = after;
    state.proofs.fence[operation][role].ordinal = state.mutation.ordinal;
    await persist();
  };
  const fenceStage = async (role, operation) => {
    if (state.proofs.fence[operation][role] === null) {
      const before = await invoke({
        kind: 'tenant-fence',
        role,
        operation: 'read',
      });
      state.proofs.fence[operation][role] = {
        before,
        after: null,
        ordinal: null,
      };
      await persist();
    }
    if (state.proofs.fence[operation][role].after === null)
      await fenceTransition(role, operation);
  };
  const fenceSweep = async (role) => {
    const result = await invoke({
      kind: 'tenant-fence',
      role,
      operation: 'inventory',
    });
    requireFact(
      result.fence.state === 'draining' &&
        result.categories.every(
          (category) => category.class !== 'work' || category.empty,
        ),
    );
    return { ...result, ordinal: state.lastCall.ordinal };
  };
  const record = (role) => control.records.find((entry) => entry.role === role);
  const slot = (name) =>
    control.operations.find((entry) => entry.slot === name);
  const observer = {
    prepared,
    journal,
    apiToken,
    fetch: async (request, init) => {
      state.sdkRequests++;
      await persist();
      return (input.fetch ?? globalThis.fetch)(request, init);
    },
  };
  const observe = (role, release, candidate = false) =>
    observeDirectWorkerVersion({
      ...observer,
      ...expectedVersion(record(role), release, candidate),
    });
  const health = async (role, release, marker) => {
    if (
      state.proofs.health.some(
        (entry) => entry.role === role && entry.release === release,
      )
    )
      return;
    requireFact(
      state.proofs.health.length < DIRECT_SCENARIO_ARRAY_MAXIMA.health,
      'proof-unavailable',
    );
    const result = await invoke({
      kind: 'tenant-probe',
      role,
      operation: 'health',
    });
    equal(result, { role, operation: 'health', release, marker });
    state.proofs.health.push({
      role,
      release,
      marker,
      ordinal: journal.snapshot().invocationCount,
    });
    await persist();
  };
  const objectRead = async (role, present) => {
    const result = await invoke({
      kind: 'tenant-probe',
      role,
      operation: 'object-read',
    });
    equal(result, {
      role,
      operation: 'object-read',
      present,
      ...(present ? { size: objectSize, sha256: objectDigest } : {}),
    });
    return present ? { size: result.size, sha256: result.sha256 } : null;
  };
  const provision = async (role) => {
    await sync();
    if (!record(role).present) {
      requireFact(
        !slot(
          role === 'recovery' ? 'cleanup-recovery-initial' : `cleanup-${role}`,
        ),
        'proof-unavailable',
      );
      const result = await mutate({
        kind: 'provision',
        role,
        release: 'initial',
      });
      requireFact(result.status === 'ready');
      await sync();
    }
    equal(record(role).phase, 'ready');
    if (!state.proofs.initial[role]) {
      const observation = await observe(role, '1');
      equal(observation.trafficPercentage, 100);
      requireFact(
        observation.currentDeployment.versions.length <=
          DIRECT_SCENARIO_ARRAY_MAXIMA.deploymentVersions,
      );
      state.proofs.initial[role] = observation;
      await persist();
    }
    await health(role, '1', 'initial');
    if (role !== 'recovery' && !state.proofs.objects[role]) {
      const result = await mutate({
        kind: 'tenant-probe',
        role,
        operation: 'object-put',
      });
      equal(result, { role, operation: 'object-put', returned: true });
      const object = await objectRead(role, true);
      state.proofs.objects[role] = object;
      await persist();
    }
    if (role === 'recovery') await objectRead(role, false);
    await advancePhase();
  };
  const inventory = async (when) => {
    const name = `inventory-${when}`;
    if (state.proofs.inventories[when]) {
      const selected = await invoke({ kind: 'inventory-read', slot: name });
      equal(selected.operationId, state.proofs.inventories[when].operationId);
      equal(selected.generation, state.proofs.inventories[when].generation);
      await advancePhase();
      return;
    }
    let result = await mutate({ kind: 'inventory-start', slot: name });
    while (result.status === 'pending')
      result = await mutate({
        kind: 'inventory-continue',
        slot: name,
        token: result.token,
      });
    requireFact(result.status === 'complete' && state.inventoryCalls[when] > 1);
    const selected = await invoke({ kind: 'inventory-read', slot: name });
    equal(selected.operationId, result.generation.operationId);
    equal(selected.generation, result.generation.generation);
    const observed = selected.inventory;
    const routes = NORMAL_ROLES.map((role) =>
      observed.routes.filter(
        (route) =>
          route.backend === 'plain-worker' &&
          route.surface === 'custom-domain' &&
          route.scriptName === prepared.names.roles[role].scriptName,
      ),
    );
    const routeHostnames = routes
      .flatMap((rows) => rows.map((row) => row.hostname))
      .sort();
    equal(routeHostnames.length, NORMAL_ROLES.length);
    const proof = {
      operationId: selected.operationId,
      generation: selected.generation,
      calls: state.inventoryCalls[when],
      databaseIds: [...observed.databaseIds].sort(),
      namespaceIds: [...observed.namespaceIds].sort(),
      scriptNames: observed.deployments.map((entry) => entry.scriptName).sort(),
      routeHostnames,
      bucketNames: observed.r2Buckets.map((entry) => entry.bucketName).sort(),
      findings: observed.findings.map((entry) => ({
        kind: entry.kind,
        detailSha256: hash(entry.detail),
      })),
    };
    for (const [key, maximum] of Object.entries(
      DIRECT_SCENARIO_ARRAY_MAXIMA.inventory,
    ))
      requireFact(proof[key].length <= maximum, 'proof-unavailable');
    equal(
      proof.databaseIds,
      NORMAL_ROLES.map((role) => state.proofs.initial[role].databaseId).sort(),
    );
    equal(
      proof.scriptNames,
      NORMAL_ROLES.map((role) => prepared.names.roles[role].scriptName).sort(),
    );
    for (const [index, role] of NORMAL_ROLES.entries())
      equal(
        routes[index].map((row) => row.hostname),
        [prepared.names.roles[role].routeHostname],
      );
    if (when === 'after') {
      requireFact(
        proof.generation > state.proofs.inventories.before.generation,
      );
      equal(
        proof.routeHostnames,
        state.proofs.inventories.before.routeHostnames,
      );
    }
    state.proofs.inventories[when] = proof;
    await persist();
    await advancePhase();
  };
  const audit = async (when) => {
    const name = `audit-${when}`;
    let result = await mutate({ kind: 'audit-start', slot: name });
    while (result.status === 'pending')
      result = await mutate({
        kind: 'audit-continue',
        slot: name,
        token: result.token,
      });
    requireFact(result.status === 'complete');
    equal(result.result.generation, state.proofs.inventories[when].generation);
    equal(result.result.recordCount, 2);
    await sync();
    const frozen = parse(slot(name).inputJson);
    equal(
      frozen.records.map((entry) => entry.tenantTag),
      NORMAL_ROLES.map((role) => prepared.names.roles[role].tenantTag),
    );
    const findings = [];
    let afterOrdinal;
    for (;;) {
      const page = await invoke({
        kind: 'audit-page',
        slot: name,
        limit: 32,
        ...(afterOrdinal === undefined ? {} : { afterOrdinal }),
      });
      findings.push(
        ...page.findings.map((finding) => ({
          tenantTag: finding.tenantTag,
          environment: finding.environment,
          kind: finding.kind,
          detailSha256: hash(finding.detail),
        })),
      );
      requireFact(
        findings.length <= DIRECT_SCENARIO_ARRAY_MAXIMA.auditFindings,
      );
      if (page.done) break;
      requireFact(
        Number.isSafeInteger(page.nextAfterOrdinal) &&
          page.nextAfterOrdinal > (afterOrdinal ?? -1),
      );
      afterOrdinal = page.nextAfterOrdinal;
    }
    equal(findings.length, result.result.findingCount);
    state.proofs.audits[when] = { ...result.result, findings };
    await persist();
    requireFact(findings.length === 0);
    await advancePhase();
  };
  const items = async () => {
    const page = await invoke({ kind: 'migration-page', limit: 2 });
    requireFact(page.done && page.items.length === 2);
    page.items.forEach((item, index) => {
      equal(item.ordinal, index);
      equal(
        item.tenantTag,
        prepared.names.roles[NORMAL_ROLES[index]].tenantTag,
      );
      equal(item.environment, prepared.config.environment);
      requireFact(item.status !== 'failed');
    });
    checkItemConvergence(page.items);
    return page.items;
  };
  const interruption = async () => {
    const witness = checkInterruptionWitness(control.interruption, {
      operationId: slot('migration-next').operationId,
      tenantTag: prepared.names.roles.a.tenantTag,
      environment: prepared.config.environment,
    });
    const current = await items();
    checkInterruptedItems(witness.value, current);
    return { ...witness, current };
  };
  const migration = async () => {
    for (;;) {
      await sync();
      const current = await items();
      const previous = state.mutation;
      if (previous?.migration) {
        const frame = previous.migration;
        const item = current[frame.itemOrdinal];
        equal(item.planCursor, frame.cursor + 1);
        equal(
          jsonHash(
            current.map((entry, index) =>
              index === frame.itemOrdinal
                ? { ...entry, planCursor: frame.cursor, status: 'active' }
                : entry,
            ),
          ),
          frame.itemsSha256,
        );
        requireFact(previous.outcome === 'returned', 'proof-unavailable');
        if (
          !state.proofs.steps.some(
            (entry) => entry.ordinal === previous.ordinal,
          )
        ) {
          if (frame.step === 'arm-maintenance')
            requireFact(previous.attempts.maintenance > 0);
          requireFact(
            state.proofs.steps.length < DIRECT_SCENARIO_ARRAY_MAXIMA.steps,
            'proof-unavailable',
          );
          state.proofs.steps.push({
            ordinal: previous.ordinal,
            itemOrdinal: frame.itemOrdinal,
            step: frame.step,
            beforeCursor: frame.cursor,
            afterCursor: item.planCursor,
            ...previous.attempts,
          });
          await persist();
        }
        state.mutation = null;
        await persist();
      }
      for (const [index, role] of NORMAL_ROLES.entries()) {
        const item = current[index];
        equal(record(role).databaseId, state.proofs.initial[role].databaseId);
        if (item.status === 'pending') {
          equal(expectedVersion(record(role), '1'), {
            role,
            versionId: state.proofs.initial[role].versionId,
            databaseId: state.proofs.initial[role].databaseId,
            specDigest: state.proofs.initial[role].specDigest,
            applicationRelease: '1',
          });
          continue;
        }
        const cursor = item.planCursor;
        const plan = item.plan;
        const next = plan[cursor]?.step;
        if (next === 'arm-maintenance' && !state.proofs.candidate[role]) {
          const candidate = await observe(role, '2', true);
          equal(candidate.specDigest, item.targetSpecDigest);
          checkTrafficDistribution(candidate, state.proofs.initial[role]);
          state.proofs.candidate[role] = candidate;
          await persist();
        }
        if (next === 'promote') {
          requireFact(state.proofs.candidate[role]);
          requireFact(
            state.proofs.steps.some(
              (entry) =>
                entry.itemOrdinal === index &&
                entry.step === 'arm-maintenance' &&
                entry.maintenance > 0,
            ),
          );
        }
        const promoted = plan.findIndex((entry) => entry.step === 'promote');
        if (promoted >= 0 && cursor > promoted && !state.proofs.final[role]) {
          requireFact(state.proofs.candidate[role]);
          const version = state.proofs.candidate[role];
          const observed = await observeDirectWorkerVersion({
            ...observer,
            role,
            versionId: version.versionId,
            databaseId: version.databaseId,
            specDigest: version.specDigest,
            applicationRelease: '2',
          });
          equal(observed.trafficPercentage, 100);
          equal(observed.currentDeployment.versions, [
            { versionId: version.versionId, percentage: 100 },
          ]);
          state.proofs.final[role] = observed;
          await persist();
        }
      }
      if (current.every((item) => item.status === 'complete')) {
        const result = await mutate({ kind: 'migration-continue' });
        requireFact(result.status === 'complete');
        const expected = NORMAL_ROLES.map((role) =>
          expectedVersion(record(role), '2'),
        );
        const effects = await readDirectSettlementEffects({
          ...observer,
          expected,
        });
        state.proofs.effects = effects;
        await persist();
        await advancePhase();
        return;
      }
      const active = current.find((item) => item.status === 'active');
      const frame = active
        ? {
            itemOrdinal: active.ordinal,
            cursor: active.planCursor,
            step: active.plan[active.planCursor].step,
            itemsSha256: jsonHash(current),
          }
        : null;
      const result = await mutate({ kind: 'migration-continue' }, frame);
      requireFact(result.status === 'pending' || result.status === 'complete');
    }
  };
  const decommission = async (role) => {
    await sync();
    let result;
    if (!slot(`decommission-${role}`))
      result = await mutate({ kind: 'decommission-start', role });
    while (result?.status !== 'complete') {
      await sync();
      const metadata = await invoke({ kind: 'decommission-export', role });
      if (metadata.available) {
        if (state.proofs.exports[role]) {
          const { receipt, location, size, sha256 } =
            state.proofs.exports[role];
          equal(
            {
              receipt: metadata.receipt,
              location: metadata.location,
              size: metadata.size,
              sha256: metadata.sha256,
            },
            { receipt, location, size, sha256 },
          );
        } else {
          requireFact(
            metadata.lifecyclePhase === 'database-exported',
            'proof-unavailable',
          );
        }
        const verified = await verifyDirectDecommissionExport({
          ...observer,
          role,
          metadata,
          sourceInvocationOrdinal: journal.snapshot().invocationCount,
        });
        requireFact(
          state.proofs.exportVerifications.length <
            DIRECT_SCENARIO_ARRAY_MAXIMA.exportVerifications,
          'proof-unavailable',
        );
        state.proofs.exports[role] = verified;
        state.proofs.exportVerifications.push(verified);
        await persist();
      } else requireFact(!state.proofs.exports[role]);
      result = await invoke({ kind: 'decommission-continue', role }, true);
      requireFact(result.status !== 'blocked', 'blocked');
      requireFact(result.status === 'pending' || result.status === 'complete');
    }
    const terminal = result.result.record;
    const proof = state.proofs.exports[role];
    requireFact(proof);
    equal(terminal.phase, 'decommissioned');
    equal(terminal.databaseId, proof.receipt.databaseId);
    equal(terminal.scriptName, prepared.names.roles[role].scriptName);
    equal(terminal.decommissionIntent.operationId, proof.receipt.operationId);
    equal(terminal.decommissionIntent.state, 'complete');
    equal(result.result.databaseExport, {
      databaseId: proof.receipt.databaseId,
      location: proof.location,
      size: proof.size,
      sha256: proof.sha256,
    });
    state.proofs.decommission[role] = {
      operationId: proof.receipt.operationId,
      databaseId: terminal.databaseId,
      scriptName: terminal.scriptName,
      phase: terminal.phase,
    };
    await persist();
    await advancePhase();
  };
  const footprintExpectation = (retained) => ({
    retained,
    resource: state.proofs.initial.recovery,
    databaseName: prepared.names.roles.recovery.databaseName,
    cleanup: state.proofs.cleanup,
    force: state.proofs.force,
    versionIdMaximum: DIRECT_SCENARIO_ARRAY_MAXIMA.footprintVersionIds,
  });
  try {
    requireFact(
      journal &&
        typeof journal === 'object' &&
        invocation &&
        !busy.has(journal),
      'invalid-input',
    );
    busy.add(journal);
    acquired = true;
    const snapshot = journal.snapshot();
    requireFact(
      snapshot.lastInvocation?.state !== 'pending' &&
        !snapshot.bootstrap?.pending,
      'outcome-unknown',
    );
    requireFact(
      snapshot.bootstrap?.controlReadOrdinal &&
        snapshot.binding.configSha256 === prepared.configSha256 &&
        snapshot.binding.referenceModuleSetSha256 ===
          prepared.referenceModuleSetSha256 &&
        snapshot.binding.maxInvocations ===
          prepared.config.referenceWorker.maxInvocations,
      'invalid-input',
    );
    state = structuredClone(
      snapshot.scenario ?? initialState(snapshot.invocationCount),
    );
    if (state.failure)
      requireFact(false, state.failure.code, state.failure.detail);
    if (
      state.lastCall?.outcome === 'prepared' ||
      state.mutation?.outcome === 'prepared'
    )
      requireFact(false, 'proof-unavailable');
    requireFact(
      snapshot.binding.maxInvocations >= DIRECT_SCENARIO_MIN_INVOCATIONS,
      'budget-exhausted',
      'below-scenario-floor',
    );
    await persist();
    if (state.phase === 'migration-restart') {
      const currentProcess = await processIdentity();
      if (isDeepStrictEqual(currentProcess, state.proofs.restart.process))
        return { status: 'restart-required' };
    }
    await sync();
    for (;;) {
      switch (state.phase) {
        case 'provision-a':
          await provision('a');
          break;
        case 'provision-b':
          await provision('b');
          break;
        case 'inventory-before':
          await inventory('before');
          break;
        case 'inventory-after':
          await inventory('after');
          break;
        case 'audit-before':
          await audit('before');
          break;
        case 'audit-after':
          await audit('after');
          break;
        case 'fence-drain':
          for (const role of NORMAL_ROLES) {
            await fenceStage(role, 'drain');
            if (state.proofs.fence.sweeps[role] === null) {
              const first = await fenceSweep(role);
              state.proofs.fence.sweeps[role] = {
                first,
                second: null,
                intervalMs: null,
              };
              await persist();
            }
          }
          await advancePhase();
          break;
        case 'fence-reopen':
          for (const role of NORMAL_ROLES) {
            if (state.proofs.fence.sweeps[role].second === null) {
              const second = await fenceSweep(role);
              state.proofs.fence.sweeps[role].second = second;
              state.proofs.fence.sweeps[role].intervalMs = Math.max(
                0,
                second.observedAt -
                  state.proofs.fence.sweeps[role].first.observedAt,
              );
              await persist();
            }
            await fenceStage(role, 'reopen');
          }
          await advancePhase();
          break;
        case 'fence-proofs':
          for (const role of NORMAL_ROLES) {
            if (state.proofs.fence.probes[role] !== null) continue;
            const current = await invoke({
              kind: 'tenant-fence',
              role,
              operation: 'mutate-current',
            });
            requireFact(current.accepted === true);
            const stale = await invoke({
              kind: 'tenant-fence',
              role,
              operation: 'probe-stale',
            });
            requireFact(stale.classification === 'stale');
            const missing = await invoke({
              kind: 'tenant-fence',
              role,
              operation: 'probe-missing',
            });
            requireFact(missing.classification === 'missing');
            const future = await invoke({
              kind: 'tenant-fence',
              role,
              operation: 'probe-future',
            });
            requireFact(future.classification === 'future');
            state.proofs.fence.probes[role] = {
              current: 'accepted',
              missing: 'missing',
              stale: 'stale',
              future: 'future',
              mutationEpoch:
                state.proofs.fence.reopen[role].after.mutationEpoch,
              ordinal: state.lastCall.ordinal,
            };
            await persist();
          }
          await advancePhase();
          break;
        case 'migration-start': {
          await sync();
          if (!migrationStartSettled(control, state.mutation)) {
            const result = await invoke({ kind: 'migration-start' }, true);
            equal(result.status, 'pending');
          }
          await advancePhase();
          break;
        }
        case 'migration-interrupt': {
          await sync();
          if (!migrationInterruptSettled(control, state.mutation)) {
            try {
              await invoke({ kind: 'migration-continue' }, true);
              requireFact(false, 'proof-unavailable');
            } catch (error) {
              if (!(error instanceof DirectInvocationError)) throw error;
              requireFact(error.code === 'injected-response-loss', error.code);
            }
            await sync();
          }
          if (!state.proofs.restart) {
            const lossOrdinal = state.mutation.ordinal;
            const witness = await interruption();
            state.proofs.restart = {
              process: await processIdentity(),
              resumedProcess: null,
              lossOrdinal,
              operationId: witness.value.operationId,
              witnessSha256: hash(control.interruption),
              claimSha256: hash(witness.value.claimJson),
              successorSha256: hash(witness.value.returnedTokenJson),
              itemsSha256: jsonHash(witness.current),
              replayOrdinal: null,
            };
            await persist();
          }
          await advancePhase();
          return { status: 'restart-required' };
        }
        case 'migration-restart': {
          const restart = state.proofs.restart;
          const currentProcess = await processIdentity();
          requireFact(
            !isDeepStrictEqual(currentProcess, restart.process),
            'proof-unavailable',
          );
          await sync();
          equal(hash(control.interruption), restart.witnessSha256);
          const witness = await interruption();
          equal(hash(witness.value.claimJson), restart.claimSha256);
          equal(hash(witness.value.returnedTokenJson), restart.successorSha256);
          equal(jsonHash(witness.current), restart.itemsSha256);
          if (restart.replayOrdinal === null) {
            const replay = await invoke(
              { kind: 'migration-continue', token: witness.claim },
              true,
            );
            const replayOrdinal = state.mutation.ordinal;
            equal(state.mutation.attempts, zeroAttempts());
            equal(replay.token, witness.successor);
            equal(replay.status, 'pending');
            equal(replay.itemOrdinal, 0);
            equal(replay.planCursor, 0);
            equal(await items(), witness.current);
            await sync();
            equal(hash(control.interruption), restart.witnessSha256);
            state.proofs.restart = {
              ...restart,
              resumedProcess: currentProcess,
              replayOrdinal,
            };
            await persist();
          }
          await advancePhase();
          break;
        }
        case 'migration':
          await migration();
          break;
        case 'post-migration':
          for (const role of NORMAL_ROLES) {
            await health(role, '2', 'next');
            equal(await objectRead(role, true), state.proofs.objects[role]);
          }
          await advancePhase();
          break;
        case 'failed-recovery': {
          await sync();
          if (!slot('cleanup-recovery')) {
            const failed = await mutate({
              kind: 'provision',
              role: 'recovery',
              release: 'failed-recovery',
            });
            equal(failed.status, 'failed-provision');
            equal(failed.slot, 'cleanup-recovery');
          } else
            requireFact(
              slot('cleanup-recovery').tokenJson,
              'proof-unavailable',
            );
          await advancePhase();
          break;
        }
        case 'cleanup-recovery': {
          let result = await mutate({
            kind: 'cleanup-continue',
            role: 'recovery',
          });
          while (result.status === 'pending')
            result = await mutate({
              kind: 'cleanup-continue',
              role: 'recovery',
              token: result.token,
            });
          requireFact(result.status !== 'blocked', 'blocked');
          equal(result.status, 'complete');
          const historical = await invoke({
            kind: 'cleanup-receipt',
            role: 'recovery',
          });
          equal(historical.slot, 'cleanup-recovery');
          equal(historical.receipt, result.receipt);
          state.proofs.cleanup = historical.receipt;
          await persist();
          await advancePhase();
          break;
        }
        case 'provision-recovery':
          await provision('recovery');
          break;
        case 'delete-objects':
          for (const role of NORMAL_ROLES) {
            if (state.proofs.objectDeletions[role] !== null) continue;
            const result = await mutate({
              kind: 'tenant-probe',
              role,
              operation: 'object-delete',
            });
            equal(result, { role, operation: 'object-delete', returned: true });
            await objectRead(role, false);
            state.proofs.objectDeletions[role] =
              journal.snapshot().invocationCount;
            await persist();
          }
          await advancePhase();
          break;
        case 'decommission-a':
          await decommission('a');
          break;
        case 'decommission-b':
          await decommission('b');
          break;
        case 'force-terminal-a': {
          if (!state.proofs.terminalForce.a) {
            const prior = state.mutation;
            const resumed =
              prior?.action.kind === 'force-terminal' &&
              prior.action.role === 'a';
            if (resumed) {
              // The force settles and persists its attempts and the identity it
              // deleted before returning. Repeating it answers from the
              // absent-record branch, which replaces the deleting call's witness
              // with a no-op's zeros and its `before` with null, so the
              // settlement this run persisted is what proves the call deleted
              // the row. A prior that settled any other way is therefore
              // terminal here: re-issuing the force recovers neither witness.
              requireFact(
                prior.outcome === 'returned' && prior.attempts !== null,
                'proof-unavailable',
              );
            } else {
              const result = await mutate({
                kind: 'force-terminal',
                role: 'a',
              });
              requireFact(
                result.returned === true && result.after.present === false,
              );
            }
            const settled = resumed ? prior : state.mutation;
            equal(settled.attempts, zeroAttempts());
            equal(settled.before, {
              databaseId: state.proofs.decommission.a.databaseId,
              scriptName: state.proofs.decommission.a.scriptName,
            });
            await sync();
            equal(record('a'), { role: 'a', present: false });
            state.proofs.terminalForce.a = {
              databaseId: settled.before.databaseId,
              scriptName: settled.before.scriptName,
              ordinal: settled.ordinal,
              attempts: settled.attempts,
            };
            await persist();
          }
          await advancePhase();
          break;
        }
        case 'force-recovery': {
          await sync();
          if (control.forceBefore) {
            // The recorded force is what `forceBefore` came from, so a
            // settlement this run cannot read leaves the phase terminal:
            // a second force observes its own call, not the one that ran.
            requireFact(
              state.mutation?.action.kind === 'force-recovery' &&
                state.mutation.outcome === 'returned',
              'proof-unavailable',
            );
          } else {
            await objectRead('recovery', false);
            const metadata = await invoke({
              kind: 'decommission-export',
              role: 'recovery',
            });
            equal(metadata, {
              available: false,
              role: 'recovery',
              lifecyclePhase: 'not-started',
            });
            if (state.proofs.recoveryExportAbsent.beforeOrdinal === null) {
              state.proofs.recoveryExportAbsent.beforeOrdinal =
                journal.snapshot().invocationCount;
              await persist();
            }
          }
          const result = await mutate({ kind: 'force-recovery' });
          requireFact(result.returned === true);
          await advancePhase();
          break;
        }
        case 'force-observe': {
          const result = await invoke({ kind: 'force-observe' });
          state.proofs.force = checkFootprint(
            result.observation,
            footprintExpectation(true),
          );
          await persist();
          const metadata = await invoke({
            kind: 'decommission-export',
            role: 'recovery',
          });
          equal(metadata, {
            available: false,
            role: 'recovery',
            lifecyclePhase: 'not-started',
          });
          if (state.proofs.recoveryExportAbsent.afterOrdinal === null)
            state.proofs.recoveryExportAbsent.afterOrdinal =
              journal.snapshot().invocationCount;
          await persist();
          await advancePhase();
          break;
        }
        case 'recover-force-residual': {
          const result = await mutate({ kind: 'recover-force-residual' });
          requireFact(result.returned === true);
          state.proofs.residual = checkFootprint(
            result.observation,
            footprintExpectation(false),
          );
          await persist();
          await advancePhase();
          break;
        }
        case 'complete':
          requireFact(
            state.proofs.restart?.replayOrdinal &&
              state.proofs.effects.length === 2 &&
              state.proofs.decommission.a &&
              state.proofs.decommission.b &&
              state.proofs.terminalForce.a &&
              state.proofs.force &&
              state.proofs.residual,
          );
          await sync();
          return {
            status: 'complete',
            facts: journal.snapshot().scenario.proofs,
            invocationCount: journal.snapshot().invocationCount,
            attempts: journal.snapshot().scenario.attempts,
            sdkRequests: journal.snapshot().scenario.sdkRequests,
          };
        default:
          requireFact(false, 'invalid-input');
      }
    }
  } catch (error) {
    const observed = failureCodes.has(error?.code)
      ? error.code
      : 'observation-mismatch';
    const detail = failureDetails.has(error?.detail) ? error.detail : undefined;
    let code = observed;
    const snapshot = acquired ? journal.snapshot() : null;
    if (
      state &&
      snapshot &&
      snapshot.lastInvocation?.state !== 'pending' &&
      !snapshot.bootstrap?.pending
    ) {
      state.failure ??= {
        code: observed,
        ordinal: snapshot.invocationCount,
        ...(detail === undefined ? {} : { detail }),
      };
      try {
        await persist();
      } catch (unwritable) {
        if (!(unwritable instanceof DirectRunStateError)) throw unwritable;
        code = 'journal-failed';
      }
    }
    return {
      status: 'failed',
      reason: code,
      ...(code === observed && detail !== undefined ? { detail } : {}),
      phase: state?.phase ?? null,
      invocationCount: snapshot?.invocationCount ?? 0,
    };
  } finally {
    if (acquired) busy.delete(journal);
  }
}
