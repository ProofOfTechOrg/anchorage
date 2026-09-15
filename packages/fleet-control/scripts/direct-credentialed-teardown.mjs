// SPDX-License-Identifier: Apache-2.0

import { isDeepStrictEqual } from 'node:util';

import { validateDirectConformanceConfig } from './direct-credentialed-conformance-config.mjs';
import {
  bucketPages,
  classifyDispatchNamespaces,
  identifier,
  inventory,
  openDirectProviderSession,
  probeAbsent,
  providerErrorFrom,
  singlePage,
  validateProviderAuth,
} from './direct-credentialed-provider.mjs';
import {
  DIRECT_RESIDUAL_SURFACES,
  DIRECT_TEARDOWN_FAILURES,
  DIRECT_TEARDOWN_MAXIMA,
  DirectRunStateError,
} from './direct-credentialed-run-state.mjs';

const ERROR_CODES = new Set(DIRECT_TEARDOWN_FAILURES);
const PROVIDER_CODES = Object.freeze({
  forbidden: 'forbidden',
  'budget-exhausted': 'budget-exhausted',
  'provider-unavailable': 'provider-unavailable',
  'observation-mismatch': 'provider-unavailable',
  'invalid-input': 'invalid-state',
});
const SETTLE_DELAY_MS = 3_000;
const OBJECT_SETTLE_ATTEMPTS = 3;
const OBJECT_SETTLE_DELAY_MS = 2_000;
// The reference upload binds exactly these secrets. Teardown asserts the set it
// observes instead of following whatever the script currently carries.
const REFERENCE_SECRET_NAMES = Object.freeze([
  'CLOUDFLARE_API_TOKEN',
  'DIRECT_DEPLOYMENT_SECRETS',
  'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
]);
const BOOTSTRAP_RECEIPTS = Object.freeze([
  'fleet',
  'quota',
  'exports',
  'upload',
  'active',
  'ingress',
]);
const EMPTY_RECEIPTS = Object.freeze({
  ingress: null,
  worker: null,
  fleet: null,
  quota: null,
  exportObjects: Object.freeze([]),
  exports: null,
});
const NO_IDENTITIES = Object.freeze({
  fleetUuid: null,
  quotaUuid: null,
  exportBucket: null,
  scriptName: null,
  activeVersionId: null,
});
const MUTATION_OPTIONS = Object.freeze({ maxRetries: 0 });

export class DirectTeardownError extends Error {
  constructor(code = 'invalid-state') {
    const accepted = ERROR_CODES.has(code) ? code : 'invalid-state';
    super(accepted);
    this.name = 'DirectTeardownError';
    this.code = accepted;
  }
}

function refuse(code = 'invalid-state') {
  throw new DirectTeardownError(code);
}

function teardownCode(error, apiError) {
  if (error instanceof DirectTeardownError) return error.code;
  const raised = providerErrorFrom(error);
  if (raised) return PROVIDER_CODES[raised.code] ?? 'provider-unavailable';
  if (error instanceof DirectRunStateError)
    return error.code === 'outcome-unknown' ? error.code : 'invalid-state';
  if (
    apiError &&
    error instanceof apiError &&
    (error.status === 401 || error.status === 403)
  )
    return 'forbidden';
  return 'provider-unavailable';
}

function pause(milliseconds) {
  return new Promise((fulfill) => setTimeout(fulfill, milliseconds));
}

function recordableName(value) {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value === value.trim() &&
    Buffer.byteLength(value) <= DIRECT_TEARDOWN_MAXIMA.nameBytes &&
    ![...value].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  );
}

function maximal(length) {
  return Array.from({ length }, () =>
    'n'.repeat(DIRECT_TEARDOWN_MAXIMA.nameBytes),
  );
}

function checkedInput(input) {
  try {
    const journal = input.journal;
    for (const method of [
      'snapshot',
      'recordTeardown',
      'assertTeardownCapacity',
    ])
      if (typeof journal[method] !== 'function') refuse();
    const snapshot = journal.snapshot();
    const prepared = structuredClone(input.prepared);
    const config = validateDirectConformanceConfig(prepared.config);
    if (
      snapshot.version !== 2 ||
      !isDeepStrictEqual(config, prepared.config) ||
      snapshot.binding.configSha256 !== prepared.configSha256 ||
      snapshot.binding.referenceModuleSetSha256 !==
        prepared.referenceModuleSetSha256 ||
      snapshot.binding.resourcePrefix !== config.resourcePrefix ||
      snapshot.binding.maxInvocations !== config.referenceWorker.maxInvocations
    )
      refuse();
    validateProviderAuth(input.apiToken);
    const fetchRequest = input.fetch ?? globalThis.fetch;
    const delay = input.delay ?? pause;
    if (typeof fetchRequest !== 'function' || typeof delay !== 'function')
      refuse();
    return {
      journal,
      config,
      snapshot,
      accountId: snapshot.binding.accountId,
      prefix: snapshot.binding.resourcePrefix,
      apiToken: input.apiToken,
      fetchRequest,
      delay,
    };
  } catch {
    refuse('invalid-state');
  }
}

function survivingIdentities(bootstrap, receipts) {
  return Object.freeze({
    fleetUuid: receipts.fleet ? null : (bootstrap?.fleet?.uuid ?? null),
    quotaUuid: receipts.quota ? null : (bootstrap?.quota?.uuid ?? null),
    exportBucket: receipts.exports ? null : (bootstrap?.exports?.name ?? null),
    scriptName: receipts.worker
      ? null
      : (bootstrap?.upload?.scriptName ?? null),
    activeVersionId: receipts.worker
      ? null
      : (bootstrap?.active?.versionId ?? null),
  });
}

function confirmedExportKeys(scenario, prefix) {
  const keys = [];
  const add = (proof) => {
    const receipt = proof?.receipt;
    if (!receipt) refuse('invalid-state');
    const key = `${prefix}/receipts/v1/${receipt.databaseId}/${receipt.operationId}.sql`;
    if (!keys.includes(key)) keys.push(key);
  };
  add(scenario?.proofs.exports.a);
  add(scenario?.proofs.exports.b);
  for (const proof of scenario?.proofs.exportVerifications ?? []) add(proof);
  if (keys.length !== DIRECT_TEARDOWN_MAXIMA.exportObjects)
    refuse('invalid-state');
  return keys;
}

export async function teardownDirectReference(input) {
  let context;
  try {
    context = checkedInput(input);
  } catch (error) {
    const code = teardownCode(error);
    return Object.freeze({
      status: 'retained',
      reason: code,
      phase: 'refused',
      facts: Object.freeze({
        retainedIdentities: NO_IDENTITIES,
        receipts: EMPTY_RECEIPTS,
        residual: null,
        providerRequests: 0,
        failure: code,
      }),
    });
  }
  const {
    journal,
    config,
    snapshot,
    accountId,
    prefix,
    apiToken,
    fetchRequest,
    delay,
  } = context;
  const selectors = { account_id: accountId };
  const bootstrap = snapshot.bootstrap;
  let teardown = snapshot.teardown ?? null;
  let phase = teardown?.phase ?? 'refused';
  let receipts = teardown?.receipts ?? EMPTY_RECEIPTS;
  let residual = teardown?.residual ?? null;
  let providerRequests = teardown?.providerRequests ?? 0;
  let transport;
  let apiError;
  const facts = (failure) =>
    Object.freeze({
      retainedIdentities: survivingIdentities(bootstrap, receipts),
      receipts,
      residual,
      providerRequests,
      failure,
    });
  const write = async (fields) => {
    try {
      await journal.recordTeardown({
        version: 1,
        phase,
        pending: null,
        receipts,
        residual,
        providerRequests,
        failure: null,
        ...fields,
      });
    } catch (error) {
      refuse(teardownCode(error, apiError));
    }
    teardown = journal.snapshot().teardown;
    phase = teardown.phase;
    receipts = teardown.receipts;
    residual = teardown.residual;
  };
  try {
    if (
      snapshot.lastInvocation?.state === 'pending' ||
      snapshot.bootstrap?.pending
    )
      refuse('outcome-unknown');
    if (!bootstrap || BOOTSTRAP_RECEIPTS.some((key) => !bootstrap[key]))
      refuse('invalid-state');
    if (phase === 'complete' && teardown.failure === null)
      return Object.freeze({ status: 'cleaned', facts: facts(null) });
    const names = bootstrap.context.names;
    const script = names.referenceWorker;
    const bucket = bootstrap.exports.name;
    const zoneId = bootstrap.context.zoneId;
    // A recorded refusal is terminal for automation: it never advances into a
    // deletion phase, whatever the scenario reached afterwards.
    const refusing =
      teardown?.phase === 'refused' ||
      snapshot.scenario?.phase !== 'complete' ||
      snapshot.scenario.failure !== null;
    const confirmed = refusing
      ? []
      : confirmedExportKeys(snapshot.scenario, prefix);
    const session = await openDirectProviderSession({
      apiToken,
      fetchRequest: (target, init) => {
        providerRequests += 1;
        return fetchRequest(target, init);
      },
      timeoutMs: config.referenceWorker.requestTimeoutMs,
    });
    transport = session.transport;
    const { sdk, numbered, single, status, settled, APIError, bound } = session;
    apiError = APIError;

    const observe = async () => {
      const disposable = config.disposableAccount === true;
      const surface = (matched, exhaustive, count) =>
        Object.freeze({
          prefixCount: matched.length,
          prefixNames: Object.freeze(
            matched
              .filter(recordableName)
              .slice(0, DIRECT_TEARDOWN_MAXIMA.prefixNames),
          ),
          globalCount: disposable ? count : null,
          exhaustive,
        });
      const matching = (rows, field) =>
        rows
          .map((row) => row[field])
          .filter(
            (value) => typeof value === 'string' && value.startsWith(prefix),
          );
      const databaseIdentity = (row) => {
        identifier(row.name);
        return [identifier(row.uuid)];
      };
      const databases = await inventory(
        numbered.d1.database.list({ ...selectors, name: prefix }),
        databaseIdentity,
        bound,
      );
      const allDatabases = disposable
        ? await inventory(
            numbered.d1.database.list(selectors),
            databaseIdentity,
            bound,
          )
        : [];
      const namespaces = await inventory(
        numbered.durableObjects.namespaces.list(selectors),
        (row) => [`id:${identifier(row.id)}`],
        bound,
      );
      const scripts = await singlePage(single.workers.scripts.list(selectors));
      const buckets = await bucketPages({
        sdk,
        selectors,
        jurisdiction: 'default',
      });
      const domains = await singlePage(
        single.workers.domains.list({ ...selectors, zone_id: zoneId }),
      );
      const routes = await singlePage(
        single.workers.routes.list({ zone_id: zoneId }),
      );
      let queues;
      try {
        queues = await singlePage(single.queues.list(selectors));
      } catch (error) {
        // A 404 states the account carries no queue collection: an empty page
        // the provider did not attest.
        if (!(error instanceof APIError) || error.status !== 404) throw error;
        queues = { rows: [], exhaustive: false };
      }
      let dispatch;
      try {
        const classified = await classifyDispatchNamespaces(
          single,
          selectors,
          bound,
        );
        dispatch = Object.freeze({
          kind: classified.kind,
          count: classified.count,
          status: null,
          prefixCount: classified.names.filter(
            (name) => typeof name === 'string' && name.startsWith(prefix),
          ).length,
        });
      } catch (error) {
        if (!(error instanceof APIError) || providerErrorFrom(error))
          throw error;
        dispatch = Object.freeze({
          kind: 'fail-closed',
          count: 0,
          status: error.status ?? null,
          prefixCount: 0,
        });
      }
      let versionsGone = null;
      if (receipts.worker) {
        let page;
        const seen = await probeAbsent(
          sdk.workers.scripts.versions.list(script, selectors).then((value) => {
            page = value;
            return value;
          }),
        );
        versionsGone =
          seen === 'absent' ||
          (Array.isArray(page?.result?.items) &&
            page.result.items.length === 0);
      }
      return {
        version: 1,
        surfaces: {
          databases: surface(
            matching(databases, 'name'),
            true,
            allDatabases.length,
          ),
          durableObjectNamespaces: surface(
            matching(namespaces, 'script'),
            true,
            namespaces.length,
          ),
          scripts: surface(
            matching(scripts.rows, 'id'),
            scripts.exhaustive,
            scripts.rows.length,
          ),
          buckets: surface(matching(buckets, 'name'), true, buckets.length),
          domains: surface(
            matching(domains.rows, 'service'),
            domains.exhaustive,
            domains.rows.length,
          ),
          routes: surface(
            matching(routes.rows, 'script'),
            routes.exhaustive,
            routes.rows.length,
          ),
          queues: surface(
            matching(queues.rows, 'queue_name'),
            queues.exhaustive,
            queues.rows.length,
          ),
        },
        bucketJurisdictions: ['default'],
        dispatch,
        versionsGone,
        settleAttempts: 1,
      };
    };
    const isSettled = (observation) =>
      DIRECT_RESIDUAL_SURFACES.every((name) => {
        const entry = observation.surfaces[name];
        return (
          entry.prefixCount === 0 &&
          (entry.globalCount === null || entry.globalCount === 0)
        );
      }) &&
      observation.versionsGone !== false &&
      observation.dispatch.kind !== 'fail-closed' &&
      observation.dispatch.prefixCount === 0;
    const settle = async (attempts) => {
      let observation;
      for (let attempt = 1; attempt <= attempts; attempt += 1) {
        transport.assertBudget();
        observation = { ...(await observe()), settleAttempts: attempt };
        if (isSettled(observation) || attempt === attempts) break;
        await delay(SETTLE_DELAY_MS);
      }
      return observation;
    };

    if (refusing) {
      const failure = teardown?.failure ?? 'scenario-incomplete';
      const observation = await settle(1);
      await write({
        phase: 'refused',
        receipts: EMPTY_RECEIPTS,
        residual: observation,
        failure,
      });
      refuse(failure);
    }

    const settlement = (settledByReread) =>
      Object.freeze({ ordinal: providerRequests, settledByReread });
    const mutate = async ({
      kind,
      key,
      nextPhase,
      prepare,
      probe,
      identity,
      call,
      receipt,
    }) => {
      transport.assertBudget();
      const pending = teardown?.pending ?? null;
      if (pending && (pending.kind !== kind || pending.key !== key))
        refuse('invalid-state');
      if ((await probe()) === 'absent') {
        await write({ phase: nextPhase, receipts: receipt(true) });
        return;
      }
      // Ownership attestation gates every dispatch, not only a resumed one, and
      // runs only against a resource the probe has proved present. Steps whose
      // probe already establishes identity carry none of their own.
      await identity?.();
      // Reads that gate the delete run outside its ambiguity window: a refusal
      // here leaves nothing pending because nothing was issued.
      await prepare?.();
      if (!pending)
        await write({
          phase: nextPhase,
          pending: key === undefined ? { kind } : { kind, key },
        });
      let settledByReread = false;
      try {
        await call();
      } catch (error) {
        const code = teardownCode(error, apiError);
        if (code === 'forbidden' || code === 'budget-exhausted') refuse(code);
        settledByReread = true;
      }
      if ((await probe()) !== 'absent') refuse('outcome-unknown');
      await write({ receipts: receipt(settledByReread) });
    };

    const ingressProbe = async () => {
      let observed;
      const seen = await probeAbsent(
        sdk.workers.scripts.subdomain.get(script, selectors).then((value) => {
          observed = value;
          return value;
        }),
      );
      return seen === 'absent' || observed?.enabled === false
        ? 'absent'
        : 'present';
    };
    const scriptProbe = () =>
      probeAbsent(status.workers.scripts.get(script, selectors).asResponse());
    const databaseProbe = (uuid) => () =>
      probeAbsent(sdk.d1.database.get(uuid, selectors));
    const objectProbe = (key) => () =>
      probeAbsent(
        status.r2.buckets.objects
          .get(key, {
            ...selectors,
            bucket_name: bucket,
            jurisdiction: 'default',
          })
          .asResponse(),
      );
    const bucketProbe = () =>
      probeAbsent(
        sdk.r2.buckets.get(bucket, { ...selectors, jurisdiction: 'default' }),
      );

    // Shared so that a step which mutates one of these resources attests it
    // itself, rather than on the strength of a later step's check.
    const scriptIdentity = async () => {
      const { exactActiveVersionId } = await import('../src/active-route.ts');
      const deployments = (
        await sdk.workers.scripts.deployments.list(script, selectors)
      ).deployments;
      if (!Array.isArray(deployments) || deployments.length === 0)
        refuse('provider-unavailable');
      let active;
      try {
        active = exactActiveVersionId(deployments[0], 'reference');
      } catch {
        refuse('identity-mismatch');
      }
      if (active !== bootstrap.active.versionId) refuse('identity-mismatch');
    };
    const bucketIdentity = async () => {
      const observed = await sdk.r2.buckets.get(bucket, {
        ...selectors,
        jurisdiction: 'default',
      });
      if (
        observed?.name !== bucket ||
        (observed.jurisdiction !== undefined &&
          observed.jurisdiction !== 'default') ||
        typeof observed.creation_date !== 'string' ||
        !Number.isFinite(Date.parse(observed.creation_date)) ||
        new Date(observed.creation_date).toISOString() !==
          bootstrap.exports.creationDate
      )
        refuse('identity-mismatch');
    };

    if (!receipts.exports) {
      const ceiling = Number.MAX_SAFE_INTEGER;
      try {
        await journal.assertTeardownCapacity({
          version: 1,
          phase: 'complete',
          pending: null,
          receipts: {
            ingress: { ordinal: ceiling, settledByReread: true },
            worker: {
              scriptName: script,
              secretNames: maximal(DIRECT_TEARDOWN_MAXIMA.secretNames),
              ordinal: ceiling,
              settledByReread: true,
            },
            fleet: {
              uuid: bootstrap.fleet.uuid,
              ordinal: ceiling,
              settledByReread: true,
            },
            quota: {
              uuid: bootstrap.quota.uuid,
              ordinal: ceiling,
              settledByReread: true,
            },
            exportObjects: confirmed.map((key) => ({
              key,
              ordinal: ceiling,
              settledByReread: true,
            })),
            exports: { name: bucket, ordinal: ceiling, settledByReread: true },
          },
          residual: {
            version: 1,
            surfaces: Object.fromEntries(
              DIRECT_RESIDUAL_SURFACES.map((name) => [
                name,
                {
                  prefixCount: ceiling,
                  prefixNames: maximal(DIRECT_TEARDOWN_MAXIMA.prefixNames),
                  globalCount: ceiling,
                  exhaustive: true,
                },
              ]),
            ),
            bucketJurisdictions: ['default'],
            dispatch: {
              kind: 'enumerated',
              count: ceiling,
              status: ceiling,
              prefixCount: ceiling,
            },
            versionsGone: false,
            settleAttempts: DIRECT_TEARDOWN_MAXIMA.settleAttempts,
          },
          providerRequests: ceiling,
          failure: 'residual-present',
        });
      } catch {
        refuse('invalid-state');
      }
    }

    if (!receipts.ingress)
      await mutate({
        kind: 'disable-reference-ingress',
        nextPhase: 'ingress',
        probe: ingressProbe,
        identity: scriptIdentity,
        call: async () => {
          const answer = await sdk.workers.scripts.subdomain.create(
            script,
            { ...selectors, enabled: false, previews_enabled: false },
            MUTATION_OPTIONS,
          );
          if (answer?.enabled !== false || answer.previews_enabled !== false)
            refuse('provider-unavailable');
        },
        receipt: (settledByReread) => ({
          ...receipts,
          ingress: settlement(settledByReread),
        }),
      });

    if (!receipts.worker) {
      let secretNames = [];
      await mutate({
        kind: 'delete-reference-worker',
        nextPhase: 'worker',
        probe: scriptProbe,
        identity: scriptIdentity,
        prepare: async () => {
          const listed = await singlePage(
            single.workers.scripts.secrets.list(script, selectors),
          );
          // The complete observed set is what is compared: a name
          // `recordableName` rejects is still a secret the script carries.
          const observed = listed.rows.map((row) => row.name).sort();
          if (!isDeepStrictEqual(observed, [...REFERENCE_SECRET_NAMES]))
            refuse('identity-mismatch');
          secretNames = observed;
        },
        call: () =>
          settled.workers.scripts.delete(
            script,
            { ...selectors },
            MUTATION_OPTIONS,
          ),
        receipt: (settledByReread) => ({
          ...receipts,
          worker: {
            scriptName: script,
            secretNames,
            ...settlement(settledByReread),
          },
        }),
      });
    }

    for (const [field, kind, uuid, name] of [
      ['fleet', 'delete-fleet-d1', bootstrap.fleet.uuid, names.fleetDatabase],
      ['quota', 'delete-quota-d1', bootstrap.quota.uuid, names.quotaDatabase],
    ]) {
      if (receipts[field]) continue;
      await mutate({
        kind,
        nextPhase: field,
        probe: databaseProbe(uuid),
        identity: async () => {
          const observed = await sdk.d1.database.get(uuid, selectors);
          if (observed?.uuid !== uuid || observed.name !== name)
            refuse('identity-mismatch');
        },
        call: () =>
          settled.d1.database.delete(uuid, selectors, MUTATION_OPTIONS),
        receipt: (settledByReread) => ({
          ...receipts,
          [field]: { uuid, ...settlement(settledByReread) },
        }),
      });
    }

    if (receipts.exportObjects.length < confirmed.length) {
      const listObjects = async (scoped) =>
        (
          await singlePage(
            single.r2.buckets.objects.list(bucket, {
              ...selectors,
              jurisdiction: 'default',
              ...(scoped ? { prefix: `${prefix}/receipts/v1/` } : {}),
            }),
          )
        ).rows;
      const inspect = (rows) => {
        for (const row of rows)
          if (typeof row?.key !== 'string' || !confirmed.includes(row.key))
            refuse('unexpected-object');
        return rows;
      };
      inspect(await listObjects(true));
      inspect(await listObjects(false));
      // The listings already prove the bucket present, so this attestation
      // needs no probe of its own.
      await bucketIdentity();
      for (const key of confirmed) {
        if (receipts.exportObjects.some((entry) => entry.key === key)) continue;
        await mutate({
          kind: 'delete-export-object',
          key,
          nextPhase: 'export-objects',
          probe: objectProbe(key),
          call: () =>
            settled.r2.buckets.objects.delete(
              key,
              {
                ...selectors,
                bucket_name: bucket,
                jurisdiction: 'default',
              },
              MUTATION_OPTIONS,
            ),
          receipt: (settledByReread) => ({
            ...receipts,
            exportObjects: [
              ...receipts.exportObjects,
              { key, ...settlement(settledByReread) },
            ],
          }),
        });
      }
      for (let attempt = 1; attempt <= OBJECT_SETTLE_ATTEMPTS; attempt += 1) {
        transport.assertBudget();
        if (inspect(await listObjects(true)).length === 0) break;
        if (attempt === OBJECT_SETTLE_ATTEMPTS) refuse('provider-unavailable');
        await delay(OBJECT_SETTLE_DELAY_MS);
      }
    }

    if (!receipts.exports)
      await mutate({
        kind: 'delete-export-r2',
        nextPhase: 'exports',
        probe: bucketProbe,
        identity: bucketIdentity,
        call: () =>
          settled.r2.buckets.delete(
            bucket,
            { ...selectors, jurisdiction: 'default' },
            MUTATION_OPTIONS,
          ),
        receipt: (settledByReread) => ({
          ...receipts,
          exports: { name: bucket, ...settlement(settledByReread) },
        }),
      });

    if (phase !== 'residual' && phase !== 'complete')
      await write({ phase: 'residual' });
    const observation = await settle(DIRECT_TEARDOWN_MAXIMA.settleAttempts);
    const failure = isSettled(observation) ? null : 'residual-present';
    await write({ phase: 'complete', residual: observation, failure });
    if (failure) refuse(failure);
    return Object.freeze({ status: 'cleaned', facts: facts(null) });
  } catch (error) {
    const exhausted = transport?.failure();
    const code =
      error instanceof DirectTeardownError
        ? error.code
        : exhausted
          ? (PROVIDER_CODES[exhausted] ?? 'provider-unavailable')
          : teardownCode(error, apiError);
    return Object.freeze({
      status: 'retained',
      reason: code,
      phase,
      facts: facts(code),
    });
  } finally {
    transport?.close();
  }
}
