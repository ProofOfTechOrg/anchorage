// SPDX-License-Identifier: Apache-2.0

import { isDeepStrictEqual } from 'node:util';

import { databaseExportReceiptKey } from '../src/export-file-name.ts';
import { validateDirectConformanceConfig } from './direct-credentialed-conformance-config.mjs';
import {
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
  DIRECT_TEARDOWN_RECOVERABLE_FAILURES,
  REFERENCE_SECRET_NAMES,
} from './direct-credentialed-reference-vocabulary.mjs';
import { listDirectCredentialedResiduals } from './direct-credentialed-residual-listing.mjs';
import {
  DirectRunStateError,
  isAbandonedDirectScenario,
  mutationPending,
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
// Teardown asserts the set it observes against the set the reference upload
// binds, sorted because a listing's order is the provider's. Reading the
// upload's own list makes a bootstrap-side change visible here.
const EXPECTED_SECRET_NAMES = Object.freeze([...REFERENCE_SECRET_NAMES].sort());
// Exactly the three receipt objects a complete scenario exports; the journal's
// `exportObjects` maximum bounds what a record may carry and is not this
// expectation.
const CONFIRMED_EXPORT_KEYS = 3;
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

export function survivingIdentities(bootstrap, receipts) {
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

function confirmedExportKeys(scenario, prefix, allowPartial) {
  const keys = [];
  const add = (proof) => {
    const receipt = proof?.receipt;
    if (!receipt) {
      if (allowPartial) return;
      refuse('invalid-state');
    }
    const key = databaseExportReceiptKey(prefix, receipt);
    if (!keys.includes(key)) keys.push(key);
  };
  add(scenario?.proofs.exports.a);
  add(scenario?.proofs.exports.b);
  add(scenario?.proofs.reprovisionExports.a);
  if (!allowPartial && keys.length !== CONFIRMED_EXPORT_KEYS)
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
        retainedIdentities: survivingIdentities(null, EMPTY_RECEIPTS),
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
    if (mutationPending(snapshot)) refuse('outcome-unknown');
    if (!bootstrap || BOOTSTRAP_RECEIPTS.some((key) => !bootstrap[key]))
      refuse('invalid-state');
    if (phase === 'complete' && teardown.failure === null)
      return Object.freeze({ status: 'cleaned', facts: facts(null) });
    const names = bootstrap.context.names;
    const script = names.referenceWorker;
    const bucket = bootstrap.exports.name;
    const receiptsPrefix = `${prefix}/receipts/v1/`;
    const zoneId = bootstrap.context.zoneId;
    // A recorded refusal is terminal for automation unless its reason is one a
    // later run clears. Teardown does not reconcile an invocation-level
    // outcome-unknown; it only resumes its own pending provider mutation.
    const abandoned = isAbandonedDirectScenario(snapshot.scenario);
    const sweptAbandoned = abandoned && snapshot.sweep?.phase === 'complete';
    const refusing =
      (teardown?.phase === 'refused' &&
        !DIRECT_TEARDOWN_RECOVERABLE_FAILURES.includes(teardown.failure)) ||
      // D-CC-24/25 permit an incomplete abandoned scenario to delete only
      // after its lifecycle sweep has removed the tenant deployments.
      (!sweptAbandoned &&
        (snapshot.scenario?.phase !== 'complete' ||
          snapshot.scenario.failure !== null));
    const confirmed = refusing
      ? []
      : confirmedExportKeys(snapshot.scenario, prefix, sweptAbandoned);
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
      // A row whose classifying field is unusable is not evidence of absence,
      // so it refuses here. Databases and buckets reach the same refusal one
      // step earlier, in the identity and name checks their listings run.
      const matching = (rows, field) =>
        rows
          .map((row) => {
            const value = row?.[field];
            if (typeof value !== 'string') refuse('provider-unavailable');
            return value;
          })
          .filter((value) => value.startsWith(prefix));
      const listed = (page, field) =>
        surface(matching(page.rows, field), page.exhaustive, page.rows.length);
      const listings = await listDirectCredentialedResiduals({
        sdk,
        numbered,
        single,
        APIError,
        selectors,
        zoneId,
        prefix,
        bound,
        disposable,
      });
      // Both listings are scoped to the zone the bootstrap recorded, so the
      // `globalCount` each contributes below covers that zone and not the
      // account — the scope `bucketJurisdictions` records for the bucket count.
      const dispatch = Object.freeze({
        kind: listings.dispatch.kind,
        count: listings.dispatch.count,
        status: listings.dispatch.status,
        prefixCount: listings.dispatch.names.filter(
          (name) => typeof name === 'string' && name.startsWith(prefix),
        ).length,
      });
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
            matching(listings.databases.rows, 'name'),
            listings.databases.exhaustive,
            listings.allDatabases.rows.length,
          ),
          durableObjectNamespaces: surface(
            matching(listings.namespaces.rows, 'script'),
            listings.namespaces.exhaustive,
            listings.namespaces.rows.length,
          ),
          scripts: listed(listings.scripts, 'id'),
          buckets: surface(
            matching(listings.buckets.rows, 'name'),
            listings.buckets.exhaustive,
            listings.buckets.rows.length,
          ),
          domains: listed(listings.domains, 'service'),
          routes: listed(listings.routes, 'script'),
          queues: listed(listings.queues, 'queue_name'),
        },
        bucketJurisdictions: ['default'],
        dispatch,
        versionsGone,
        settleAttempts: 1,
      };
    };
    const isSettled = (settleObservation) =>
      DIRECT_RESIDUAL_SURFACES.every((name) => {
        const entry = settleObservation.surfaces[name];
        return (
          entry.prefixCount === 0 &&
          (entry.globalCount === null || entry.globalCount === 0)
        );
      }) &&
      settleObservation.versionsGone !== false &&
      settleObservation.dispatch.kind !== 'fail-closed' &&
      settleObservation.dispatch.prefixCount === 0;
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
      field,
      append = false,
      probe,
      identity,
      call,
      receipt,
    }) => {
      transport.assertBudget();
      const pending = teardown?.pending ?? null;
      if (pending && (pending.kind !== kind || pending.key !== key))
        refuse('invalid-state');
      const merged = (receiptSettledByReread) => {
        const entry = {
          ...receipt(),
          ...settlement(receiptSettledByReread),
        };
        return {
          ...receipts,
          [field]: append ? [...receipts[field], entry] : entry,
        };
      };
      // The probe runs first because the identity reads are not
      // `probeAbsent`-wrapped: a 404 there refuses instead of settling.
      if ((await probe()) === 'absent') {
        // The phase advances even where the probe settles the step:
        // `recordTeardown` admits one position at a time, so a phase left
        // behind here makes the next write a two-position jump it refuses.
        await write({ phase: nextPhase, receipts: merged(true) });
        return;
      }
      // Attestation runs ahead of the pending write. On a first attempt a
      // refusal here has issued nothing; on a resume the pending record it
      // refuses in front of is the earlier run's.
      await identity();
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
      await write({ receipts: merged(settledByReread) });
    };

    const ingressProbe = async () => {
      let observed;
      const seen = await probeAbsent(
        sdk.workers.scripts.subdomain.get(script, selectors).then((value) => {
          observed = value;
          return value;
        }),
      );
      // The same predicate the disable call checks its own answer against: a
      // script still reachable on a preview URL carries ingress, whatever
      // `enabled` says.
      return seen === 'absent' ||
        (observed?.enabled === false && observed.previews_enabled === false)
        ? 'absent'
        : 'present';
    };
    const scriptProbe = () =>
      probeAbsent(status.workers.scripts.get(script, selectors).asResponse());
    const databaseProbe = (uuid) => () =>
      probeAbsent(sdk.d1.database.get(uuid, selectors));
    const databaseIdentity = (uuid, name) => async () => {
      const observed = await sdk.d1.database.get(uuid, selectors);
      if (observed?.uuid !== uuid || observed.name !== name)
        refuse('identity-mismatch');
    };
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
            exportObjects: (sweptAbandoned
              ? Array.from(
                  { length: DIRECT_TEARDOWN_MAXIMA.exportObjects },
                  (_entry, index) => `${receiptsPrefix}capacity-${index}`,
                )
              : confirmed
            ).map((key) => ({
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
        field: 'ingress',
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
        receipt: () => ({}),
      });

    if (!receipts.worker) {
      // The attestation lists the secrets; a script the probe already finds
      // absent settles without one and records the empty set.
      let secretNames = [];
      await mutate({
        kind: 'delete-reference-worker',
        nextPhase: 'worker',
        field: 'worker',
        probe: scriptProbe,
        identity: async () => {
          await scriptIdentity();
          const page = await singlePage(
            single.workers.scripts.secrets.list(script, selectors),
          );
          // The observed set is compared whole: a name `recordableName` rejects
          // is still a secret the script carries. The comparison is an
          // equality, so a page that returned fewer rows than the script holds
          // refuses here rather than passing, and the page's own `exhaustive`
          // decides nothing.
          const observed = page.rows.map((row) => row.name).sort();
          if (!isDeepStrictEqual(observed, [...EXPECTED_SECRET_NAMES]))
            refuse('identity-mismatch');
          secretNames = observed;
        },
        call: () =>
          settled.workers.scripts.delete(
            script,
            { ...selectors },
            MUTATION_OPTIONS,
          ),
        receipt: () => ({ scriptName: script, secretNames }),
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
        field,
        probe: databaseProbe(uuid),
        identity: databaseIdentity(uuid, name),
        call: () =>
          settled.d1.database.delete(uuid, selectors, MUTATION_OPTIONS),
        receipt: () => ({ uuid }),
      });
    }

    // The listing carries no `exhaustive`, and needs none: `validateEnvelope`
    // refuses a non-empty cursor, so a page that returns at all is the last
    // one the provider has.
    const listObjects = async (scoped) =>
      (
        await singlePage(
          single.r2.buckets.objects.list(bucket, {
            ...selectors,
            jurisdiction: 'default',
            ...(scoped ? { prefix: receiptsPrefix } : {}),
          }),
        )
      ).rows;
    const inspect = (rows) => {
      for (const row of rows)
        if (typeof row?.key !== 'string' || !confirmed.includes(row.key))
          refuse('unexpected-object');
      return rows;
    };
    const admitAbandonedKeys = (rows) => {
      for (const row of rows) {
        const key = row?.key;
        if (typeof key !== 'string' || !key.startsWith(receiptsPrefix))
          refuse('unexpected-object');
        if (!confirmed.includes(key)) confirmed.push(key);
      }
      if (
        new Set([
          ...confirmed,
          ...receipts.exportObjects.map((entry) => entry.key),
        ]).size > DIRECT_TEARDOWN_MAXIMA.exportObjects
      )
        refuse('unexpected-object');
      return rows;
    };
    // One attestation for the whole bucket sequence: the first call proves the
    // bucket, and each delete's `identity` reads that same proof.
    let attested;
    const attestBucket = () => (attested ??= bucketIdentity());
    if (
      !receipts.exports &&
      (sweptAbandoned || receipts.exportObjects.length < confirmed.length)
    ) {
      // Ownership is attested before the content checks, so an unexpected
      // object cannot pre-empt the proof that this is the run's own bucket.
      // An absent bucket refuses as `provider-unavailable` here exactly as it
      // does from the listings.
      await attestBucket();
      const scoped = await listObjects(true);
      if (sweptAbandoned) admitAbandonedKeys(scoped);
      else inspect(scoped);
      inspect(await listObjects(false));
      for (const key of confirmed) {
        if (receipts.exportObjects.some((entry) => entry.key === key)) continue;
        await mutate({
          kind: 'delete-export-object',
          key,
          nextPhase: 'export-objects',
          field: 'exportObjects',
          append: true,
          probe: objectProbe(key),
          identity: attestBucket,
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
          receipt: () => ({ key }),
        });
      }
    }

    if (!receipts.exports) {
      // A resume that owes only this step has skipped the block above, so the
      // attestation happens here, ahead of the content check below.
      await attestBucket();
      // The empty prefix is owed by the run that deletes the bucket, not by
      // the run that issued the object deletes: a resume holding every object
      // receipt reads the prefix here rather than inheriting an earlier run's
      // reading. Once `exports` is receipted the bucket is gone, so this is
      // also the last point at which the listing has anything to address.
      for (let attempt = 1; attempt <= OBJECT_SETTLE_ATTEMPTS; attempt += 1) {
        transport.assertBudget();
        if (inspect(await listObjects(true)).length === 0) break;
        if (attempt === OBJECT_SETTLE_ATTEMPTS) refuse('provider-unavailable');
        await delay(OBJECT_SETTLE_DELAY_MS);
      }
      await mutate({
        kind: 'delete-export-r2',
        nextPhase: 'exports',
        field: 'exports',
        probe: bucketProbe,
        identity: attestBucket,
        call: () =>
          settled.r2.buckets.delete(
            bucket,
            { ...selectors, jurisdiction: 'default' },
            MUTATION_OPTIONS,
          ),
        receipt: () => ({ name: bucket }),
      });
    }

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
