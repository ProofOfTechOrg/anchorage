// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { validateDirectConformanceConfig } from './direct-credentialed-conformance-config.mjs';
import {
  DIRECT_PROVIDER_MAX_REQUESTS,
  identifier,
  inventory,
  openDirectProviderSession,
  resolveDirectZone,
  validateProviderAuth,
} from './direct-credentialed-provider.mjs';
import { DIRECT_TEARDOWN_MAXIMA } from './direct-credentialed-reference-vocabulary.mjs';
import { listDirectCredentialedResiduals } from './direct-credentialed-residual-listing.mjs';

export const DIRECT_PURGE_OUTPUT_PREFIX = 'DIRECT_PURGE ';
export const DIRECT_PURGE_USAGE =
  'Usage: node packages/fleet-control/scripts/direct-credentialed-purge.mjs [--list|--delete <resourcePrefix>|--help]. Exit codes: 0 success, 1 residual, 2 invalid input, 3 provider failure, 4 internal error.';
export const DIRECT_PURGE_MAX_REQUESTS = DIRECT_PROVIDER_MAX_REQUESTS;
export const DIRECT_PURGE_EXIT_CODES = Object.freeze({
  success: 0,
  residual: 1,
  invalidInput: 2,
  providerFailed: 3,
  internalError: 4,
});

const DELETE_OPTIONS = Object.freeze({ maxRetries: 0 });
const SETTLE_DELAY_MS = 3_000;

class PurgeFailure extends Error {
  constructor(code, surface, step) {
    super(code);
    this.code = code;
    this.surface = surface;
    this.step = step;
  }
}

const lineOf = (summary) =>
  `${DIRECT_PURGE_OUTPUT_PREFIX}${JSON.stringify(summary)}\n`;

function validEnvironment(value) {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value === value.trim() &&
    ![...value].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  );
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

function providerId(value, surface) {
  try {
    return identifier(value, DIRECT_TEARDOWN_MAXIMA.nameBytes);
  } catch {
    throw new PurgeFailure('provider-failed', surface, 'classify');
  }
}

function matches(rows, field, prefix, surface) {
  return rows.filter((row) => {
    const value = row?.[field];
    // A row without a usable classifying field is not evidence of absence.
    if (typeof value !== 'string')
      throw new PurgeFailure('provider-failed', surface, 'classify');
    return value.startsWith(prefix);
  });
}

function summarize(listing, prefix) {
  const selections = {
    databases: matches(listing.databases.rows, 'name', prefix, 'databases'),
    durableObjectNamespaces: matches(
      listing.namespaces.rows,
      'script',
      prefix,
      'durableObjectNamespaces',
    ),
    scripts: matches(listing.scripts.rows, 'id', prefix, 'scripts'),
    buckets: matches(listing.buckets.rows, 'name', prefix, 'buckets'),
    domains: matches(listing.domains.rows, 'service', prefix, 'domains'),
    routes: matches(listing.routes.rows, 'script', prefix, 'routes'),
    // Queues and dispatch namespaces are reported but never deleted because the
    // lane creates neither and its token carries no permission to remove them.
    queues: matches(listing.queues.rows, 'queue_name', prefix, 'queues'),
    dispatch:
      listing.dispatch.kind === 'fail-closed'
        ? []
        : listing.dispatch.names.filter((name) => {
            if (typeof name !== 'string')
              throw new PurgeFailure('provider-failed', 'dispatch', 'classify');
            return name.startsWith(prefix);
          }),
  };
  const fields = {
    databases: 'name',
    durableObjectNamespaces: 'script',
    scripts: 'id',
    buckets: 'name',
    domains: 'service',
    routes: 'script',
    queues: 'queue_name',
  };
  const exhaustive = {
    databases: listing.databases.exhaustive,
    durableObjectNamespaces: listing.namespaces.exhaustive,
    scripts: listing.scripts.exhaustive,
    buckets: listing.buckets.exhaustive,
    domains: listing.domains.exhaustive,
    routes: listing.routes.exhaustive,
    queues: listing.queues.exhaustive,
    dispatch: listing.dispatch.kind !== 'fail-closed',
  };
  const surfaces = Object.fromEntries(
    Object.entries(selections).map(([surface, rows]) => {
      const names =
        surface === 'dispatch' ? rows : rows.map((row) => row[fields[surface]]);
      return [
        surface,
        {
          count: rows.length,
          names: names
            .filter(recordableName)
            .slice(0, DIRECT_TEARDOWN_MAXIMA.prefixNames),
          exhaustive: exhaustive[surface],
        },
      ];
    }),
  );
  const uncorroborated = Object.entries(surfaces)
    .filter(([, summary]) => !summary.exhaustive)
    .map(([surface]) => surface);
  return { selections, surfaces, uncorroborated };
}

function residualOf(listing, surfaces) {
  if (listing.dispatch.kind === 'fail-closed') return 'unverified';
  return Object.values(surfaces).some(({ count }) => count > 0)
    ? 'present'
    : 'none';
}

async function readConfiguration(configPath) {
  const bytes = await readFile(configPath, 'utf8');
  return validateDirectConformanceConfig(JSON.parse(bytes));
}

export function parseDirectPurgeArgs(argv) {
  const args = argv[0] === '--' ? argv.slice(1) : argv;
  if (args.length === 0 || (args.length === 1 && args[0] === '--list'))
    return { mode: 'list', confirmation: null };
  if (args.length === 1 && args[0] === '--help')
    return { mode: 'help', confirmation: null };
  if (args.length === 2 && args[0] === '--delete')
    return { mode: 'delete', confirmation: args[1] };
  return null;
}

export async function runDirectCredentialedPurge(input) {
  let requestCount = 0;
  let transport;
  let before = null;
  const result = (exitCode, summary) => ({
    exitCode,
    summary,
    stdoutLine: lineOf(summary),
    stderrLine: null,
  });
  const parsed = Object.hasOwn(input, 'parsed')
    ? input.parsed
    : parseDirectPurgeArgs(input.argv ?? []);
  if (!parsed)
    return result(2, { mode: null, code: 'usage', usage: DIRECT_PURGE_USAGE });
  if (parsed.mode === 'help')
    return result(0, { mode: 'help', usage: DIRECT_PURGE_USAGE });
  if (!validEnvironment(input.configPath))
    return result(2, {
      mode: parsed.mode,
      code: 'invalid-input',
      variable: 'FLEET_DIRECT_CONFORMANCE_CONFIG',
    });
  let config;
  try {
    config = await readConfiguration(input.configPath);
  } catch {
    return result(2, { mode: parsed.mode, code: 'invalid-input' });
  }
  const prefix = config.resourcePrefix;
  if (parsed.mode === 'delete' && parsed.confirmation !== prefix)
    return result(2, {
      mode: parsed.mode,
      accountId: input.env?.CLOUDFLARE_ACCOUNT_ID ?? null,
      prefix,
      code: 'prefix-mismatch',
    });
  for (const variable of ['CLOUDFLARE_ACCOUNT_ID', 'CLOUDFLARE_API_TOKEN']) {
    if (!validEnvironment(input.env?.[variable]))
      return result(2, {
        mode: parsed.mode,
        prefix,
        code: 'invalid-input',
        variable,
      });
  }
  const accountId = input.env.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = input.env.CLOUDFLARE_API_TOKEN;
  try {
    validateProviderAuth(apiToken);
  } catch {
    return result(2, {
      mode: parsed.mode,
      accountId,
      prefix,
      code: 'invalid-input',
      variable: 'CLOUDFLARE_API_TOKEN',
    });
  }
  let surface = 'session';
  let step = 'open';
  try {
    const session = await openDirectProviderSession({
      apiToken,
      fetchRequest: (target, init) => {
        requestCount += 1;
        return (input.fetch ?? globalThis.fetch)(target, init);
      },
      timeoutMs: config.referenceWorker.requestTimeoutMs,
    });
    transport = session.transport;
    const { sdk, numbered, single, settled, APIError, bound } = session;
    const selectors = { account_id: accountId };
    surface = 'account';
    step = 'verify';
    if ((await sdk.accounts.get(selectors)).id !== accountId)
      throw new PurgeFailure('account-mismatch', surface, step);
    surface = 'zone';
    step = 'resolve';
    const zone = await resolveDirectZone({
      sdk,
      numbered,
      accountId,
      ownedHostname: config.ownedHostname,
      bound,
    });
    const list = () =>
      listDirectCredentialedResiduals({
        sdk,
        numbered,
        single,
        APIError,
        selectors,
        zoneId: zone.id,
        prefix,
        bound,
        disposable: config.disposableAccount === true,
      });
    surface = 'inventory';
    step = 'before';
    const initialListing = await list();
    const initial = summarize(initialListing, prefix);
    before = initial.surfaces;
    if (parsed.mode === 'list') {
      const residual = residualOf(initialListing, before);
      return result(0, {
        mode: 'list',
        accountId,
        prefix,
        before,
        after: before,
        requestCount,
        maxRequestCount: DIRECT_PURGE_MAX_REQUESTS,
        uncorroborated: initial.uncorroborated,
        residual,
      });
    }
    const remove = async (nextSurface, nextStep, call) => {
      surface = nextSurface;
      step = nextStep;
      transport.assertBudget();
      await call();
    };
    for (const row of initial.selections.domains) {
      const id = providerId(row.id, 'domains');
      await remove('domains', 'delete', () =>
        settled.workers.domains.delete(id, selectors, DELETE_OPTIONS),
      );
    }
    for (const row of initial.selections.routes) {
      const id = providerId(row.id, 'routes');
      await remove('routes', 'delete', () =>
        settled.workers.routes.delete(id, { zone_id: zone.id }, DELETE_OPTIONS),
      );
    }
    for (const row of initial.selections.scripts) {
      const id = providerId(row.id, 'scripts');
      await remove('scripts', 'delete', () =>
        settled.workers.scripts.delete(id, selectors, DELETE_OPTIONS),
      );
    }
    surface = 'durableObjectNamespaces';
    step = 'settle';
    for (
      let attempt = 1;
      attempt <= DIRECT_TEARDOWN_MAXIMA.settleAttempts;
      attempt += 1
    ) {
      const namespaces = await inventory(
        numbered.durableObjects.namespaces.list(selectors),
        (row) => [`id:${identifier(row.id)}`],
        bound,
      );
      if (matches(namespaces, 'script', prefix, surface).length === 0) break;
      if (attempt === DIRECT_TEARDOWN_MAXIMA.settleAttempts)
        throw new PurgeFailure('provider-failed', surface, step);
      await (
        input.delay ??
        ((milliseconds) =>
          new Promise((fulfill) => setTimeout(fulfill, milliseconds)))
      )(SETTLE_DELAY_MS);
    }
    for (const row of initial.selections.databases) {
      const uuid = providerId(row.uuid, 'databases');
      await remove('databases', 'delete', () =>
        settled.d1.database.delete(uuid, selectors, DELETE_OPTIONS),
      );
    }
    for (const row of initial.selections.buckets) {
      const bucket = providerId(row.name, 'buckets');
      surface = 'buckets';
      step = 'list-objects';
      const listObjects = () =>
        inventory(
          numbered.r2.buckets.objects.list(bucket, {
            ...selectors,
            jurisdiction: 'default',
          }),
          (object) => [identifier(object.key, DIRECT_TEARDOWN_MAXIMA.keyBytes)],
          bound,
        );
      const objects = await listObjects();
      for (const object of objects) {
        const key = identifier(object.key, DIRECT_TEARDOWN_MAXIMA.keyBytes);
        await remove('buckets', 'delete-object', () =>
          settled.r2.buckets.objects.delete(
            key,
            {
              ...selectors,
              bucket_name: bucket,
              jurisdiction: 'default',
            },
            DELETE_OPTIONS,
          ),
        );
      }
      surface = 'buckets';
      step = 'verify-empty';
      if ((await listObjects()).length !== 0)
        throw new PurgeFailure('provider-failed', surface, step);
      await remove('buckets', 'delete', () =>
        settled.r2.buckets.delete(
          bucket,
          { ...selectors, jurisdiction: 'default' },
          DELETE_OPTIONS,
        ),
      );
    }
    surface = 'inventory';
    step = 'after';
    const finalListing = await list();
    const final = summarize(finalListing, prefix);
    const after = final.surfaces;
    const residual = residualOf(finalListing, after);
    return result(residual === 'none' ? 0 : 1, {
      mode: 'delete',
      accountId,
      prefix,
      before,
      after,
      requestCount,
      maxRequestCount: DIRECT_PURGE_MAX_REQUESTS,
      uncorroborated: final.uncorroborated,
      residual,
    });
  } catch (error) {
    if (error instanceof PurgeFailure && error.code === 'account-mismatch')
      return result(2, {
        mode: parsed.mode,
        accountId,
        prefix,
        code: error.code,
        requestCount,
      });
    const failure =
      error instanceof PurgeFailure
        ? { surface: error.surface, step: error.step }
        : { surface, step };
    return result(3, {
      mode: parsed.mode,
      accountId,
      prefix,
      before,
      after: null,
      requestCount,
      maxRequestCount: DIRECT_PURGE_MAX_REQUESTS,
      residual: 'unverified',
      failure,
    });
  } finally {
    transport?.close();
  }
}
