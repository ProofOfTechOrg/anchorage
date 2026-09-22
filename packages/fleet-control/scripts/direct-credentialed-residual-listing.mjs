// SPDX-License-Identifier: Apache-2.0

import {
  bucketPages,
  classifyDispatchNamespaces,
  identifier,
  inventory,
  providerErrorFrom,
  singlePage,
} from './direct-credentialed-provider.mjs';

export async function listDirectCredentialedResiduals({
  sdk,
  numbered,
  single,
  APIError,
  selectors,
  zoneId,
  prefix,
  bound,
  disposable,
}) {
  const databaseRowKeys = (row) => {
    identifier(row.name);
    return [identifier(row.uuid)];
  };
  const databases = await inventory(
    numbered.d1.database.list({ ...selectors, name: prefix }),
    databaseRowKeys,
    bound,
  );
  const allDatabases = disposable
    ? await inventory(
        numbered.d1.database.list(selectors),
        databaseRowKeys,
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
    bound,
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
    if (!(error instanceof APIError) || error.status !== 404) throw error;
    // A 404 is read as an account that carries no queue collection. No provider
    // capture in this repository attests that reading, so the empty page it
    // stands in for is recorded `exhaustive: false` and the counts below are
    // this reading rather than a page the provider sent.
    queues = { rows: [], exhaustive: false };
  }
  let dispatch;
  try {
    const classified = await classifyDispatchNamespaces(
      single,
      selectors,
      bound,
    );
    dispatch = { ...classified, status: null };
  } catch (error) {
    if (!(error instanceof APIError) || providerErrorFrom(error)) throw error;
    dispatch = {
      kind: 'fail-closed',
      count: 0,
      names: [],
      status: error.status ?? null,
    };
  }
  return {
    databases: { rows: databases, exhaustive: true },
    allDatabases: { rows: allDatabases, exhaustive: true },
    namespaces: { rows: namespaces, exhaustive: true },
    scripts,
    buckets: { rows: buckets, exhaustive: true },
    domains,
    routes,
    queues,
    dispatch,
  };
}
