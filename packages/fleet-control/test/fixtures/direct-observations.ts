// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildDirectConformanceArtifacts } from '../../scripts/direct-credentialed-artifacts.mjs';
import { preflightDirectConformance } from '../../scripts/direct-credentialed-conformance-preflight.mjs';
import type { DirectExpectedWorkerVersion } from '../../scripts/direct-credentialed-observations.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../../scripts/direct-credentialed-run-state.mjs';
import type { DirectReferenceAction } from '../../scripts/direct-reference-contract.mjs';
import type { DirectDecommissionExportMetadata } from '../../scripts/direct-reference-lifecycle.js';

export const observationHash = (value: string | Uint8Array) =>
  createHash('sha256').update(value).digest('hex');
export const providerJson = (result: unknown, result_info?: unknown) =>
  Response.json({
    success: true,
    errors: [],
    result,
    ...(result_info === undefined ? {} : { result_info }),
  });
export const OBSERVATION_TOKEN = 'legacy/provider-token+sentinel==';
export const SQL_SENTINEL = 'SELECT private_sql_sentinel;';
export type ObservationHook = (
  request: Request,
  fallback: () => Response,
) => Response | Promise<Response>;

export async function closeDirectObservationFixture(
  journal: Pick<DirectRunJournal, 'close'>,
  directory: string,
): Promise<void> {
  try {
    await journal.close();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

export async function directObservationFixture(
  timeout = 1000,
  stage:
    | 'confirmed'
    | 'absent'
    | 'provider-pending'
    | 'no-control' = 'confirmed',
  runtimeOptions: Readonly<{
    invocationTimeoutMs?: number;
    maxProviderRequests?: number;
    maxInvocations?: number;
    nativeArtifacts?: boolean;
  }> = {},
) {
  const directory = await mkdtemp(join(tmpdir(), 'direct-observations-'));
  const config = JSON.parse(
    await readFile(
      new URL(
        '../../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
      'utf8',
    ),
  );
  const reference =
    "import manifest from './direct-run-manifest.js'; export default {fetch(){return Response.json(manifest.contractVersion)}};";
  const tenant =
    'export class Maintenance {} export class Runner {} export default {};';
  config.referenceWorker.artifact = {
    bundle: './reference.mjs',
    mainModule: 'worker.js',
    sha256: observationHash(reference),
  };
  config.deployment.artifact = {
    bundle: './tenant.mjs',
    mainModule: 'worker.js',
    sha256: observationHash(tenant),
  };
  config.referenceWorker.requestTimeoutMs = timeout;
  config.referenceWorker.invocationTimeoutMs = 2000;
  const { nativeArtifacts = false, ...referenceRuntime } = runtimeOptions;
  Object.assign(config.referenceWorker, referenceRuntime);
  const inputConfigPath = join(directory, 'config.json');
  await Promise.all([
    writeFile(inputConfigPath, JSON.stringify(config)),
    writeFile(join(directory, 'reference.mjs'), reference),
    writeFile(join(directory, 'tenant.mjs'), tenant),
  ]);
  const built = nativeArtifacts
    ? await buildDirectConformanceArtifacts({
        configPath: inputConfigPath,
        outputDirectory: join(directory, 'built'),
      })
    : undefined;
  const configPath = built?.configPath ?? inputConfigPath;
  const prepared =
    built?.prepared ?? (await preflightDirectConformance({ configPath }));
  const journal = await openDirectRunState({
    configPath,
    prepared,
    accountId: 'account',
    mode: 'run',
  });
  const names = prepared.names;
  async function settle(action: DirectReferenceAction) {
    const reservation = await journal.reserveInvocation(
      JSON.stringify({
        contractVersion: 2,
        configSha256: prepared.configSha256,
        action,
      }),
    );
    await journal.settleInvocation(reservation);
    return reservation.ordinal;
  }
  if (stage !== 'absent') {
    await journal.bindBootstrapContext({
      names,
      zoneId: 'zone',
      zoneName: config.ownedHostname,
      accountWorkersDevSubdomain: 'attested-account',
      dispatch: { kind: 'empty', count: 0 },
    });
    if (stage === 'provider-pending')
      await journal.beginBootstrapMutation('create-fleet-d1');
    else {
      for (const [kind, name, uuid] of [
        ['create-fleet-d1', names.fleetDatabase, 'fleet-id'],
        ['create-quota-d1', names.quotaDatabase, 'quota-id'],
      ] as const) {
        await journal.beginBootstrapMutation(kind);
        await journal.confirmBootstrapMutation({
          kind,
          receipt: { name, uuid },
        });
      }
      await journal.beginBootstrapMutation('create-export-r2');
      await journal.confirmBootstrapMutation({
        kind: 'create-export-r2',
        receipt: {
          name: names.exportBucket,
          jurisdiction: 'default',
          creationDate: '2026-09-09T12:00:00.000Z',
        },
      });
      await journal.beginBootstrapMutation('upload-reference');
      await journal.confirmBootstrapMutation({
        kind: 'upload-reference',
        receipt: { scriptName: names.referenceWorker, tag: null, etag: null },
      });
      await journal.recordBootstrapObservation({
        kind: 'active',
        deploymentId: 'reference-deployment',
        versionId: 'reference-version',
      });
      await journal.beginBootstrapMutation('enable-reference-ingress');
      await journal.confirmBootstrapMutation({
        kind: 'enable-reference-ingress',
        receipt: { enabled: true, previewsEnabled: false },
      });
      if (stage !== 'no-control')
        await journal.recordBootstrapObservation({
          kind: 'control-read',
          ordinal: await settle({ kind: 'control-read' }),
        });
    }
  }
  const expected: DirectExpectedWorkerVersion[] = ['a', 'b'].map((role) => ({
    role: role as 'a' | 'b',
    versionId: `version-${role}`,
    databaseId: `database-${role}`,
    specDigest: observationHash(`spec-${role}`),
    applicationRelease: '2',
  }));
  const weights = [{ version_id: 'version-a', percentage: 100 }];
  const deployment = {
    id: 'deployment-a',
    strategy: 'percentage',
    versions: weights,
  };
  const bindingList = (index: number, current = false) => {
    const target = expected[index];
    if (!target) throw new Error('fixture target absent');
    const roleNames = names.roles[target.role];
    return [
      { name: 'DB', type: 'd1', database_id: target.databaseId },
      {
        name: 'MAINTENANCE',
        type: 'durable_object_namespace',
        class_name: 'Maintenance',
        namespace_id: 'maintenance-id',
      },
      {
        name: 'RUNNER',
        type: 'durable_object_namespace',
        class_name: 'Runner',
        namespace_id: 'runner-id',
        script_name: roleNames.scriptName,
      },
      {
        name: 'PROBE_BUCKET',
        type: 'r2_bucket',
        bucket_name: 'allocated-probe-bucket',
      },
      ...Object.entries({
        DEPLOYMENT_TENANT: roleNames.tenantTag,
        FLEET_ENVIRONMENT: config.environment,
        FLEET_INGRESS_CONTRACT: 'guarded-object-v1',
        FLEET_SCHEMA_VERSION:
          current && weights[0]?.version_id !== target.versionId ? '1' : '2',
        FLEET_SPEC_DIGEST:
          current && weights[0]?.version_id !== target.versionId
            ? observationHash('old')
            : target.specDigest,
        APPLICATION_RELEASE:
          current && weights[0]?.version_id !== target.versionId ? '1' : '2',
        APPROVAL_ALLOW_SELF_DECISION: 'true',
      }).map(([name, text]) => ({ name, type: 'plain_text', text })),
      { name: 'APP_PROBE_TOKEN', type: 'secret_text' },
      { name: 'DEPLOYMENT_IDENTITY_SECRET', type: 'secret_text' },
      { name: 'MAINTENANCE_ADMIN_SECRET', type: 'secret_text' },
    ];
  };
  const runtime = {
    compatibility_date: config.deployment.compatibilityDate,
    compatibility_flags: config.deployment.compatibilityFlags,
    limits: { cpu_ms: config.deployment.cpuLimitMs },
  };
  const requests: Request[] = [];
  const unexpected: string[] = [];
  let hook: ObservationHook | undefined;
  const effects = expected.map((target) => {
    const identity = {
      version: 1,
      role: target.role,
      tenantTag: names.roles[target.role].tenantTag,
      environment: config.environment,
      target: {
        physicalScriptName: names.roles[target.role].scriptName,
        specDigest: target.specDigest,
        artifactVersion: target.versionId,
      },
    };
    const key = observationHash(
      JSON.stringify({
        tenantTag: identity.tenantTag,
        environment: identity.environment,
        specDigest: target.specDigest,
        artifactVersion: target.versionId,
      }),
    );
    const identity_json = JSON.stringify(identity);
    const provenance_json = JSON.stringify({
      entry: { privateToken: 'opaque-token-sentinel' },
      alreadySettled: false,
      observedAt: '2026-09-09T12:00:00.000Z',
    });
    const bound = (field: string, value: string) =>
      observationHash(
        JSON.stringify([
          'observation',
          config.resourcePrefix,
          'settlement',
          key,
          field,
          value,
        ]),
      );
    return {
      run_key: config.resourcePrefix,
      observation_kind: 'settlement',
      observation_key: key,
      identity_json,
      identity_sha256: bound('identity', identity_json),
      provenance_json,
      provenance_sha256: bound('provenance', provenance_json),
    };
  });
  const exportBytes = Buffer.from(SQL_SENTINEL);
  const metadata: Extract<
    DirectDecommissionExportMetadata,
    { available: true }
  > = {
    available: true,
    role: 'a',
    receipt: {
      version: 1,
      authority: `r2://${names.exportBucket}/${config.resourcePrefix}/receipts/v1`,
      databaseId: 'database-a',
      operationId: 'operation-a',
    },
    location: `r2://${names.exportBucket}/${config.resourcePrefix}/receipts/v1/database-a/operation-a.sql`,
    size: exportBytes.length,
    sha256: observationHash(exportBytes),
    lifecyclePhase: 'database-exported',
    intentState: 'transitioning',
    revision: 4,
    generation: 1,
  };
  const fetchRequest: typeof fetch = async (input, init) => {
    const request = new Request(input, init);
    requests.push(request);
    const url = new URL(request.url);
    if (
      url.origin !== 'https://api.cloudflare.com' ||
      request.headers.get('authorization') !== `Bearer ${OBSERVATION_TOKEN}` ||
      request.redirect !== 'manual'
    )
      throw new Error('unexpected transport');
    const root = '/client/v4/accounts/account';
    const path = url.pathname;
    let response: (() => Response) | undefined;
    if (request.method === 'GET' && path === `${root}/workers/domains`)
      response = () =>
        providerJson(
          expected.map((target, index) => ({
            id: `domain-${index}`,
            hostname: names.roles[target.role].routeHostname,
            service: names.roles[target.role].scriptName,
          })),
          {
            page: 1,
            per_page: expected.length,
            count: expected.length,
            total_count: expected.length,
            total_pages: 1,
          },
        );
    for (const [index, target] of expected.entries()) {
      const script = `${root}/workers/scripts/${names.roles[target.role].scriptName}`;
      if (request.method === 'GET' && path === `${script}/deployments`)
        response = () => providerJson({ deployments: [deployment] });
      if (
        request.method === 'GET' &&
        path === `${script}/deployments/deployment-a`
      )
        response = () => providerJson(deployment);
      if (
        request.method === 'GET' &&
        path === `${script}/versions/${target.versionId}`
      )
        response = () =>
          providerJson({
            id: target.versionId,
            resources: {
              script_runtime: runtime,
              bindings: bindingList(index),
            },
          });
      if (request.method === 'GET' && path === `${script}/settings`)
        response = () =>
          providerJson({
            ...runtime,
            limits: {
              cpu_ms: 999,
              subrequests: config.deployment.subrequestLimit,
            },
            bindings: bindingList(index, true),
          });
    }
    if (
      request.method === 'GET' &&
      path === `${root}/r2/buckets/allocated-probe-bucket`
    )
      response = () =>
        providerJson({
          name: 'allocated-probe-bucket',
          creation_date: '2026-09-09T13:00:00Z',
        });
    if (
      request.method === 'POST' &&
      path === `${root}/d1/database/fleet-id/query`
    ) {
      const body = (await request.clone().json()) as {
        sql: string;
        params: string[];
      };
      if (
        body.sql ===
          "SELECT run_key,observation_kind,observation_key,identity_json,identity_sha256,provenance_json,provenance_sha256 FROM direct_reference_observations WHERE run_key=? AND observation_kind='settlement'" &&
        JSON.stringify(body.params) === JSON.stringify([config.resourcePrefix])
      )
        response = () => providerJson([{ success: true, results: effects }]);
      if (
        body.sql ===
          "SELECT run_key,observation_kind,observation_key,identity_json,identity_sha256,provenance_json,provenance_sha256 FROM direct_reference_observations WHERE run_key=? AND observation_kind='settlement' AND observation_key=?" &&
        body.params[0] === config.resourcePrefix
      ) {
        const effect = effects.find(
          ({ observation_key }) => observation_key === body.params[1],
        );
        response = () =>
          providerJson([{ success: true, results: effect ? [effect] : [] }]);
      }
      if (
        body.sql ===
        'SELECT tenant_tag,environment,backend,script_name,database_id,schema_version,artifact_version,desired_spec_digest,phase,settled_settlement_key FROM anchorage_fleet_deployments WHERE tenant_tag=? AND environment=?'
      ) {
        const index = expected.findIndex(
          (value) =>
            names.roles[value.role].tenantTag === body.params[0] &&
            body.params[1] === config.environment,
        );
        const target = expected[index];
        if (target)
          response = () =>
            providerJson([
              {
                success: true,
                results: [
                  {
                    tenant_tag: names.roles[target.role].tenantTag,
                    environment: config.environment,
                    backend: 'plain-worker',
                    script_name: names.roles[target.role].scriptName,
                    database_id: target.databaseId,
                    schema_version: 2,
                    artifact_version: target.versionId,
                    desired_spec_digest: target.specDigest,
                    phase: 'ready',
                    settled_settlement_key: effects[index]?.observation_key,
                  },
                ],
              },
            ]);
      }
    }
    if (
      request.method === 'GET' &&
      decodeURIComponent(path) ===
        `${root}/r2/buckets/${names.exportBucket}/objects/${config.resourcePrefix}/receipts/v1/database-a/operation-a.sql`
    )
      response = () => new Response(exportBytes);
    if (!response || (url.search && path !== `${root}/workers/domains`)) {
      unexpected.push(`${request.method} ${path}`);
      throw new Error('unexpected request');
    }
    return hook ? hook(request, response) : response();
  };
  return {
    prepared,
    built,
    journal,
    expected,
    deployment,
    runtime,
    effects,
    metadata,
    requests,
    unexpected,
    directory,
    configPath,
    settle,
    input: {
      prepared,
      journal,
      apiToken: OBSERVATION_TOKEN,
      fetch: fetchRequest,
    },
    hook(value: ObservationHook | undefined) {
      hook = value;
    },
    async exportInput() {
      return {
        prepared,
        journal,
        apiToken: OBSERVATION_TOKEN,
        fetch: fetchRequest,
        role: 'a' as const,
        metadata,
        sourceInvocationOrdinal: await settle({
          kind: 'decommission-export',
          role: 'a',
        }),
      };
    },
    async close() {
      await closeDirectObservationFixture(journal, directory);
    },
  };
}
