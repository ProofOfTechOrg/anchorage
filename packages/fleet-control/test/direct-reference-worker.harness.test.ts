// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { D1Database } from '@cloudflare/workers-types';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import { DIRECT_REFERENCE_PATH } from '../scripts/direct-reference-contract.mjs';
import { directFixtureManifest } from './fixtures/direct-credentialed-config.js';

const manifest = directFixtureManifest();
const fleetDatabaseId = '00000000-0000-0000-0000-000000000001';
const quotaDatabaseId = '00000000-0000-0000-0000-000000000002';
const binding = {
  version: 1,
  accountId: 'account',
  fleetDatabaseId,
  quotaDatabaseId,
  exportBucketName: manifest.names.exportBucket,
  referenceModuleSetSha256: 'b'.repeat(64),
  accountWorkersDevSubdomain: 'direct-fixture',
};
const secrets = Object.fromEntries(
  ['a', 'b', 'recovery'].map((role) => [
    role,
    {
      deploymentIdentity: `identity-${role}`.padEnd(40, 'x'),
      maintenanceAdmin: `maintenance-${role}`.padEnd(40, 'y'),
      application: { APP_PROBE_TOKEN: `probe-${role}`.padEnd(40, 'z') },
    },
  ]),
);

describe.sequential('real direct reference context and inventory', {
  timeout: 30_000,
}, () => {
  let directory: string;
  let server: TestHarness;
  let db: D1Database;
  let reload: () => Promise<void>;
  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'direct-reference-'));
    const main = join(directory, 'worker.ts');
    const source = (path: string) =>
      JSON.stringify(fileURLToPath(new URL(path, import.meta.url)));
    await writeFile(
      main,
      `
import {createDirectReferenceWorker} from ${source('../scripts/direct-reference-worker.ts')};
import {createDirectReferenceContext} from ${source('../scripts/direct-reference-context.ts')};
import {DirectReferenceTransport} from ${source('../scripts/direct-reference-transport.ts')};
import {recordDirectResource,directSettlementHost} from ${source('../scripts/direct-reference-observations.ts')};
import {fleetSettlementKey} from ${source('../src/settlement.ts')};
import {deploymentSpecDigest} from ${source('../src/spec-digest.ts')};
const manifest=${JSON.stringify(manifest)};
const binding=${JSON.stringify(binding)};
const secretMap=${JSON.stringify(secrets)};
function single(result){return Response.json({success:true,errors:[],messages:[],result});}
function page(result){return single(result);}
let instance;
export default {async fetch(request,env){
  instance??=crypto.randomUUID();
  const mode=new URL(request.url).searchParams.get('mode');const calls=[];
  const provider=async(input,init)=>{
    const req=new Request(input,init),url=new URL(req.url);
    if(url.href==='data:,')return new Response('');
    if(req.method!=='GET'||url.origin!=='https://api.cloudflare.com'||!url.pathname.startsWith('/client/v4/'))throw new Error('unexpected fixture dispatch');
    const path=url.pathname.slice('/client/v4'.length);
    calls.push({path,page:url.searchParams.get('page'),jurisdiction:req.headers.get('cf-r2-jurisdiction')});
    if(path==='/user/tokens/verify')return single({id:'token-id',status:'active'});
    if(path==='/accounts/account/tokens/token-id')return single({id:'token-id',status:'active',policies:[{id:'zone-authority',effect:'allow',permission_groups:[{id:'zone-read',name:'Zone Read'},{id:'routes-read',name:'Workers Routes Read'},{id:'routes-write',name:'Workers Routes Write'}],resources:{'com.cloudflare.api.account.account':{'com.cloudflare.api.account.zone.*':'*'}}}]});
    if(path==='/zones'){if(url.searchParams.get('account.id')!=='account')throw new Error('wrong zone account');return page([]);}
    if(path==='/accounts/account/workers/domains'||path==='/accounts/account/workers/scripts'||path==='/accounts/account/workers/durable_objects/namespaces')return page([]);
    if(path==='/accounts/account/d1/database'){
      const ordinal=Number(url.searchParams.get('page')??1);
      return page(ordinal===1?[{uuid:'db-a',name:manifest.names.roles.a.databaseName},{uuid:'manager-db',name:manifest.names.fleetDatabase}]:ordinal===2?[{uuid:'db-b',name:manifest.names.roles.b.databaseName}]:[]);
    }
    if(path==='/accounts/account/r2/buckets')return single({buckets:[]});
    throw new Error('unhandled fixture dispatch');
  };
  let current={...env,CLOUDFLARE_API_TOKEN:'inert-reference-token',FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET:'test-invoke',DIRECT_RUN_BINDING:JSON.stringify(binding),DIRECT_DEPLOYMENT_SECRETS:JSON.stringify(secretMap)};
  if(mode==='bad-json')current.DIRECT_RUN_BINDING='{';
  if(mode==='wrong-account')current.DIRECT_RUN_BINDING=JSON.stringify({...binding,accountId:'other'});
  if(mode==='same-database')current.DIRECT_RUN_BINDING=JSON.stringify({...binding,quotaDatabaseId:binding.fleetDatabaseId});
  if(mode==='wrong-secrets')current.DIRECT_DEPLOYMENT_SECRETS=JSON.stringify({...secretMap,a:{...secretMap.a,maintenanceAdmin:'different-maintenance'.padEnd(40,'x')}});
  if(mode==='bad-secrets')current.DIRECT_DEPLOYMENT_SECRETS=JSON.stringify({...secretMap,a:{application:{APP_PROBE_TOKEN:'invalid'}}});
  if(mode==='auth-probe')current=new Proxy(current,{get(target,key){if(key==='FLEET_DB'||key==='QUOTA_DB'||key==='EXPORTS')throw new Error('binding touched before authentication');return Reflect.get(target,key);}});
  if(mode==='deadline'){
    let count=0,ended=false,observedFailure=null;const oldNow=performance.now,oldSnapshot=DirectReferenceTransport.prototype.snapshot;
    Object.defineProperty(performance,'now',{configurable:true,value:()=>ended?1001:(count++===0?0:10)});
    DirectReferenceTransport.prototype.snapshot=function(){ended=true;const value=oldSnapshot.call(this);observedFailure=value.failure;return value;};
    try{const worker=createDirectReferenceWorker({...manifest,referenceRuntime:{...manifest.referenceRuntime,invocationTimeoutMs:1000}},{fetch:provider});const response=await worker.fetch(request,current);return Response.json({status:response.status,observedFailure,body:await response.json(),providerCalls:calls.length});}
    finally{Object.defineProperty(performance,'now',{configurable:true,value:oldNow});DirectReferenceTransport.prototype.snapshot=oldSnapshot;}
  }
  if(mode==='observations'){
    const context=await createDirectReferenceContext(manifest,current,{startedAt:performance.now(),signal:request.signal,fetch:provider});
    const spec=context.spec('a','initial'),digest=deploymentSpecDigest(spec);
    const record={tenantTag:spec.tenantTag,environment:spec.environment,backend:'plain-worker',scriptName:spec.scriptName,databaseId:'resource-db-one',databaseName:spec.databaseName,schemaVersion:1,artifactVersion:'resource-version-one',desiredSpecDigest:digest,routeHostname:spec.routeHostname,phase:'ready',updatedAt:'2026-09-10T00:00:00Z',durableObjectBindings:[{name:'Runner',className:'Runner',namespaceId:'namespace-runner'},{name:'Maintenance',className:'Maintenance',namespaceId:'namespace-maintenance'}],applicationResources:[{name:'B',bucketName:'fixture-b',jurisdiction:'default',reservationNonce:'nonce-b',creationDate:'2026-09-10T00:00:00Z',state:'created'},{name:'A',bucketName:'fixture-a',jurisdiction:'eu',reservationNonce:'nonce-a',state:'reserved'}],applicationBindings:{vars:[{name:'PRIVATE_VALUE',value:secretMap.a.application.APP_PROBE_TOKEN}],secrets:[],r2Buckets:[]}};
    const resource=await recordDirectResource(context,record,'provision-read');
    const repeated=await recordDirectResource(context,{...record,phase:'decommissioning',updatedAt:'2026-09-11T00:00:00Z',durableObjectBindings:[...record.durableObjectBindings].reverse(),applicationResources:[...record.applicationResources].reverse().map(r=>({...r,state:'detached'}))},'migration-read');
    const second=await recordDirectResource(context,{...record,databaseId:'resource-db-two'},'provision-read');
    const reserved=await recordDirectResource(context,{...record,phase:'database-create-authorized',databaseId:'reserved-'+digest.slice(0,48),artifactVersion:'pending',durableObjectBindings:[],applicationResources:[]},'provision-read');
    const target={physicalScriptName:record.scriptName,specDigest:digest,artifactVersion:record.artifactVersion,releaseSchemaVersion:1,application:record.applicationBindings};
    const attestation={physicalScriptName:target.physicalScriptName,specDigest:digest,artifactVersion:target.artifactVersion,source:'workers-deployments',observedAt:'2026-09-10T00:00:00Z'};
    const key=fleetSettlementKey({...record,specDigest:digest});
    const settlement={tenantTag:record.tenantTag,environment:record.environment,target,attestation,settlementKey:key,entry:'migration',alreadySettled:true};
    const host=directSettlementHost(context,record);
    await host.settle(settlement);
    await host.settle({...settlement,entry:'rollback',alreadySettled:false,attestation:{...attestation,observedAt:'2026-09-11T00:00:00Z'}});
    const rejected=[];
    for(const change of [{tenantTag:'foreign'},{environment:'foreign'},{settlementKey:'f'.repeat(64)},{target:{...target,physicalScriptName:'foreign'}},{target:{...target,specDigest:'f'.repeat(64)}},{target:{...target,artifactVersion:'pending'}},{attestation:{...attestation,artifactVersion:'foreign'}}]){
      try{await host.settle({...settlement,...change});rejected.push(false);}catch{rejected.push(true);}
    }
    let writeFailure=false;const remember=context.journal.recordSettlement;
    context.journal.recordSettlement=async()=>{throw new Error('injected native producer write failure');};
    try{await host.settle(settlement);}catch{writeFailure=true;}finally{context.journal.recordSettlement=remember;}
    const reloaded=await createDirectReferenceContext(manifest,current,{startedAt:performance.now(),signal:request.signal,fetch:provider});
    return Response.json({resource,repeated,second,reserved,settlement:await reloaded.journal.readSettlement(key),reloadedResource:await reloaded.journal.readResource('a',resource.identitySha256),rejected,writeFailure,providerCalls:calls.length});
  }
  if(mode==='recipes'||mode==='release-pin'||mode==='prune'){
    const context=await createDirectReferenceContext(manifest,current,{startedAt:performance.now(),signal:request.signal,fetch:provider});
    if(mode==='recipes'){
      const initial=context.spec('a','initial'),next=context.spec('a','next');
      const record={tenantTag:initial.tenantTag,environment:initial.environment,backend:'plain-worker',scriptName:initial.scriptName,databaseId:'db-a',databaseName:initial.databaseName,schemaVersion:1,artifactVersion:'version-1',desiredSpecDigest:deploymentSpecDigest(initial),durableObjectBindings:[],routeHostname:initial.routeHostname,phase:'ready',updatedAt:new Date().toISOString()};
      const activeRelease={physicalScriptName:record.scriptName,specDigest:record.desiredSpecDigest,artifactVersion:record.artifactVersion,releaseSchemaVersion:initial.schemaVersion};
      const migrating={...record,phase:'migrating',schemaVersion:next.schemaVersion,pendingSpecDigest:deploymentSpecDigest(next),activeRelease};
      const pendingRelease={physicalScriptName:record.scriptName,specDigest:deploymentSpecDigest(next),artifactVersion:'version-2',releaseSchemaVersion:next.schemaVersion};
      const teardown={...record,phase:'decommissioning',desiredSpecDigest:deploymentSpecDigest(next),activeRelease,pendingRelease};
      const resource=await recordDirectResource(context,teardown,'teardown-read');
      const rejected=[];
      for(const change of [{activeRelease:{...activeRelease,physicalScriptName:'foreign'}},{activeRelease:{...activeRelease,specDigest:'f'.repeat(64)}},{activeRelease:{...activeRelease,releaseSchemaVersion:99}},{activeRelease:{...activeRelease,artifactVersion:'pending'}},{activeRelease:{...activeRelease,artifactVersion:'foreign'}},{activeRelease:{...activeRelease,topology:{}}},{migrationPriorRelease:activeRelease},{migrationIntent:{}},{phase:'ready'}]){
        try{context.specFor({...migrating,...change});rejected.push(false);}catch{rejected.push(true);}
      }
      return Response.json({initial:deploymentSpecDigest(initial),next:deploymentSpecDigest(next),selectedInitial:deploymentSpecDigest(context.specFor(record)),selectedNext:deploymentSpecDigest(context.specFor(migrating)),selectedTeardown:deploymentSpecDigest(context.specFor(teardown)),knownVersionIds:JSON.parse(resource.identityJson).knownVersionIds,rejected,providerCalls:calls.length});
    }
    if(mode==='release-pin'){
      const slot=await context.journal.readOperation('inventory-before');const run=await context.inventoryStore.readRunByOperation(slot.operationId);
      await context.inventoryStore.releasePin({generation:run.progress.generation,pinnedBy:'direct-reference:'+manifest.resourcePrefix+':inventory-before'});
    }else return Response.json({ok:true,result:await context.control.pruneInventoryGenerations({limit:10})});
    return Response.json({ok:true});
  }
  const worker=createDirectReferenceWorker(manifest,{fetch:provider});
  const response=await worker.fetch(request,current);
  response.headers.set('X-Fixture-Calls',JSON.stringify(calls));response.headers.set('X-Fixture-Instance',instance);return response;
}};`,
    );
    const options = {
      root: directory,
      workers: [
        {
          config: {
            name: 'direct-reference-harness',
            main,
            compatibility_date: '2026-08-06',
            compatibility_flags: ['nodejs_compat'],
            d1_databases: [
              {
                binding: 'FLEET_DB',
                database_name: 'reference-fleet',
                database_id: fleetDatabaseId,
              },
              {
                binding: 'QUOTA_DB',
                database_name: 'reference-quota',
                database_id: quotaDatabaseId,
              },
            ],
            r2_buckets: [
              { binding: 'EXPORTS', bucket_name: manifest.names.exportBucket },
            ],
          },
        },
      ],
    };
    server = createTestHarness(options);
    reload = () =>
      server.update({
        ...options,
        workers: options.workers.map((worker) => ({
          ...worker,
          config: { ...worker.config, vars: { TEST_RELOAD: 'reloaded' } },
        })),
      });
    await server.listen();
    db = (await server.getWorker<{ FLEET_DB: D1Database }>().getEnv()).FLEET_DB;
    const response = await call({ kind: 'control-read' });
    expect(response.status).toBe(200);
  }, 30_000);
  afterAll(async () => {
    try {
      await server?.close();
    } finally {
      if (directory) await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  function call(
    action: unknown,
    mode = '',
    authorization = 'Bearer test-invoke',
  ) {
    return server
      .getWorker()
      .fetch(`https://reference.test${DIRECT_REFERENCE_PATH}?mode=${mode}`, {
        method: 'POST',
        headers: { authorization },
        body: JSON.stringify({
          contractVersion: 1,
          configSha256: manifest.configSha256,
          action,
        }),
      });
  }

  it('authenticates before binding access', async () => {
    const response = await call(
      { kind: 'control-read' },
      'auth-probe',
      'Bearer wrong',
    );
    expect(response.status).toBe(401);
    expect(response.headers.get('X-Fixture-Calls')).toBe('[]');
  });

  it('retains resource and settlement identities through real context producers', async () => {
    const response = await call({ kind: 'control-read' }, 'observations');
    expect(response.status).toBe(200);
    const body = await response.text();
    const value = JSON.parse(body);
    expect(value.repeated).toEqual(value.resource);
    expect(value.reloadedResource).toEqual(value.resource);
    expect(value.second.identitySha256).not.toBe(value.resource.identitySha256);
    const identity = JSON.parse(value.resource.identityJson);
    expect(identity.database).toEqual({
      name: manifest.names.roles.a.databaseName,
      id: 'resource-db-one',
    });
    expect(identity.knownVersionIds).toEqual(['resource-version-one']);
    expect(
      identity.localNamespaces.map((item: { name: string }) => item.name),
    ).toEqual(['Maintenance', 'Runner']);
    expect(
      identity.applicationBuckets.map((item: { name: string }) => item.name),
    ).toEqual(['A', 'B']);
    expect(identity.applicationBuckets[0].creationDate).toBeNull();
    expect(JSON.parse(value.reserved.identityJson)).toMatchObject({
      database: { id: null },
      knownVersionIds: [],
    });
    expect(JSON.parse(value.reserved.provenanceJson).databaseState).toBe(
      'create-outcome-unresolved',
    );
    expect(JSON.parse(value.settlement.provenanceJson)).toEqual({
      entry: 'migration',
      alreadySettled: true,
      observedAt: '2026-09-10T00:00:00Z',
    });
    expect(value.rejected).toEqual(Array(7).fill(true));
    expect(value.writeFailure).toBe(true);
    expect(value.providerCalls).toBe(0);
    for (const role of Object.values(secrets)) {
      for (const secret of [
        role.deploymentIdentity,
        role.maintenanceAdmin,
        role.application.APP_PROBE_TOKEN,
      ])
        expect(body).not.toContain(secret);
    }
    expect(JSON.parse(value.settlement.identityJson).target).not.toHaveProperty(
      'application',
    );
  });

  it('refuses success after the final metrics observe deadline expiry', async () => {
    const response = await call({ kind: 'control-read' }, 'deadline');
    expect(await response.json()).toMatchObject({
      status: 504,
      observedFailure: 'deadline',
      providerCalls: 0,
      body: { ok: false, error: { code: 'invocation-timeout' } },
    });
  });

  it('reports a missing or unfinished inventory prerequisite without creating the after slot', async () => {
    for (const pending of [false, true]) {
      if (pending) {
        const before = await call({
          kind: 'inventory-start',
          slot: 'inventory-before',
        });
        expect((await before.json()) as unknown).toMatchObject({
          ok: true,
          result: { status: 'pending' },
        });
      }
      const response = await call({
        kind: 'inventory-start',
        slot: 'inventory-after',
      });
      expect(response.status).toBe(409);
      expect(await response.json()).toMatchObject({
        ok: false,
        error: { code: 'prerequisite-unavailable' },
      });
      expect(response.headers.get('X-Fixture-Calls')).toBe('[]');
      const control = await call({ kind: 'control-read' });
      const value = (await control.json()) as {
        result: { operations: { slot: string }[] };
      };
      expect(
        value.result.operations.some(
          (operation) => operation.slot === 'inventory-after',
        ),
      ).toBe(false);
    }
  });

  it.each([
    ['bad-json', 'operation-refused'],
    ['same-database', 'operation-refused'],
    ['bad-secrets', 'operation-refused'],
    ['wrong-account', 'run-binding-mismatch'],
    ['wrong-secrets', 'run-binding-mismatch'],
  ])('refuses %s without provider work', async (mode, code) => {
    const response = await call({ kind: 'control-read' }, mode);
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({ ok: false, error: { code } });
    expect(response.headers.get('X-Fixture-Calls')).toBe('[]');
  });

  it('retains a secret fingerprint rather than plaintext in native D1', async () => {
    const row = await db
      .prepare('SELECT binding_json FROM direct_reference_run WHERE run_key=?')
      .bind(manifest.resourcePrefix)
      .first<string>('binding_json');
    expect(row).toBeTruthy();
    expect(row).toContain('tenantSecretsSha256');
    for (const role of Object.values(secrets)) {
      for (const value of [
        role.deploymentIdentity,
        role.maintenanceAdmin,
        ...Object.values(role.application),
      ])
        expect(row).not.toContain(value);
    }
    expect(row).not.toContain('inert-reference-token');
  });

  it('selects retained initial and pending-target recipes without provider work', async () => {
    const response = await call({ kind: 'control-read' }, 'recipes');
    const value = (await response.json()) as {
      initial: string;
      next: string;
      selectedInitial: string;
      selectedNext: string;
      selectedTeardown: string;
      knownVersionIds: string[];
      rejected: boolean[];
      providerCalls: number;
    };
    expect(value.initial).not.toBe(value.next);
    expect(value.selectedInitial).toBe(value.initial);
    expect(value.selectedNext).toBe(value.next);
    expect(value.selectedTeardown).toBe(value.next);
    expect(value.knownVersionIds).toEqual(['version-1', 'version-2']);
    expect(value.rejected).toEqual(Array(9).fill(true));
    expect(value.providerCalls).toBe(0);
  });

  it('refuses missing selected generations instead of substituting latest', async () => {
    const response = await call({
      kind: 'inventory-read',
      slot: 'inventory-before',
    });
    expect(response.status).toBe(409);
    expect(response.headers.get('X-Fixture-Calls')).toBe('[]');
  });

  it('runs real multi-request inventories and retains the selected before generation', async () => {
    const observations: {
      path: string;
      page: string | null;
      jurisdiction: string | null;
    }[] = [];
    async function drain(slot: 'inventory-before' | 'inventory-after') {
      let response = await call({ kind: 'inventory-start', slot });
      let value = (await response.json()) as {
        ok: boolean;
        result: {
          status: string;
          token: { version: number; operationId: string; revision: number };
          generation?: { generation: number };
        };
      };
      expect(
        value.ok,
        JSON.stringify({
          value,
          calls: response.headers.get('X-Fixture-Calls'),
        }),
      ).toBe(true);
      let calls = 1;
      while (value.result.status === 'pending' && calls < 40) {
        const old = value.result.token;
        response = await call({ kind: 'inventory-continue', slot });
        observations.push(
          ...JSON.parse(response.headers.get('X-Fixture-Calls') ?? '[]'),
        );
        value = (await response.json()) as typeof value;
        expect(
          value.ok,
          JSON.stringify({
            value,
            calls: response.headers.get('X-Fixture-Calls'),
          }),
        ).toBe(true);
        calls++;
        if (calls === 2) {
          const replay = await call({
            kind: 'inventory-continue',
            slot,
            token: old,
          });
          expect(replay.status).toBe(200);
          expect(replay.headers.get('X-Fixture-Calls')).toBe('[]');
          expect(((await replay.json()) as typeof value).result.token).toEqual(
            value.result.token,
          );
          for (const token of [
            null,
            { ...old, operationId: 'foreign' },
            {
              ...value.result.token,
              revision: value.result.token.revision + 100,
            },
          ]) {
            const refused = await call({
              kind: 'inventory-continue',
              slot,
              token,
            });
            expect(refused.status).toBeGreaterThanOrEqual(400);
            expect(refused.headers.get('X-Fixture-Calls')).toBe('[]');
          }
        }
      }
      expect(calls).toBeGreaterThan(2);
      expect(value.result.status).toBe('complete');
      return value.result.generation?.generation;
    }
    const before = await drain('inventory-before');
    const beforeRead = await call({
      kind: 'inventory-read',
      slot: 'inventory-before',
    });
    expect(await beforeRead.json()).toMatchObject({
      ok: true,
      result: {
        generation: before,
        inventory: { databaseIds: ['db-a', 'db-b'] },
      },
    });
    const priorInstance = beforeRead.headers.get('X-Fixture-Instance');
    expect(priorInstance).toBeTruthy();
    await reload();
    const reloaded = await call({
      kind: 'inventory-read',
      slot: 'inventory-before',
    });
    expect(reloaded.headers.get('X-Fixture-Instance')).not.toBe(priorInstance);
    expect(await reloaded.json()).toMatchObject({
      ok: true,
      result: {
        generation: before,
        inventory: { databaseIds: ['db-a', 'db-b'] },
      },
    });
    const after = await drain('inventory-after');
    expect(after).toBeGreaterThan(before ?? 0);
    const pruning = await call({ kind: 'control-read' }, 'prune');
    expect(pruning.status).toBe(200);
    expect(await pruning.json()).toMatchObject({
      ok: true,
      result: { deleted: 0 },
    });
    const retained = await call({
      kind: 'inventory-read',
      slot: 'inventory-before',
    });
    expect(await retained.json()).toMatchObject({
      ok: true,
      result: { generation: before },
    });
    const latest = await call({
      kind: 'inventory-read',
      slot: 'inventory-after',
    });
    expect(await latest.json()).toMatchObject({
      ok: true,
      result: { generation: after },
    });
    expect(
      observations
        .filter((x) => x.path.endsWith('/d1/database'))
        .map((x) => x.page ?? '1'),
    ).toEqual(['1', '2', '3', '1', '2', '3']);
    expect(
      observations
        .filter((x) => x.path.endsWith('/r2/buckets'))
        .map((x) => x.jurisdiction ?? 'default'),
    ).toEqual(['default', 'eu', 'fedramp', 'default', 'eu', 'fedramp']);
    expect((await call({ kind: 'control-read' }, 'release-pin')).status).toBe(
      200,
    );
    const released = await call({
      kind: 'inventory-read',
      slot: 'inventory-before',
    });
    expect(released.status).toBeGreaterThanOrEqual(400);
    expect(
      (await call({ kind: 'inventory-read', slot: 'inventory-after' })).status,
    ).toBe(200);
    const pruningReleased = await call({ kind: 'control-read' }, 'prune');
    expect(pruningReleased.status).toBe(200);
    expect(await pruningReleased.json()).toMatchObject({
      ok: true,
      result: { deleted: 1 },
    });
    const replayAfter = await call({
      kind: 'inventory-start',
      slot: 'inventory-after',
    });
    expect(replayAfter.status).toBe(200);
    expect(await replayAfter.json()).toMatchObject({
      ok: true,
      result: { status: 'complete', generation: { generation: after } },
    });
    expect(replayAfter.headers.get('X-Fixture-Calls')).toBe('[]');
    const replayPruned = await call({
      kind: 'inventory-start',
      slot: 'inventory-before',
    });
    expect(replayPruned.status).toBeGreaterThanOrEqual(400);
    expect(replayPruned.headers.get('X-Fixture-Calls')).toBe('[]');
    const latestAfterRefusal = await call({
      kind: 'inventory-read',
      slot: 'inventory-after',
    });
    expect(await latestAfterRefusal.json()).toMatchObject({
      ok: true,
      result: { generation: after },
    });
  });
});
