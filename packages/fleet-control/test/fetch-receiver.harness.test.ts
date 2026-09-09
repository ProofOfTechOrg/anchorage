// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';

describe('native Worker fetch receivers', { timeout: 30_000 }, () => {
  let directory: string;
  let server: TestHarness;
  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'fleet-fetch-receiver-'));
    const main = join(directory, 'worker.ts');
    const source = (relative: string) =>
      JSON.stringify(fileURLToPath(new URL(relative, import.meta.url)));
    await writeFile(
      main,
      `
import {CloudflareProvisioningClient} from ${source('../src/cloudflare-client.ts')};
import {PlainWorkerBackend} from ${source('../src/plain-worker-backend.ts')};
import {deploymentSpecDigest} from ${source('../src/spec-digest.ts')};
import {PlainWorkerProvisioningApiFake} from ${source('./fixtures/plain-worker-provisioning-api-fake.ts')};
import {DirectReferenceTransport} from ${source('../scripts/direct-reference-transport.ts')};
export default {async fetch(request){
  const params=new URL(request.url).searchParams, kind=params.get('kind'), explicit=params.get('mode')==='explicit';
  const native=globalThis.fetch;
  const spec={tenantTag:'acme',environment:'production',scriptName:'receiver-probe',databaseName:'receiver-probe',compatibilityDate:'2026-08-06',mainModule:'worker.js',modules:[{name:'worker.js',content:'export default {fetch(){}}'}],authoredBy:'platform',schemaVersion:1,migrations:[],durableObjectMigrations:[],durableObjectBindings:[],maintenanceBaseUrl:'https://control.example.test',routeHostname:'app.example.test'};
  const digest=deploymentSpecDigest(spec);
  const version={versionId:'version-1',tag:digest,bindings:[{type:'d1',name:'DB',databaseId:'database-1'},...Object.entries({DEPLOYMENT_TENANT:spec.tenantTag,FLEET_ENVIRONMENT:spec.environment,FLEET_SCHEMA_VERSION:'1',FLEET_SPEC_DIGEST:digest,FLEET_INGRESS_CONTRACT:'guarded-object-v1'}).map(([name,value])=>({type:'plain-text',name,value}))]};
  const seen=[];
  const intercept=async function(input,init){
    const proof=await Reflect.apply(native,this,['data:,']);await proof.text();
    seen.push({status:proof.status,receiver:this===undefined?'undefined':'other',method:init?.method??'GET'});
    return kind==='client'?Response.json({success:true,errors:[],result:{uuid:'database-1',name:'receiver-probe'}}):Response.json({nextSweepAt:2000,nextPurgeAt:3000,alarmAt:2000,lastSweepAt:1000,deploymentSpecDigest:digest});
  };
  try{
    if(kind==='transport'){
      const transport=new DirectReferenceTransport({runtime:{requestTimeoutMs:1000,invocationTimeoutMs:5000,maxProviderRequests:9},startedAt:performance.now(),signal:request.signal,...(explicit?{fetch:native}:{})});
      const result=await transport.providerFetch('data:,');await result.text();
      return Response.json({ok:true,status:result.status,metrics:transport.snapshot()});
    }
    globalThis.fetch=intercept;
    if(kind==='client'){
      const client=new CloudflareProvisioningClient({plane:'plain-worker',accountId:'account',apiToken:'inert',requestTimeoutMs:1000,rateCoordinator:{async acquire(){}},...(explicit?{fetch:intercept}:{})});
      const result=await client.getDatabase('database-1');
      return Response.json({ok:true,result,seen});
    }
    const api=new PlainWorkerProvisioningApiFake('per-request',1000);
    api.versions.set(spec.scriptName,[version]);api.deployments.set(spec.scriptName,{versions:[{versionId:'version-1',percentage:100}]});
    const backend=new PlainWorkerBackend({api,identityCaller:'receiver-probe',maintenanceRequestTimeoutMs:1000,...(explicit?{fetch:intercept}:{})});
    const result=kind==='ensure'?await backend.ensureMaintenance(spec,'x'.repeat(32),api.fence(),'version-1'):await backend.inspect(spec,'x'.repeat(32),'version-1');
    return Response.json({ok:true,digest:kind==='ensure'?result.deploymentSpecDigest:result.maintenance.deploymentSpecDigest,seen});
  }catch(error){return Response.json({ok:false,error:String(error),seen});}
  finally{globalThis.fetch=native;}
}};`,
    );
    server = createTestHarness({
      root: directory,
      workers: [
        {
          config: {
            name: 'fleet-fetch-receiver',
            main,
            compatibility_date: '2026-08-06',
            compatibility_flags: ['nodejs_compat'],
          },
        },
      ],
    });
    await server.listen();
  }, 30_000);
  afterAll(async () => {
    try {
      await server?.close();
    } finally {
      if (directory) await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  it.each([
    'default',
    'explicit',
  ])('supports %s native reference transport', async (mode) => {
    const response = await server
      .getWorker()
      .fetch(`https://fixture.test?kind=transport&mode=${mode}`);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      ok: true,
      status: 200,
      metrics: { providerAttempts: 0, maintenanceAttempts: 0 },
    });
  });
  it.each([
    'default',
    'explicit',
  ])('supports %s native client fetch without HTTP', async (mode) => {
    const response = await server
      .getWorker()
      .fetch(`https://fixture.test?kind=client&mode=${mode}`);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      ok: true,
      result: { id: 'database-1', name: 'receiver-probe', created: false },
      seen: [{ status: 200, receiver: 'undefined', method: 'GET' }],
    });
  });
  it.each([
    ['ensure', 'default'],
    ['ensure', 'explicit'],
    ['inspect', 'default'],
    ['inspect', 'explicit'],
  ])('supports %s with %s native maintenance fetch', async (kind, mode) => {
    const response = await server
      .getWorker()
      .fetch(`https://fixture.test?kind=${kind}&mode=${mode}`);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      ok: true,
      digest: expect.stringMatching(/^[a-f0-9]{64}$/),
      seen: [
        {
          status: 200,
          receiver: 'undefined',
          method: kind === 'ensure' ? 'POST' : 'GET',
        },
      ],
    });
  });
});
