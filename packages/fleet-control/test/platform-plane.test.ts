// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  type PlatformPlaneClient,
  provisionPlatformPlane,
} from '../src/platform-plane.js';
import type {
  ExternalMutationFence,
  PlatformPlaneLease,
  PlatformPlaneResourceSet,
  PlatformPlaneStateStore,
} from '../src/types.js';

const MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

class FakePlatformStore implements PlatformPlaneStateStore {
  readonly resourceSets: PlatformPlaneResourceSet[] = [];

  async withPlatformPlaneLease<T>(
    resourceSet: PlatformPlaneResourceSet,
    _platformPlaneIdentity: string,
    operation: (lease: PlatformPlaneLease) => Promise<T>,
  ): Promise<T> {
    this.resourceSets.push(resourceSet);
    return operation({
      resourceSetKey: 'resource-set',
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned: async () => {},
      renew: async () => {},
    });
  }
}

class FakePlatformClient implements PlatformPlaneClient {
  readonly calls: string[] = [];
  mutationFenceScopes = 0;
  readonly uploads: Array<{
    scriptName: string;
    bindings: readonly Readonly<Record<string, unknown>>[];
  }> = [];
  readonly secretUpdates: Array<Readonly<Record<string, string | null>>> = [];
  failUploadFor?: string;
  failDisableFor?: string;
  commitThenFailBootstrapFor?: string;
  readonly inspections = new Map<
    string,
    {
      artifactVersion: string;
      databaseIds: string[];
      durableObjectBindings: unknown[];
      serviceBindings: Array<{ name: string; service: string }>;
      queueProducerBindings: Array<{ name: string; queueName: string }>;
      kvNamespaceBindings: Array<{ name: string; namespaceId: string }>;
      dispatchNamespaceBindings: Array<{
        name: string;
        namespace: string;
        outbound: unknown;
      }>;
      plainTextBindings: Record<string, string>;
      workersDevEnabled: boolean;
      previewUrlsEnabled: boolean;
      routeHostnames: string[];
      zoneRoutes: Array<{ zoneId: string; routeId: string; pattern: string }>;
    }
  >();

  platformPlaneScope() {
    return { accountId: 'account', dispatchNamespace: 'anchorage-tenants' };
  }

  async withMutationFence<T>(
    _fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    this.mutationFenceScopes += 1;
    return operation();
  }

  async ensureDispatchNamespace(): Promise<void> {
    this.calls.push('namespace');
  }

  async assertUntrustedDispatchNamespace(): Promise<void> {
    this.calls.push('namespace:assert');
  }

  async uploadControlWorker(spec: {
    readonly scriptName: string;
    readonly bindings: readonly Readonly<Record<string, unknown>>[];
  }): Promise<string> {
    this.calls.push(`upload:${spec.scriptName}`);
    if (this.failUploadFor === spec.scriptName) {
      throw new Error('simulated upload failure');
    }
    this.uploads.push(spec);
    this.inspections.set(spec.scriptName, {
      artifactVersion: `etag:${spec.scriptName}`,
      plainTextBindings: Object.fromEntries(
        spec.bindings.flatMap((binding) =>
          binding.type === 'plain_text'
            ? [[String(binding.name), String(binding.text)] as const]
            : [],
        ),
      ),
      databaseIds: [],
      durableObjectBindings: [],
      serviceBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'service'
          ? [{ name: String(binding.name), service: String(binding.service) }]
          : [],
      ),
      queueProducerBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'queue'
          ? [
              {
                name: String(binding.name),
                queueName: String(binding.queue_name),
              },
            ]
          : [],
      ),
      kvNamespaceBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'kv_namespace'
          ? [
              {
                name: String(binding.name),
                namespaceId: String(binding.namespace_id),
              },
            ]
          : [],
      ),
      dispatchNamespaceBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'dispatch_namespace'
          ? [
              {
                name: String(binding.name),
                namespace: String(binding.namespace),
                outbound: binding.outbound,
              },
            ]
          : [],
      ),
      workersDevEnabled: true,
      previewUrlsEnabled: true,
      routeHostnames: [],
      zoneRoutes: [],
    });
    if (
      this.commitThenFailBootstrapFor === spec.scriptName &&
      spec.bindings.some(
        (binding) => binding.name === 'FLEET_PRIVATE_BOOTSTRAP',
      )
    ) {
      throw new Error('simulated bootstrap response loss');
    }
    return `etag:${spec.scriptName}`;
  }

  async inspectControlWorker(scriptName: string) {
    return this.inspections.get(scriptName);
  }

  async disableControlWorkerPublicAccess(scriptName: string): Promise<void> {
    this.calls.push(`private:${scriptName}`);
    if (this.failDisableFor === scriptName) {
      throw new Error('simulated privacy failure');
    }
    const inspection = this.inspections.get(scriptName);
    if (inspection) {
      inspection.workersDevEnabled = false;
      inspection.previewUrlsEnabled = false;
      inspection.routeHostnames = [];
    }
  }

  async putControlSecrets(
    scriptName: string,
    secrets: Readonly<Record<string, string>>,
  ): Promise<void> {
    this.calls.push(`secrets:${scriptName}`);
    this.secretUpdates.push(secrets);
  }

  async ensureQueueConsumer(options: {
    readonly scriptName: string;
  }): Promise<void> {
    this.calls.push(`consumer:${options.scriptName}`);
  }
}

function platformSpec(siemAuthHeader?: string) {
  const modules = [{ name: 'worker.js', content: 'export default {}' }];
  return {
    platformPlaneIdentity: 'anchorage:primary',
    dispatchNamespace: 'anchorage-tenants',
    compatibilityDate: '2026-08-10',
    hostRoutingKvId: 'kv-hosts',
    tenantCpuLimitMs: 20,
    tenantSubrequestLimit: 50,
    auditQueueName: 'anchorage-audit',
    maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
    siemEndpoint: 'https://siem.example.com/events',
    ...(siemAuthHeader ? { siemAuthHeader } : {}),
    outboundWorker: {
      scriptName: 'fleet-outbound',
      mainModule: 'worker.js',
      modules,
    },
    auditWorker: {
      scriptName: 'fleet-audit',
      mainModule: 'worker.js',
      modules,
    },
    dispatchWorker: {
      scriptName: 'fleet-dispatch',
      mainModule: 'worker.js',
      modules,
    },
  } as const;
}

describe('platform plane provisioning', () => {
  it('deploys outbound and audit controls before the public dispatcher', async () => {
    const client = new FakePlatformClient();
    const store = new FakePlatformStore();
    const module = [{ name: 'worker.js', content: 'export default {}' }];
    const result = await provisionPlatformPlane({
      client,
      store,
      spec: {
        platformPlaneIdentity: 'anchorage:primary',
        dispatchNamespace: 'anchorage-tenants',
        compatibilityDate: '2026-08-10',
        hostRoutingKvId: 'kv-hosts',
        tenantCpuLimitMs: 20,
        tenantSubrequestLimit: 50,
        auditQueueName: 'anchorage-audit',
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        auditDeadLetterQueue: 'anchorage-audit-dlq',
        siemEndpoint: 'https://siem.example.com/events',
        siemAuthHeader: 'Bearer secret',
        outboundWorker: {
          scriptName: 'fleet-outbound',
          mainModule: 'worker.js',
          modules: module,
        },
        auditWorker: {
          scriptName: 'fleet-audit',
          mainModule: 'worker.js',
          modules: module,
        },
        dispatchWorker: {
          scriptName: 'fleet-dispatch',
          mainModule: 'worker.js',
          modules: module,
        },
      },
    });

    expect(client.calls).toEqual([
      'namespace',
      'upload:fleet-outbound',
      'private:fleet-outbound',
      'upload:fleet-outbound',
      'private:fleet-outbound',
      'secrets:fleet-outbound',
      'upload:fleet-audit',
      'private:fleet-audit',
      'upload:fleet-audit',
      'private:fleet-audit',
      'secrets:fleet-audit',
      'consumer:fleet-audit',
      'upload:fleet-dispatch',
      'secrets:fleet-dispatch',
      'namespace:assert',
      'consumer:fleet-audit',
    ]);
    const dispatch = client.uploads.at(-1);
    expect(dispatch?.bindings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'dispatch_namespace',
          namespace: 'anchorage-tenants',
          outbound: expect.objectContaining({
            params: expect.arrayContaining([
              { name: 'environment' },
              { name: 'policyId' },
              { name: 'policyDigest' },
              { name: 'policyHosts' },
            ]),
          }),
        }),
        expect.objectContaining({
          type: 'kv_namespace',
          namespace_id: 'kv-hosts',
        }),
      ]),
    );
    expect(result).toEqual({
      platformPlaneIdentity: 'anchorage:primary',
      dispatchArtifactVersion: 'etag:fleet-dispatch',
      outboundArtifactVersion: 'etag:fleet-outbound',
      auditArtifactVersion: 'etag:fleet-audit',
    });
    expect(client.mutationFenceScopes).toBe(1);
    expect(client.secretUpdates).toEqual([
      {},
      { SIEM_AUTH_HEADER: 'Bearer secret' },
      {},
    ]);
    expect(store.resourceSets).toEqual([
      {
        accountId: 'account',
        dispatchNamespace: 'anchorage-tenants',
        dispatchScriptName: 'fleet-dispatch',
        outboundScriptName: 'fleet-outbound',
        auditScriptName: 'fleet-audit',
        hostRoutingKvId: 'kv-hosts',
        auditQueueName: 'anchorage-audit',
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        auditDeadLetterQueue: 'anchorage-audit-dlq',
      },
    ]);
    expect(
      client.inspections.get('fleet-outbound')?.plainTextBindings,
    ).toMatchObject({
      FLEET_PLATFORM_PLANE_ID: 'anchorage:primary',
      FLEET_RESOURCE_ROLE: 'shared-outbound',
    });
    expect(client.inspections.get('fleet-outbound')).toMatchObject({
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      routeHostnames: [],
      zoneRoutes: [],
    });
    expect(client.inspections.get('fleet-audit')).toMatchObject({
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      routeHostnames: [],
      zoneRoutes: [],
    });
  });

  it('rejects name collisions and foreign Workers before any mutation', async () => {
    const module = [{ name: 'worker.js', content: 'export default {}' }];
    const baseSpec = {
      platformPlaneIdentity: 'anchorage:primary',
      dispatchNamespace: 'anchorage-tenants',
      compatibilityDate: '2026-08-10',
      hostRoutingKvId: 'kv-hosts',
      tenantCpuLimitMs: 20,
      tenantSubrequestLimit: 50,
      auditQueueName: 'anchorage-audit',
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      siemEndpoint: 'https://siem.example.com/events',
      outboundWorker: {
        scriptName: 'fleet-outbound',
        mainModule: 'worker.js',
        modules: module,
      },
      auditWorker: {
        scriptName: 'fleet-audit',
        mainModule: 'worker.js',
        modules: module,
      },
      dispatchWorker: {
        scriptName: 'fleet-dispatch',
        mainModule: 'worker.js',
        modules: module,
      },
    } as const;
    const collisionClient = new FakePlatformClient();
    await expect(
      provisionPlatformPlane({
        client: collisionClient,
        store: new FakePlatformStore(),
        spec: {
          ...baseSpec,
          auditWorker: {
            ...baseSpec.auditWorker,
            scriptName: 'fleet-outbound',
          },
        },
      }),
    ).rejects.toThrow(/must be distinct/);
    expect(collisionClient.calls).toEqual([]);

    const foreignClient = new FakePlatformClient();
    foreignClient.inspections.set('fleet-outbound', {
      artifactVersion: 'foreign-version',
      databaseIds: [],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      kvNamespaceBindings: [],
      dispatchNamespaceBindings: [],
      plainTextBindings: {
        FLEET_PLATFORM_PLANE_ID: 'another-plane',
        FLEET_RESOURCE_ROLE: 'shared-outbound',
      },
      workersDevEnabled: true,
      previewUrlsEnabled: true,
      routeHostnames: ['foreign.example.test'],
      zoneRoutes: [],
    });
    await expect(
      provisionPlatformPlane({
        client: foreignClient,
        spec: baseSpec,
        store: new FakePlatformStore(),
      }),
    ).rejects.toThrow(/not owned by platform plane/);
    expect(foreignClient.calls).toEqual([]);
  });

  it('rejects a global dispatcher verifier change before provider mutation', async () => {
    const client = new FakePlatformClient();
    client.inspections.set('fleet-dispatch', {
      artifactVersion: 'etag:fleet-dispatch',
      databaseIds: [],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      kvNamespaceBindings: [],
      dispatchNamespaceBindings: [],
      plainTextBindings: {
        FLEET_PLATFORM_PLANE_ID: 'anchorage:primary',
        FLEET_RESOURCE_ROLE: 'shared-dispatch',
        FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY:
          '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-previous","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      },
      workersDevEnabled: true,
      previewUrlsEnabled: true,
      routeHostnames: [],
      zoneRoutes: [],
    });

    await expect(
      provisionPlatformPlane({
        client,
        store: new FakePlatformStore(),
        spec: platformSpec(),
      }),
    ).rejects.toThrow(/verifier is immutable/);
    expect(client.calls).toEqual([]);
    expect(client.uploads).toEqual([]);
  });

  it('fails the final whole-group attestation if preview exposure appears mid-operation', async () => {
    class OverwritingClient extends FakePlatformClient {
      override async assertUntrustedDispatchNamespace(): Promise<void> {
        await super.assertUntrustedDispatchNamespace();
        const outbound = this.inspections.get('fleet-outbound');
        if (outbound) {
          outbound.previewUrlsEnabled = true;
        }
      }
    }
    const client = new OverwritingClient();
    const module = [{ name: 'worker.js', content: 'export default {}' }];

    await expect(
      provisionPlatformPlane({
        client,
        store: new FakePlatformStore(),
        spec: {
          platformPlaneIdentity: 'anchorage:primary',
          dispatchNamespace: 'anchorage-tenants',
          compatibilityDate: '2026-08-10',
          hostRoutingKvId: 'kv-hosts',
          tenantCpuLimitMs: 20,
          tenantSubrequestLimit: 50,
          auditQueueName: 'anchorage-audit',
          maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
          siemEndpoint: 'https://siem.example.com/events',
          outboundWorker: {
            scriptName: 'fleet-outbound',
            mainModule: 'worker.js',
            modules: module,
          },
          auditWorker: {
            scriptName: 'fleet-audit',
            mainModule: 'worker.js',
            modules: module,
          },
          dispatchWorker: {
            scriptName: 'fleet-dispatch',
            mainModule: 'worker.js',
            modules: module,
          },
        },
      }),
    ).rejects.toThrow(/remains publicly routable/);
  });

  it('converges a removed SIEM secret to exact absence', async () => {
    const client = new FakePlatformClient();
    const store = new FakePlatformStore();

    await provisionPlatformPlane({
      client,
      store,
      spec: platformSpec('Bearer old-secret'),
    });
    await provisionPlatformPlane({ client, store, spec: platformSpec() });

    expect(client.secretUpdates).toEqual([
      {},
      { SIEM_AUTH_HEADER: 'Bearer old-secret' },
      {},
      {},
      {},
      {},
    ]);
  });

  it('repairs private exposure before a retrying upload can fail', async () => {
    const client = new FakePlatformClient();
    client.inspections.set('fleet-outbound', {
      artifactVersion: 'old-version',
      databaseIds: [],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      kvNamespaceBindings: [],
      dispatchNamespaceBindings: [],
      plainTextBindings: {
        FLEET_PLATFORM_PLANE_ID: 'anchorage:primary',
        FLEET_RESOURCE_ROLE: 'shared-outbound',
      },
      workersDevEnabled: true,
      previewUrlsEnabled: true,
      routeHostnames: ['public.example.test'],
      zoneRoutes: [],
    });
    client.failUploadFor = 'fleet-outbound';

    await expect(
      provisionPlatformPlane({
        client,
        store: new FakePlatformStore(),
        spec: platformSpec(),
      }),
    ).rejects.toThrow(/simulated upload failure/);

    expect(client.calls.slice(0, 3)).toEqual([
      'namespace',
      'private:fleet-outbound',
      'upload:fleet-outbound',
    ]);
    expect(client.inspections.get('fleet-outbound')).toMatchObject({
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      routeHostnames: [],
    });
  });

  it('exposes only a deny-all bootstrap if initial privatization crashes', async () => {
    const client = new FakePlatformClient();
    client.failDisableFor = 'fleet-outbound';

    await expect(
      provisionPlatformPlane({
        client,
        store: new FakePlatformStore(),
        spec: platformSpec(),
      }),
    ).rejects.toThrow(/simulated privacy failure/);

    expect(client.inspections.get('fleet-outbound')).toMatchObject({
      databaseIds: [],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      kvNamespaceBindings: [],
      dispatchNamespaceBindings: [],
      plainTextBindings: {
        FLEET_PLATFORM_PLANE_ID: 'anchorage:primary',
        FLEET_RESOURCE_ROLE: 'shared-outbound',
        FLEET_PRIVATE_BOOTSTRAP: 'deny-all-v1',
      },
    });
    expect(client.secretUpdates).toEqual([]);
  });

  it('reconciles a committed private bootstrap after provider response loss', async () => {
    const client = new FakePlatformClient();
    client.commitThenFailBootstrapFor = 'fleet-outbound';

    await expect(
      provisionPlatformPlane({
        client,
        store: new FakePlatformStore(),
        spec: platformSpec(),
      }),
    ).resolves.toMatchObject({
      outboundArtifactVersion: 'etag:fleet-outbound',
    });
    expect(
      client.calls.filter((call) => call === 'upload:fleet-outbound'),
    ).toHaveLength(2);
    expect(client.inspections.get('fleet-outbound')?.workersDevEnabled).toBe(
      false,
    );
  });

  it('rejects a provider response that drops or retains role bindings', async () => {
    class BindingDriftClient extends FakePlatformClient {
      override async uploadControlWorker(spec: {
        readonly scriptName: string;
        readonly bindings: readonly Readonly<Record<string, unknown>>[];
      }): Promise<string> {
        const version = await super.uploadControlWorker(spec);
        if (
          spec.scriptName === 'fleet-audit' &&
          spec.bindings.some((binding) => binding.name === 'SIEM_ENDPOINT')
        ) {
          const inspection = this.inspections.get(spec.scriptName);
          if (inspection) {
            delete inspection.plainTextBindings.SIEM_ENDPOINT;
            inspection.serviceBindings = [
              { name: 'UNEXPECTED', service: 'foreign-worker' },
            ];
          }
        }
        return version;
      }
    }

    await expect(
      provisionPlatformPlane({
        client: new BindingDriftClient(),
        store: new FakePlatformStore(),
        spec: platformSpec(),
      }),
    ).rejects.toThrow(/drifted role bindings/);
  });

  it('attests exact binding sets independent of provider response order', async () => {
    class ReorderedBindingsClient extends FakePlatformClient {
      override async uploadControlWorker(spec: {
        readonly scriptName: string;
        readonly bindings: readonly Readonly<Record<string, unknown>>[];
      }): Promise<string> {
        const version = await super.uploadControlWorker(spec);
        const inspection = this.inspections.get(spec.scriptName);
        if (!inspection) return version;
        inspection.serviceBindings.reverse();
        inspection.queueProducerBindings.reverse();
        inspection.kvNamespaceBindings.reverse();
        inspection.dispatchNamespaceBindings.reverse();
        inspection.plainTextBindings = Object.fromEntries(
          Object.entries(inspection.plainTextBindings).reverse(),
        );
        for (const binding of inspection.dispatchNamespaceBindings) {
          const outbound = binding.outbound as {
            params?: unknown[];
          };
          outbound.params?.reverse();
        }
        return version;
      }
    }

    await expect(
      provisionPlatformPlane({
        client: new ReorderedBindingsClient(),
        store: new FakePlatformStore(),
        spec: platformSpec(),
      }),
    ).resolves.toMatchObject({
      dispatchArtifactVersion: 'etag:fleet-dispatch',
    });
  });

  it('rejects a role binding retained after the initial upload attestation', async () => {
    class LateBindingDriftClient extends FakePlatformClient {
      override async assertUntrustedDispatchNamespace(): Promise<void> {
        await super.assertUntrustedDispatchNamespace();
        const inspection = this.inspections.get('fleet-audit');
        if (inspection) {
          inspection.serviceBindings = [
            { name: 'UNEXPECTED', service: 'foreign-worker' },
          ];
        }
      }
    }

    await expect(
      provisionPlatformPlane({
        client: new LateBindingDriftClient(),
        store: new FakePlatformStore(),
        spec: platformSpec(),
      }),
    ).rejects.toThrow(/drifted role bindings/);
  });
});
