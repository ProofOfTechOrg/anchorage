// SPDX-License-Identifier: Apache-2.0

import {
  DEPLOYMENT_TAG_PATTERN,
  isDeploymentEnvironment,
} from '../deployment-identity-protocol.js';
import {
  credentialsMatch,
  DEPLOYMENT_IDENTITY_HEADER,
  deploymentIdentityHeaders,
} from '../do-runner/deployment-identity.js';
import { readBoundedBody } from '../http-body.js';
import type { AuditQueue } from './queue-types.js';

const AUDIT_PROXY_PATH = '/internal/audit';
const MAX_AUDIT_PROXY_BODY_BYTES = 120 * 1_024;
const AUDIT_DECISIONS = new Set(['allowed', 'denied', 'error']);
const SCRIPT_NAME_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
export const AUDIT_PROXY_INSTANCE_NAME = 'flowsafe-fleet-audit-proxy';

export interface AuditProxyStubLike {
  fetch(request: Request): Promise<Response>;
}

export type AuditProxyNamespaceLike =
  | Readonly<{
      idFromName(name: string): unknown;
      get(id: unknown): AuditProxyStubLike;
    }>
  | Readonly<{
      getByName(name: string): AuditProxyStubLike;
    }>;

/** Fixed-target binding consumed by the audit queue adapter. */
export interface AuditProxyBinding {
  fetch(request: Request): Promise<Response>;
}

/** Explicit compatibility type for an ordinary private service binding. */
export interface AuditProxyServiceBinding {
  fetch(request: Request): Promise<Response>;
}

export interface AuditProxyAttribution {
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
}

export interface InfrastructureAuditEnvelope {
  readonly fleetAttribution: Readonly<{
    source: 'external-candidate-via-trusted-proxy';
    eventTrust: 'untrusted';
    tenantTag: string;
    environment: string;
    scriptName: string;
  }>;
  readonly event: Readonly<Record<string, unknown>>;
}

export interface AuditProxyDurableObjectState {
  readonly id: { readonly name?: string };
}

export interface AuditProxyDurableObjectEnv {
  readonly AUDIT_QUEUE: AuditQueue<InfrastructureAuditEnvelope>;
  readonly DEPLOYMENT_IDENTITY_SECRET: string;
  readonly DEPLOYMENT_TENANT: string;
  readonly FLEET_ENVIRONMENT: string;
  readonly FLEET_DEPLOYMENT_SCRIPT: string;
}

export type AuditProxyDurableObjectConstructor = new (
  state: AuditProxyDurableObjectState,
  env: AuditProxyDurableObjectEnv,
) => { fetch(request: Request): Promise<Response> };

/**
 * Bind an external candidate to the one trusted audit object. The returned
 * surface has no object-id or object-name parameter, so candidate code cannot
 * address an arbitrary object through this adapter.
 */
export function createAuditProxyDurableObjectBinding(
  namespace: AuditProxyNamespaceLike,
): AuditProxyBinding {
  let stub: AuditProxyStubLike;
  if ('getByName' in namespace && typeof namespace.getByName === 'function') {
    stub = namespace.getByName(AUDIT_PROXY_INSTANCE_NAME);
  } else if (
    'idFromName' in namespace &&
    typeof namespace.idFromName === 'function' &&
    'get' in namespace &&
    typeof namespace.get === 'function'
  ) {
    stub = namespace.get(namespace.idFromName(AUDIT_PROXY_INSTANCE_NAME));
  } else {
    throw new Error('audit proxy binding is not a Durable Object namespace');
  }
  return { fetch: (request) => stub.fetch(request) };
}

/** Keep the pre-existing private-service seam explicit for ordinary hosts. */
export function createAuditProxyServiceBinding(
  service: AuditProxyServiceBinding,
): AuditProxyBinding {
  return { fetch: (request) => service.fetch(request) };
}

function validEvent(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const event = value as Record<string, unknown>;
  return (
    typeof event.action === 'string' &&
    event.action.length > 0 &&
    typeof event.resource === 'string' &&
    event.resource.length > 0 &&
    typeof event.decision === 'string' &&
    AUDIT_DECISIONS.has(event.decision)
  );
}

export function createAuditProxyQueue<TEvent>(
  binding: AuditProxyBinding,
  deploymentIdentitySecret: string,
): AuditQueue<TEvent> {
  const headers = deploymentIdentityHeaders(deploymentIdentitySecret, {
    'content-type': 'application/json',
  });
  return {
    async send(event) {
      const response = await binding.fetch(
        new Request(`http://audit-proxy${AUDIT_PROXY_PATH}`, {
          method: 'POST',
          headers,
          body: JSON.stringify(event),
        }),
      );
      if (!response.ok) {
        throw new Error(`audit proxy rejected event with ${response.status}`);
      }
    },
  };
}

/** Trusted state Durable Object exported under the fleet's fixed class name. */
export class FlowsafeFleetAuditProxy {
  readonly #state: AuditProxyDurableObjectState;
  readonly #handler: (request: Request) => Promise<Response>;

  constructor(
    state: AuditProxyDurableObjectState,
    env: AuditProxyDurableObjectEnv,
  ) {
    this.#state = state;
    this.#handler = createAuditProxyHandler({
      queue: env.AUDIT_QUEUE,
      deploymentIdentitySecret: env.DEPLOYMENT_IDENTITY_SECRET,
      attribution: {
        tenantTag: env.DEPLOYMENT_TENANT,
        environment: env.FLEET_ENVIRONMENT,
        scriptName: env.FLEET_DEPLOYMENT_SCRIPT,
      },
    });
  }

  async fetch(request: Request): Promise<Response> {
    if (this.#state.id.name !== AUDIT_PROXY_INSTANCE_NAME) {
      throw new Error(
        `audit proxy must be addressed as '${AUDIT_PROXY_INSTANCE_NAME}'`,
      );
    }
    return this.#handler(request);
  }
}

export function createAuditProxyDurableObject(): AuditProxyDurableObjectConstructor {
  return FlowsafeFleetAuditProxy;
}

export function createAuditProxyHandler(options: {
  readonly queue: AuditQueue<InfrastructureAuditEnvelope>;
  readonly deploymentIdentitySecret: string;
  readonly attribution: AuditProxyAttribution;
}): (request: Request) => Promise<Response> {
  deploymentIdentityHeaders(options.deploymentIdentitySecret);
  if (
    !DEPLOYMENT_TAG_PATTERN.test(options.attribution.tenantTag) ||
    !isDeploymentEnvironment(options.attribution.environment) ||
    !SCRIPT_NAME_PATTERN.test(options.attribution.scriptName)
  ) {
    throw new Error('audit proxy attribution is invalid');
  }
  return async (request) => {
    const url = new URL(request.url);
    if (url.pathname !== AUDIT_PROXY_PATH) {
      return new Response('not found', { status: 404 });
    }
    if (request.method !== 'POST') {
      return new Response('method not allowed', { status: 405 });
    }
    const actual = request.headers.get(DEPLOYMENT_IDENTITY_HEADER);
    if (
      !actual ||
      !(await credentialsMatch(actual, options.deploymentIdentitySecret))
    ) {
      return new Response('authentication required', { status: 401 });
    }
    const body = await readBoundedBody(
      request,
      MAX_AUDIT_PROXY_BODY_BYTES,
      'audit event exceeds proxy limit',
    );
    if (!body.ok) {
      return new Response(
        body.reason === 'payload-too-large'
          ? 'payload too large'
          : 'invalid request body',
        { status: body.reason === 'payload-too-large' ? 413 : 400 },
      );
    }
    let event: unknown;
    try {
      event = JSON.parse(body.text);
    } catch {
      return new Response('invalid request body', { status: 400 });
    }
    if (!validEvent(event)) {
      return new Response('invalid audit event', { status: 400 });
    }
    const attributed: InfrastructureAuditEnvelope = {
      fleetAttribution: {
        source: 'external-candidate-via-trusted-proxy',
        eventTrust: 'untrusted',
        ...options.attribution,
      },
      event,
    };
    try {
      await options.queue.send(attributed);
    } catch {
      return new Response('audit delivery unavailable', { status: 503 });
    }
    return new Response(null, { status: 204 });
  };
}
