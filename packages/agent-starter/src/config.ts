// SPDX-License-Identifier: Apache-2.0

import type {
  MastraModelConfig,
  OpenAICompatibleConfig,
} from '@mastra/core/llm';
import {
  hmacVerifier,
  type TokenVerifier,
} from '@proofoftech/flowsafe/host-kit';

export const SYSTEM_PRINCIPAL_ID = 'anchorage-agent-starter';
export const SWEEP_CRON = '*/5 * * * *';
export const PURGE_CRON = '17 * * * *';
export const TICK_CRON = '* * * * *';
export const UNCONFIGURED_MODEL_ID = 'provider/model';

const DEFAULT_SIGNAL_ATTRIBUTES = [
  'source',
  'kind',
  'priority',
  'resource',
  'action',
] as const;

let verifierMemo:
  | {
      key: string;
      verifier: TokenVerifier;
    }
  | undefined;
let githubOwnershipMemo:
  | {
      raw: string;
      resources: readonly string[];
    }
  | undefined;

export function buildVerifier(env: Env): TokenVerifier {
  const key = [
    env.AUTH_HMAC_SECRET ?? '',
    env.AUTH_JWT_ISSUER,
    env.AUTH_JWT_AUDIENCE,
  ].join('\0');
  if (verifierMemo?.key === key) return verifierMemo.verifier;

  const verifier: TokenVerifier = env.AUTH_HMAC_SECRET
    ? hmacVerifier({
        keys: new Map([['primary', env.AUTH_HMAC_SECRET]]),
        issuer: env.AUTH_JWT_ISSUER,
        audience: env.AUTH_JWT_AUDIENCE,
      })
    : { verify: async () => undefined };
  verifierMemo = { key, verifier };
  return verifier;
}

export function modelConfig(env: Env): MastraModelConfig {
  const id = env.MODEL_ID;
  if (id === UNCONFIGURED_MODEL_ID) {
    throw new Error(
      "MODEL_ID is not configured; replace 'provider/model' with the model provider and model ID for this deployment",
    );
  }
  const separator = id.indexOf('/');
  if (separator <= 0 || separator === id.length - 1) {
    throw new Error(
      "MODEL_ID must use Mastra's non-placeholder 'provider/model' form",
    );
  }
  if (!env.MODEL_API_KEY) {
    throw new Error('MODEL_API_KEY is required to start an agent run');
  }
  const model: OpenAICompatibleConfig = {
    id: id as `${string}/${string}`,
    apiKey: env.MODEL_API_KEY,
    ...(env.MODEL_BASE_URL ? { url: env.MODEL_BASE_URL } : {}),
  };
  return model;
}

export function signalAttributeAllowlist(env: Env): readonly string[] {
  const configured = csv(env.SIGNAL_ATTRIBUTE_ALLOWLIST);
  return configured.length > 0 ? configured : DEFAULT_SIGNAL_ATTRIBUTES;
}

export function csv(value: string | undefined): string[] {
  if (!value) return [];
  return [
    ...new Set(
      value
        .split(',')
        .map((part) => part.trim())
        .filter(Boolean),
    ),
  ];
}

export function githubResourceAllowed(
  env: Env,
  externalResourceId: string,
): boolean {
  const raw = env.GITHUB_RESOURCE_ALLOWLIST;
  if (githubOwnershipMemo?.raw !== raw) {
    let resources: readonly string[] = [];
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (
        Array.isArray(parsed) &&
        parsed.every(
          (resource) =>
            typeof resource === 'string' &&
            /^github:[^/#\s]+\/[^/#\s]+$/.test(resource),
        )
      ) {
        resources = [...new Set(parsed)];
      }
    } catch {
      // Invalid configuration is an empty allowlist, never allow-all.
    }
    githubOwnershipMemo = { raw, resources };
  }
  return githubOwnershipMemo.resources.some(
    (resource) =>
      externalResourceId === resource ||
      externalResourceId.startsWith(`${resource}#`),
  );
}
