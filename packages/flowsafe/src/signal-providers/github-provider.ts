// SPDX-License-Identifier: Apache-2.0
// Track E (M-007) — the binding-gated GitHub signal provider, the DL-017
// showcase reference connector. Webhook-only: GitHub pushes events, so there is
// no poll seam. Its signature scheme is GitHub's `X-Hub-Signature-256`
// (`sha256=<hex HMAC-SHA256(secret, rawBody)>`), verified over the RAW request
// bytes BEFORE any parse and CONSTANT-TIME via `crypto.subtle.verify` (the same
// WebCrypto primitive host-kit's verifier uses — never a hand-rolled compare).
//
// It mints NO capability (P8): a GitHub event is untrusted context that lands in
// a thread's inbox as a notification, never an approval. The secret is a
// per-deployment binding/env value; unconfigured ⇒ the host never registers this
// provider ⇒ its webhook route 404s ⇒ byte-identical.

import type { SendNotificationSignalInput } from '@mastra/core/notifications';
import type { SignalSubscription } from '@mastra/core/signals';

import {
  createWebhookSignalProvider,
  type SignalProviderAdapter,
  type WebhookHeaders,
} from './provider.js';

const encoder = new TextEncoder();

/** The GitHub delivery-signature header (lowercased; `Headers.get` is case-insensitive). */
const SIGNATURE_HEADER = 'x-hub-signature-256';
const SIGNATURE_PREFIX = 'sha256=';
/** SHA-256 digest length in bytes — a signature of any other length is malformed. */
const SHA256_BYTES = 32;

/** Decode a lowercase/uppercase hex string to bytes, or undefined if malformed. */
function hexToBytes(hex: string): Uint8Array | undefined {
  if (hex.length === 0 || hex.length % 2 !== 0) return undefined;
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    const byte = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
    // parseInt tolerates trailing garbage; Number.isNaN catches non-hex.
    if (Number.isNaN(byte)) return undefined;
    bytes[index] = byte;
  }
  return bytes;
}

/**
 * Verify GitHub's `X-Hub-Signature-256` over the RAW bytes, constant-time.
 * `crypto.subtle.verify` is the constant-time comparison (never a string ===),
 * so a valid-length forged signature reveals nothing through timing. A missing
 * header, wrong prefix, non-hex, or wrong-length signature all fail closed.
 */
export async function verifyGithubSignature(
  rawBody: Uint8Array,
  headers: WebhookHeaders,
  secret: string,
): Promise<boolean> {
  const header = headers.get(SIGNATURE_HEADER);
  if (header === null || !header.startsWith(SIGNATURE_PREFIX)) return false;
  const signature = hexToBytes(header.slice(SIGNATURE_PREFIX.length));
  if (signature === undefined || signature.length !== SHA256_BYTES)
    return false;
  // Fail closed on an empty secret rather than importing a zero-length HMAC key:
  // Node's WebCrypto THROWS on it, and a runtime that ACCEPTS one would verify
  // against a known (empty) key — a trivial forgery. The webhook route already
  // treats '' as unconfigured; this is the defense-in-depth at the crypto seam.
  if (secret.length === 0) return false;
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['verify'],
  );
  // BufferSource args: a Uint8Array over the exact bytes GitHub signed.
  return crypto.subtle.verify(
    'HMAC',
    key,
    signature as unknown as ArrayBuffer,
    rawBody as unknown as ArrayBuffer,
  );
}

interface GithubRepository {
  full_name?: unknown;
}
interface GithubWebhookPayload {
  repository?: GithubRepository;
  action?: unknown;
  issue?: { number?: unknown };
  pull_request?: { number?: unknown };
  ref?: unknown;
}

/**
 * The external-resource key(s) a GitHub payload concerns — matched against a
 * subscription's `externalResourceId`. A repo event yields `github:<full_name>`;
 * an issue / PR event ALSO yields `github:<full_name>#<number>`, so a
 * subscription may watch the whole repo OR one issue/PR. Nothing usable ⇒ [].
 */
export function extractGithubResourceIds(payload: unknown): string[] {
  if (typeof payload !== 'object' || payload === null) return [];
  const event = payload as GithubWebhookPayload;
  const fullName = event.repository?.full_name;
  if (typeof fullName !== 'string' || fullName.length === 0) return [];
  const keys = [`github:${fullName}`];
  const number = event.issue?.number ?? event.pull_request?.number;
  if (typeof number === 'number') {
    keys.push(`github:${fullName}#${number}`);
  }
  return keys;
}

/** Build the notification a matched GitHub subscription receives. */
export function buildGithubNotification(
  payload: unknown,
  subscription: SignalSubscription,
): SendNotificationSignalInput {
  const event =
    typeof payload === 'object' && payload !== null
      ? (payload as GithubWebhookPayload)
      : {};
  const action = typeof event.action === 'string' ? event.action : undefined;
  // Kind: the action for issue/PR events, 'push' for a ref-bearing push, else a
  // generic 'event'. GitHub's X-GitHub-Event header is the richer source, but
  // the adapter's buildNotification sees only the payload — the action/ref shape
  // is enough for a reference notification.
  const kind = action ?? (typeof event.ref === 'string' ? 'push' : 'event');
  return {
    source: 'github',
    kind,
    summary: `GitHub ${subscription.externalResourceId}: ${kind}`,
    priority: 'medium',
    payload,
    attributes: {
      resource: subscription.externalResourceId,
      ...(action !== undefined ? { action } : {}),
    },
  };
}

export interface GithubSignalProviderOptions {
  /** Provider id (default 'github'); must be a lowercase slug with no '_'. */
  id?: string;
  name?: string;
}

/**
 * The GitHub reference provider: `X-Hub-Signature-256` verification + repo/issue
 * resource extraction + a GitHub notification shape, over the generic webhook
 * provider. The signing secret is NOT held here — the webhook route supplies it
 * per request from the host binding, so an unconfigured deployment simply never
 * registers this provider.
 */
export function githubSignalProvider(
  options: GithubSignalProviderOptions = {},
): SignalProviderAdapter {
  return createWebhookSignalProvider({
    id: options.id ?? 'github',
    name: options.name ?? 'GitHub',
    verifyWebhookSignature: verifyGithubSignature,
    extractResourceIds: extractGithubResourceIds,
    buildNotification: buildGithubNotification,
  });
}
