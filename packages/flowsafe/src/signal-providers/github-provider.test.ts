// SPDX-License-Identifier: Apache-2.0
import type { SignalSubscription } from '@mastra/core/signals';
import { describe, expect, it } from 'vitest';

import {
  buildGithubNotification,
  extractGithubResourceIds,
  githubSignalProvider,
  verifyGithubSignature,
} from './github-provider.js';

const encoder = new TextEncoder();

/** Compute a valid GitHub `X-Hub-Signature-256` (WebCrypto — no node:crypto). */
async function githubSign(secret: string, body: Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', key, body));
  const hex = [...sig].map((b) => b.toString(16).padStart(2, '0')).join('');
  return `sha256=${hex}`;
}

function headers(map: Record<string, string>): {
  get(n: string): string | null;
} {
  return new Headers(map);
}

const SECRET = 'top-secret';

describe('verifyGithubSignature', () => {
  it.each([
    ['UTF-8 JSON', encoder.encode('{"repository":{"full_name":"acme/repo"}}')],
    ['non-ASCII text', encoder.encode('{"message":"مرحبا 🌊"}')],
    ['UTF-8 BOM', new Uint8Array([0xef, 0xbb, 0xbf, 0x7b, 0x7d])],
    [
      'invalid UTF-8 bytes',
      new Uint8Array([0x7b, 0x22, 0x78, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d]),
    ],
    ['empty input', new Uint8Array()],
  ])('accepts a correctly signed %s body', async (_label, body) => {
    const signature = await githubSign(SECRET, body);

    await expect(
      verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': signature }),
        SECRET,
      ),
    ).resolves.toBe(true);
  });

  it('accepts lowercase and uppercase canonical digest hex', async () => {
    const body = encoder.encode('{}');
    const lowercase = await githubSign(SECRET, body);
    const uppercase = `sha256=${lowercase.slice('sha256='.length).toUpperCase()}`;

    await expect(
      verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': lowercase }),
        SECRET,
      ),
    ).resolves.toBe(true);
    await expect(
      verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': uppercase }),
        SECRET,
      ),
    ).resolves.toBe(true);
  });

  it('rejects a signature for a DIFFERENT secret (constant-time verify)', async () => {
    // #given
    const body = encoder.encode('{"a":1}');
    const signature = await githubSign('other-secret', body);
    // #then
    expect(
      await verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': signature }),
        SECRET,
      ),
    ).toBe(false);
  });

  it('rejects a signature over DIFFERENT bytes', async () => {
    // #given — signed the original, present it over a tampered body
    const original = encoder.encode('{"a":1}');
    const signature = await githubSign(SECRET, original);
    const tampered = encoder.encode('{"a":2}');
    // #then
    expect(
      await verifyGithubSignature(
        tampered,
        headers({ 'x-hub-signature-256': signature }),
        SECRET,
      ),
    ).toBe(false);
  });

  it('fails closed on a missing header, wrong prefix, non-hex, or wrong length', async () => {
    // #given
    const body = encoder.encode('{}');
    // #then
    expect(await verifyGithubSignature(body, headers({}), SECRET)).toBe(false);
    expect(
      await verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': 'sha1=abcd' }),
        SECRET,
      ),
    ).toBe(false);
    expect(
      await verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': 'sha256=zzzz' }),
        SECRET,
      ),
    ).toBe(false);
    // valid hex but only 2 bytes, not 32
    expect(
      await verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': 'sha256=abcd' }),
        SECRET,
      ),
    ).toBe(false);
    // ODD-length hex (cannot decode to whole bytes)
    expect(
      await verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': 'sha256=abc' }),
        SECRET,
      ),
    ).toBe(false);
    // empty secret must not throw — fail closed
    expect(
      await verifyGithubSignature(
        body,
        headers({ 'x-hub-signature-256': `sha256=${'0'.repeat(64)}` }),
        '',
      ),
    ).toBe(false);
  });

  it.each([
    `sha256=${'0g'.repeat(32)}`,
    `sha256=${'aZ'.repeat(32)}`,
    `zsha256=${'00'.repeat(32)}`,
    `SHA256=${'00'.repeat(32)}`,
    `sHa256=${'00'.repeat(32)}`,
    `sha256=${'00'.repeat(32)}z`,
    `sha256=${'00'.repeat(31)}`,
    `sha256=${'00'.repeat(33)}`,
    `sha256= ${'00'.repeat(32)}`,
  ])('rejects non-canonical signature representation %s', async (signature) => {
    await expect(
      verifyGithubSignature(
        encoder.encode('{}'),
        headers({ 'x-hub-signature-256': signature }),
        SECRET,
      ),
    ).resolves.toBe(false);
  });
});

describe('extractGithubResourceIds', () => {
  it('yields the repo key, and ALSO the issue/PR key when present', () => {
    // #then
    expect(
      extractGithubResourceIds({ repository: { full_name: 'acme/repo' } }),
    ).toEqual(['github:acme/repo']);
    expect(
      extractGithubResourceIds({
        repository: { full_name: 'acme/repo' },
        issue: { number: 12 },
      }),
    ).toEqual(['github:acme/repo', 'github:acme/repo#12']);
    expect(
      extractGithubResourceIds({
        repository: { full_name: 'acme/repo' },
        pull_request: { number: 7 },
      }),
    ).toEqual(['github:acme/repo', 'github:acme/repo#7']);
  });

  it('yields nothing when the payload names no repository', () => {
    // #then
    expect(extractGithubResourceIds({})).toEqual([]);
    expect(extractGithubResourceIds(null)).toEqual([]);
    expect(extractGithubResourceIds('nope')).toEqual([]);
  });
});

describe('buildGithubNotification', () => {
  const subscription: SignalSubscription = {
    id: 'acme_s1',
    providerId: 'github',
    threadId: 'acme_t1',
    resourceId: 'acme_u1',
    externalResourceId: 'github:acme/repo',
    subscribedAt: new Date(0),
    metadata: {},
  };

  it('derives kind from the action and carries source/attributes', () => {
    // #when
    const notification = buildGithubNotification(
      { action: 'opened', repository: { full_name: 'acme/repo' } },
      subscription,
    );
    // #then
    expect(notification.source).toBe('github');
    expect(notification.kind).toBe('opened');
    expect(notification.summary).toContain('github:acme/repo');
    expect(notification.attributes).toMatchObject({
      resource: 'github:acme/repo',
      action: 'opened',
    });
  });

  it("uses 'push' for a ref-bearing payload and 'event' otherwise", () => {
    // #then
    expect(
      buildGithubNotification({ ref: 'refs/heads/main' }, subscription).kind,
    ).toBe('push');
    expect(buildGithubNotification({}, subscription).kind).toBe('event');
  });
});

describe('githubSignalProvider', () => {
  it('exposes the webhook adapter seam with a default id', () => {
    // #when
    const provider = githubSignalProvider();
    // #then
    expect(provider.id).toBe('github');
    expect(typeof provider.verifyWebhookSignature).toBe('function');
    expect(typeof provider.extractResourceIds).toBe('function');
    expect(typeof provider.buildNotification).toBe('function');
    // webhook-only: no poll seam
    expect(provider.pollForDeliveries).toBeUndefined();
    expect(provider.pollInterval).toBeUndefined();
  });
});
