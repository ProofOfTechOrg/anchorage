// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  DIRECT_REFERENCE_BODY_LIMIT,
  DIRECT_REFERENCE_PATH,
  type DirectReferenceAction,
  DirectReferenceRequestError,
  readDirectReferenceRequest,
} from '../scripts/direct-reference-contract.mjs';

const CONFIG = 'a'.repeat(64);
const actions: readonly DirectReferenceAction[] = [
  { kind: 'control-read' },
  { kind: 'provision', role: 'a', release: 'initial' },
  { kind: 'provision', role: 'b', release: 'initial' },
  { kind: 'provision', role: 'recovery', release: 'failed-recovery' },
  { kind: 'inventory-start', slot: 'inventory-before' },
  { kind: 'inventory-continue', slot: 'inventory-before' },
  { kind: 'inventory-read', slot: 'inventory-after' },
  { kind: 'audit-start', slot: 'audit-before' },
  { kind: 'audit-continue', slot: 'audit-after', token: null },
  { kind: 'audit-page', slot: 'audit-before', limit: 1 },
  { kind: 'audit-abandon', slot: 'audit-after' },
  { kind: 'migration-start' },
  { kind: 'migration-continue' },
  {
    kind: 'migration-page',
    limit: 1_000,
    afterOrdinal: Number.MAX_SAFE_INTEGER - 1,
  },
  { kind: 'migration-abandon' },
  { kind: 'cleanup-start', role: 'recovery' },
  {
    kind: 'cleanup-continue',
    role: 'a',
    token: { operationId: 'unvalidated-claim', revision: 1 },
  },
  { kind: 'cleanup-restart-blocked', role: 'b', token: false },
  { kind: 'cleanup-receipt', role: 'recovery' },
  { kind: 'decommission-start', role: 'a' },
  { kind: 'decommission-export', role: 'a' },
  { kind: 'decommission-continue', role: 'b' },
  { kind: 'decommission-restart-blocked', role: 'recovery', token: [] },
  { kind: 'force-recovery' },
  { kind: 'force-observe' },
  { kind: 'recover-force-residual' },
  { kind: 'tenant-probe', role: 'a', operation: 'health' },
  { kind: 'tenant-probe', role: 'b', operation: 'object-put' },
  { kind: 'tenant-probe', role: 'recovery', operation: 'object-read' },
  { kind: 'tenant-probe', role: 'a', operation: 'object-delete' },
];

function request(body: NonNullable<RequestInit['body']>) {
  return new Request(`https://reference.example.test${DIRECT_REFERENCE_PATH}`, {
    method: 'POST',
    body,
  });
}

function envelope(action: unknown) {
  return { contractVersion: 1, configSha256: CONFIG, action };
}

async function read(action: unknown) {
  return readDirectReferenceRequest(
    request(JSON.stringify(envelope(action))),
    CONFIG,
  );
}

describe('direct reference request contract', () => {
  it.each(actions)('reads fixed action $kind', async (action) => {
    const result = await read(action);
    expect(result).toEqual(envelope(action));
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.action)).toBe(true);
  });

  it.each(
    actions,
  )('rejects extra fields for $kind without echoing supplied data', async (action) => {
    await expect(
      read({ ...action, providerUrl: 'secret-sentinel' }),
    ).rejects.toMatchObject({
      code: 'invalid-request',
      message: 'invalid-request',
    });
  });

  it('preserves an omitted token separately from an explicit malformed claim', async () => {
    const base = { kind: 'migration-continue' };
    expect(Object.hasOwn((await read(base)).action, 'token')).toBe(false);
    for (const token of [
      null,
      false,
      0,
      '',
      [],
      { operationId: 'foreign', version: 99 },
    ]) {
      const result = await read({ ...base, token });
      expect(Object.hasOwn(result.action, 'token')).toBe(true);
      expect(Reflect.get(result.action, 'token')).toEqual(token);
    }
    await expect(
      read({ kind: 'cleanup-restart-blocked', role: 'a' }),
    ).rejects.toMatchObject({ code: 'invalid-request' });
  });

  it.each([
    {},
    null,
    [],
    { kind: 'toString' },
    { kind: 'constructor' },
    { kind: 'provision', role: 'a', release: 'failed-recovery' },
    { kind: 'provision', role: 'other', release: 'initial' },
    { kind: 'provision', role: 'a', release: 'next' },
    { kind: 'inventory-start', slot: 'audit-before' },
    { kind: 'audit-continue', slot: 'inventory-before' },
    { kind: 'migration-start', slot: 'migration-next' },
    { kind: 'cleanup-start', role: 'other' },
    { kind: 'decommission-continue' },
    { kind: 'decommission-export', role: 'other' },
    { kind: 'decommission-export', role: 'a', view: 'bytes' },
    { kind: 'force-recovery', role: 'a' },
    { kind: 'tenant-probe', role: 'a' },
    { kind: 'tenant-probe', role: 'other', operation: 'health' },
    { kind: 'tenant-probe', role: 'a', operation: 'other' },
    { kind: 'tenant-probe', role: 'a', operation: null },
  ])('refuses an invalid action %j', async (action) => {
    await expect(read(action)).rejects.toBeInstanceOf(
      DirectReferenceRequestError,
    );
  });

  it.each([
    0,
    -1,
    1.5,
    null,
    '1',
    1001,
  ])('rejects invalid page limit %j', async (limit) => {
    await expect(
      read({ kind: 'audit-page', slot: 'audit-before', limit }),
    ).rejects.toMatchObject({ code: 'invalid-request' });
    await expect(read({ kind: 'migration-page', limit })).rejects.toMatchObject(
      { code: 'invalid-request' },
    );
  });

  it.each([
    -1,
    1.5,
    null,
    '0',
    Number.MAX_SAFE_INTEGER,
  ])('rejects invalid page cursor %j', async (afterOrdinal) => {
    await expect(
      read({ kind: 'migration-page', limit: 1, afterOrdinal }),
    ).rejects.toMatchObject({ code: 'invalid-request' });
  });

  it('refuses wrong root keys, version and run binding', async () => {
    const valid = envelope({ kind: 'control-read' });
    for (const body of [
      { ...valid, extra: 'secret-sentinel' },
      { ...valid, contractVersion: 2 },
      { ...valid, configSha256: 'bad' },
      { action: valid.action },
    ])
      await expect(
        readDirectReferenceRequest(request(JSON.stringify(body)), CONFIG),
      ).rejects.toMatchObject({ code: 'invalid-request' });
    await expect(
      readDirectReferenceRequest(
        request(JSON.stringify(valid)),
        'b'.repeat(64),
      ),
    ).rejects.toMatchObject({ code: 'run-binding-mismatch' });
    const proto = JSON.parse(
      '{"kind":"control-read","__proto__":{"secret":"sentinel"}}',
    );
    await expect(read(proto)).rejects.toMatchObject({
      code: 'invalid-request',
    });
  });

  it('accepts the body-byte boundary and refuses one extra byte', async () => {
    const json = JSON.stringify(envelope({ kind: 'control-read' }));
    const body =
      json + ' '.repeat(DIRECT_REFERENCE_BODY_LIMIT - Buffer.byteLength(json));
    await expect(
      readDirectReferenceRequest(request(body), CONFIG),
    ).resolves.toEqual(envelope({ kind: 'control-read' }));
    await expect(
      readDirectReferenceRequest(request(`${body} `), CONFIG),
    ).rejects.toMatchObject({ code: 'payload-too-large' });
  });

  it('refuses invalid UTF-8, invalid JSON and stream errors with fixed errors', async () => {
    await expect(
      readDirectReferenceRequest(request(new Uint8Array([0xc3, 0x28])), CONFIG),
    ).rejects.toMatchObject({ code: 'invalid-utf8' });
    await expect(
      readDirectReferenceRequest(request('{"secret-sentinel"'), CONFIG),
    ).rejects.toMatchObject({
      code: 'invalid-request',
      message: 'invalid-request',
    });
    const body = new ReadableStream<Uint8Array>({
      pull() {
        throw new Error('secret-sentinel');
      },
    });
    const streamed = new Request('https://reference.example.test', {
      method: 'POST',
      body,
      duplex: 'half',
    } as RequestInit);
    await expect(
      readDirectReferenceRequest(streamed, CONFIG),
    ).rejects.toMatchObject({
      code: 'invalid-request',
      message: 'invalid-request',
    });
  });
});
