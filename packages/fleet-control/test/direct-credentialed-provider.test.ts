// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  openDirectProviderSession,
  singlePage,
} from '../scripts/direct-credentialed-provider.mjs';

const sessions: Awaited<ReturnType<typeof openDirectProviderSession>>[] = [];

async function fixture(envelope: Record<string, unknown>) {
  const fetchRequest = vi.fn<typeof fetch>(async () => Response.json(envelope));
  const session = await openDirectProviderSession({
    apiToken: 'inert-provider-token',
    fetchRequest,
    timeoutMs: 5_000,
  });
  sessions.push(session);
  return { session, fetchRequest };
}

beforeEach(() => {
  vi.stubGlobal('fetch', () => {
    throw new Error('unexpected network');
  });
});

afterEach(() => {
  for (const session of sessions.splice(0)) session.transport.close();
  vi.unstubAllGlobals();
});

describe.each([
  'object',
  'numbered',
  'single',
] as const)('direct provider %s envelopes through the native SDK', (shape) => {
  const row = { id: 'account' };
  const result = shape === 'object' ? row : [];
  const read = (
    session: Awaited<ReturnType<typeof openDirectProviderSession>>,
  ) =>
    shape === 'object'
      ? session.sdk.accounts.get({ account_id: 'account' })
      : session[shape].zones.list().then((page) => page.result);

  it.each([
    undefined,
    null,
  ])('accepts null errors and messages with result_info %s', async (info) => {
    const { session, fetchRequest } = await fixture({
      success: true,
      errors: null,
      messages: null,
      result,
      result_info: info,
    });
    await expect(read(session)).resolves.toEqual(result);
    expect(fetchRequest).toHaveBeenCalledTimes(1);
    expect(
      new Headers(fetchRequest.mock.calls[0]?.[1]?.headers).has(
        'accept-encoding',
      ),
    ).toBe(false);
  });

  it.each([
    { kind: 'non-empty array', errors: [{ code: 1000, message: 'x' }] },
    { kind: 'non-array', errors: 'bad' },
  ])('refuses $kind errors', async ({ errors }) => {
    const { session } = await fixture({ success: true, result, errors });
    await expect(read(session)).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
  });
});

describe('single page attestation', () => {
  const rows = [{ queue_name: 'queue' }];
  const listed = async (envelope: Record<string, unknown>) => {
    const { session } = await fixture({
      success: true,
      errors: [],
      ...envelope,
    });
    return singlePage(session.single.queues.list({ account_id: 'account' }));
  };

  it.each([
    {
      kind: 'a total_count equal to the rows',
      result_info: { total_count: 1 },
    },
    { kind: 'a single total_page', result_info: { total_pages: 1 } },
  ])('records an exhaustive page for $kind', async ({ result_info }) => {
    await expect(listed({ result: rows, result_info })).resolves.toEqual({
      rows,
      exhaustive: true,
    });
  });

  it('records an exhaustive empty page for a zero total_pages', async () => {
    await expect(
      listed({ result: [], result_info: { total_pages: 0 } }),
    ).resolves.toEqual({ rows: [], exhaustive: true });
  });

  it.each([
    { kind: 'an absent', envelope: {} },
    { kind: 'a null', envelope: { result_info: null } },
    { kind: 'an empty', envelope: { result_info: {} } },
    {
      kind: 'a count-only',
      envelope: { result_info: { count: 1, per_page: 50 } },
    },
  ])('records a non-exhaustive page for $kind result_info', async ({
    envelope,
  }) => {
    await expect(listed({ result: rows, ...envelope })).resolves.toEqual({
      rows,
      exhaustive: false,
    });
  });

  it.each([
    { kind: 'more than one page', result_info: { total_pages: 2 } },
    { kind: 'a total_count above the rows', result_info: { total_count: 2 } },
  ])('refuses $kind', async ({ result_info }) => {
    await expect(listed({ result: rows, result_info })).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
  });
});

it('requests identity encoding for the export object GET through the native SDK', async () => {
  const { session, fetchRequest } = await fixture({});
  fetchRequest.mockResolvedValueOnce(new Response('SQL'));
  const response = await session
    .exportReader(3)
    .r2.buckets.objects.get('receipt.sql', {
      account_id: 'account',
      bucket_name: 'exports',
      jurisdiction: 'default',
    });
  expect(await response.text()).toBe('SQL');
  expect(fetchRequest).toHaveBeenCalledTimes(1);
  const call = fetchRequest.mock.calls[0];
  if (!call) throw new Error('object GET absent');
  const [input, init] = call;
  const request = new Request(input, init);
  expect(request.method).toBe('GET');
  expect(request.headers.get('accept-encoding')).toBe('identity');
  expect(request.headers.get('accept')).toBe('application/octet-stream');
  expect(request.headers.get('authorization')).toBe(
    'Bearer inert-provider-token',
  );
  expect(request.headers.get('cf-r2-jurisdiction')).toBe('default');
});
