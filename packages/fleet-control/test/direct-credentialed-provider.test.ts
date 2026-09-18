// SPDX-License-Identifier: Apache-2.0

import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import { cancelBodyWithoutAwait } from '../scripts/direct-credentialed-body-cancel.mjs';
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

describe('the body-cancel leaf', () => {
  beforeAll(() => {
    expect(
      existsSync(new URL('../dist/database-export-store.js', import.meta.url)),
      'run pnpm --filter @proofoftech/fleet-control build before this block',
    ).toBe(true);
  });

  it('hands the body and the reason to the built package on first call', async () => {
    const cancel = vi.fn(() => Promise.resolve());
    const reason = new Error('refusal');
    expect(cancelBodyWithoutAwait({ cancel }, reason)).toBeUndefined();
    await new Promise((resolve) => {
      setImmediate(resolve);
    });
    // The load starts at module evaluation, so a release issued before it
    // resolves lands with it rather than on the next turn.
    await vi.waitFor(() => {
      expect(cancel).toHaveBeenCalledTimes(1);
    });
    expect(cancel).toHaveBeenCalledWith(reason);
  });

  it("issues a release in the calling turn once the leaf's load has resolved", async () => {
    const first = vi.fn(() => Promise.resolve());
    cancelBodyWithoutAwait({ cancel: first }, new Error('warm'));
    await vi.waitFor(() => {
      expect(first).toHaveBeenCalledTimes(1);
    });
    const second = vi.fn(() => Promise.resolve());
    const reason = new Error('refusal');
    cancelBodyWithoutAwait({ cancel: second }, reason);
    expect(second).toHaveBeenCalledWith(reason);
  });

  it('reads a body that carries no cancel and calls nothing', async () => {
    let reads = 0;
    const body = {
      get cancel() {
        reads += 1;
        return undefined;
      },
    };
    const reason = new Error('refusal');
    for (const absent of [undefined, null, {}, 'body'])
      expect(cancelBodyWithoutAwait(absent, reason)).toBeUndefined();
    expect(cancelBodyWithoutAwait(body, reason)).toBeUndefined();
    await new Promise((resolve) => {
      setImmediate(resolve);
    });
    await vi.waitFor(() => {
      expect(reads).toBe(1);
    });
  });
});

// The `dist/` edge the credentialed CLI entries carry is the leaf's dynamic
// `import()`, which resolves at runtime and leaves the static module graph
// free of `dist/`. This walk follows `from`-clause specifiers only, so it
// permits that `import()` and refuses a static specifier that would replace
// it. It reads the CLI's own source tree and decides nothing about what a
// tenant artifact may import.
describe('the credentialed CLI entry module graphs', () => {
  const packageRoot = fileURLToPath(new URL('..', import.meta.url));
  const distPrefix = `${resolve(packageRoot, 'dist')}${sep}`;
  const leaf = resolve(
    packageRoot,
    'scripts/direct-credentialed-body-cancel.mjs',
  );
  const edge =
    /(?:^|\n)\s*(?:import|export)[^;'"]*?\bfrom\s*['"]([^'"]+)['"]/gu;

  function sourceOf(file: string) {
    try {
      return readFileSync(file, 'utf8');
    } catch {
      // A relative specifier naming no readable file — a `.ts` module read
      // through its `.js` specifier — ends the walk at that path. The
      // specifier itself is still checked by the caller.
      return undefined;
    }
  }

  function staticGraph(entry: string) {
    const read = new Set<string>();
    const targets = new Set<string>();
    const queue = [resolve(packageRoot, entry)];
    while (queue.length > 0) {
      const file = queue.pop();
      if (file === undefined || read.has(file)) continue;
      const source = sourceOf(file);
      if (source === undefined) continue;
      read.add(file);
      for (const match of source.matchAll(edge)) {
        const specifier = match[1] ?? '';
        if (!specifier.startsWith('./') && !specifier.startsWith('../'))
          continue;
        const target = resolve(dirname(file), specifier);
        targets.add(target);
        queue.push(target);
      }
    }
    return { read, targets };
  }

  it.each([
    'scripts/direct-credentialed-conformance.mjs',
    'scripts/credentialed-conformance.mjs',
  ])('names no dist/ specifier from %s', (entry) => {
    const { read, targets } = staticGraph(entry);
    expect([...read]).toContain(leaf);
    expect(
      [...targets].filter((target) => target.startsWith(distPrefix)),
    ).toEqual([]);
  });
});
