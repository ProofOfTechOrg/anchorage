// SPDX-License-Identifier: Apache-2.0

import { readFileSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import ts from 'typescript';
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
import { expectBuiltDist } from './fixtures/built-dist.js';

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
    expectBuiltDist(
      '../dist/database-export-store.js',
      import.meta.url,
      'run pnpm --filter @proofoftech/fleet-control build before this block',
    );
  });

  it('hands the body and reason through the pending module load', async () => {
    const cancel = vi.fn(() => Promise.resolve());
    const reason = new Error('refusal');
    expect(cancelBodyWithoutAwait({ cancel }, reason)).toBeUndefined();
    await new Promise((resolveOnImmediate) => {
      setImmediate(resolveOnImmediate);
    });
    // The load starts at module evaluation, so a release issued before it
    // resolves lands with it rather than on the next turn.
    await vi.waitFor(() => {
      expect(cancel).toHaveBeenCalledTimes(1);
    });
    expect(cancel).toHaveBeenCalledWith(reason);
  });

  it('issues a release in the calling turn once the callable is cached', async () => {
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
    await new Promise((resolveOnImmediate) => {
      setImmediate(resolveOnImmediate);
    });
    await vi.waitFor(() => {
      expect(reads).toBe(1);
    });
  });
});

// Static import and export declarations must keep the CLI entry graphs free
// of dist targets. Dynamic imports in the body-cancel leaf and credentialed
// CLI resolve at runtime; this control does not inspect tenant artifacts.
describe('the credentialed CLI entry module graphs', () => {
  const packageRoot = fileURLToPath(new URL('..', import.meta.url));
  const distRoot = resolve(packageRoot, 'dist');
  const distPrefix = `${distRoot}${sep}`;
  const leaf = resolve(
    packageRoot,
    'scripts/direct-credentialed-body-cancel.mjs',
  );

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

  function expectOutsideDist(target: string) {
    expect(
      target === distRoot || target.startsWith(distPrefix),
      `static declaration resolves into dist: ${target}`,
    ).toBe(false);
  }

  function staticGraph(entry: string, readSource = sourceOf) {
    const read = new Set<string>();
    const targets = new Set<string>();
    const queue = [resolve(packageRoot, entry)];
    while (queue.length > 0) {
      const file = queue.pop();
      if (file === undefined || read.has(file)) continue;
      const source = readSource(file);
      if (source === undefined) continue;
      read.add(file);
      const parsed = ts.createSourceFile(
        file,
        source,
        ts.ScriptTarget.ESNext,
        false,
      );
      for (const statement of parsed.statements) {
        if (
          (!ts.isImportDeclaration(statement) &&
            !ts.isExportDeclaration(statement)) ||
          !statement.moduleSpecifier ||
          !ts.isStringLiteral(statement.moduleSpecifier)
        )
          continue;
        const specifier = statement.moduleSpecifier.text;
        if (!specifier.startsWith('./') && !specifier.startsWith('../'))
          continue;
        const target = fileURLToPath(new URL(specifier, pathToFileURL(file)));
        targets.add(target);
        expectOutsideDist(target);
        queue.push(target);
      }
    }
    return { read, targets };
  }

  it.each([
    'scripts/direct-credentialed-conformance.mjs',
    'scripts/credentialed-conformance.mjs',
  ])('names no dist/ specifier from %s', (entry) => {
    const { read } = staticGraph(entry);
    expect([...read]).toContain(leaf);
  });

  function fixtureSource(
    source: string,
    nested = 'export const value = true;',
  ) {
    const files = new Map([
      [resolve(packageRoot, 'scripts/graph-fixture.mjs'), source],
      [resolve(packageRoot, 'scripts/nested.mjs'), nested],
    ]);
    return (file: string) => files.get(file);
  }

  it.each([
    ['single-quoted side-effect import', "import '../dist/index.js';"],
    ['double-quoted side-effect import', 'import "../dist/index.js";'],
    ['from import', "import { value } from '../dist/index.js';"],
    ['named re-export', "export { value } from '../dist/index.js';"],
    ['star re-export', "export * from '../dist/index.js';"],
    ['exact dist directory', "import '../dist';"],
    ['dist directory with slash', "export * from '../dist/';"],
    ['escaped directory', String.raw`import '../\x64ist/index.js';`],
    [
      'encoded directory with query and fragment',
      "import '../%64ist/index.js?probe=1#fragment';",
    ],
    ['normalized dot segments', "import '../scripts/../dist/index.js';"],
  ])('refuses %s through the graph assertion', (_name, source) => {
    const readSource = vi.fn(fixtureSource(source));
    expect(() => staticGraph('scripts/graph-fixture.mjs', readSource)).toThrow(
      'static declaration resolves into dist:',
    );
    expect(readSource).toHaveBeenCalledTimes(1);
  });

  it('refuses a dist re-export reached through a relative module', () => {
    expect(() =>
      staticGraph(
        'scripts/graph-fixture.mjs',
        fixtureSource(
          "import './nested.mjs';",
          "export * from '../dist/index.js';",
        ),
      ),
    ).toThrow('static declaration resolves into dist:');
  });

  it('allows relative modules, dist-extra, and non-static import text', () => {
    const source = `
import './nested.mjs';
export * from '../dist-extra/index.js';
import 'a-package/dist';
// import '../dist/comment.js';
/* export * from '../dist/block-comment.js'; */
const text = \`
import '../dist/template.js';
\`;
void import('../dist/dynamic.js');
`;
    const graph = staticGraph(
      'scripts/graph-fixture.mjs',
      fixtureSource(source),
    );
    expect([...graph.read]).toContain(
      resolve(packageRoot, 'scripts/nested.mjs'),
    );
    expect([...graph.targets]).toEqual([
      resolve(packageRoot, 'scripts/nested.mjs'),
      resolve(packageRoot, 'dist-extra/index.js'),
    ]);
  });
});
