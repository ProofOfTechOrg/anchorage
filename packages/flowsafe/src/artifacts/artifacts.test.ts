// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import {
  type ArtifactBucket,
  type ArtifactBucketListResult,
  InMemoryArtifactBucket,
  InvalidArtifactRefError,
  R2ArtifactStore,
} from './index.js';

const REF = { workflowId: 'wf', runId: 'run-1', name: 'reports/summary.md' };

function makeStore(): R2ArtifactStore {
  return new R2ArtifactStore(new InMemoryArtifactBucket());
}

describe('R2ArtifactStore key validation', () => {
  it.each([
    ['workflowId', { ...REF, workflowId: 'a/b' }],
    ['runId', { ...REF, runId: '..' }],
    ['empty name', { ...REF, name: '' }],
    ['dot-dot segment', { ...REF, name: 'a/../b' }],
    ['empty segment', { ...REF, name: 'a//b' }],
    ['trailing slash', { ...REF, name: 'a/' }],
  ])('rejects %s', async (_label, ref) => {
    // #given
    const store = makeStore();

    // #when / #then
    await expect(store.put(ref, 'x')).rejects.toBeInstanceOf(
      InvalidArtifactRefError,
    );
  });

  it('rejects non-string ids and names without RegExp coercion', async () => {
    const store = makeStore();

    await expect(
      store.put({ ...REF, runId: 123 as never }, 'x'),
    ).rejects.toBeInstanceOf(InvalidArtifactRefError);
    await expect(
      store.put({ ...REF, name: 123 as never }, 'x'),
    ).rejects.toBeInstanceOf(InvalidArtifactRefError);
  });

  it('rejects keys past the R2 1024-byte limit', async () => {
    // #given
    const store = makeStore();
    const name = Array.from({ length: 6 }, () => 'x'.repeat(200)).join('/');

    // #when / #then
    await expect(store.put({ ...REF, name }, 'x')).rejects.toBeInstanceOf(
      InvalidArtifactRefError,
    );
  });

  it('rejects a non-path-safe keyPrefix at construction', () => {
    // #when / #then
    expect(
      () =>
        new R2ArtifactStore(new InMemoryArtifactBucket(), { keyPrefix: '..' }),
    ).toThrow(InvalidArtifactRefError);
  });
});

describe('R2ArtifactStore put/get', () => {
  it('round-trips a string body with content type and metadata', async () => {
    // #given
    const store = makeStore();

    // #when
    const record = await store.put(REF, '# Summary', {
      contentType: 'text/markdown',
      metadata: { producedBy: 'step:report' },
    });
    const content = await store.get(REF);

    // #then
    expect(record).toMatchObject({
      ...REF,
      key: 'wf/run-1/reports/summary.md',
      size: 9,
      contentType: 'text/markdown',
      metadata: { producedBy: 'step:report' },
    });
    expect(record.uploadedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(await content?.text()).toBe('# Summary');
    expect(content?.record.contentType).toBe('text/markdown');
  });

  it.each([
    ['ArrayBuffer', new TextEncoder().encode('bytes').buffer as ArrayBuffer],
    ['ArrayBufferView', new TextEncoder().encode('bytes')],
    ['Blob', new Blob(['bytes'])],
  ])('round-trips a %s body', async (_kind, body) => {
    // #given
    const store = makeStore();

    // #when
    await store.put(REF, body);
    const content = await store.get(REF);

    // #then
    expect(new TextDecoder().decode(await content?.arrayBuffer())).toBe(
      'bytes',
    );
  });

  it('round-trips a ReadableStream body', async () => {
    // #given
    const store = makeStore();
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('str'));
        controller.enqueue(new TextEncoder().encode('eam'));
        controller.close();
      },
    });

    // #when
    await store.put(REF, stream);

    // #then
    expect(await (await store.get(REF))?.text()).toBe('stream');
  });

  it('returns undefined for a missing artifact', async () => {
    // #given
    const store = makeStore();

    // #when / #then
    expect(await store.get(REF)).toBeUndefined();
  });

  it('builds a record from inputs when the bucket returns null from put', async () => {
    // #given — a bucket whose put legally returns null (allowed by the
    // ArtifactBucketObject | null contract; some R2-alikes do)
    const backing = new InMemoryArtifactBucket();
    const nullPutBucket: ArtifactBucket = {
      put: async (key, value, options) => {
        await backing.put(key, value, options);
        return null;
      },
      get: (key) => backing.get(key),
      delete: (key) => backing.delete(key),
      list: (options) => backing.list(options),
    };
    const store = new R2ArtifactStore(nullPutBucket);

    // #when
    const record = await store.put(REF, 'x', { contentType: 'text/plain' });

    // #then — record is synthesized from the ref + options, key still correct
    expect(record).toEqual({
      ...REF,
      key: 'wf/run-1/reports/summary.md',
      contentType: 'text/plain',
      metadata: undefined,
    });
  });
});

describe('R2ArtifactStore list/delete', () => {
  it('lists only the scoped run, parses names, honors namePrefix', async () => {
    // #given
    const store = makeStore();
    await store.put({ ...REF, name: 'reports/a.md' }, 'a');
    await store.put({ ...REF, name: 'reports/b.md' }, 'b');
    await store.put({ ...REF, name: 'logs/run.txt' }, 'c');
    await store.put({ ...REF, runId: 'run-2', name: 'reports/a.md' }, 'd');
    await store.put({ ...REF, workflowId: 'other', name: 'reports/a.md' }, 'e');

    // #when
    const all = await store.list({ workflowId: 'wf', runId: 'run-1' });
    const reports = await store.list({
      workflowId: 'wf',
      runId: 'run-1',
      namePrefix: 'reports/',
    });

    // #then
    expect(all.map((r) => r.name)).toEqual([
      'logs/run.txt',
      'reports/a.md',
      'reports/b.md',
    ]);
    expect(reports.map((r) => r.name)).toEqual([
      'reports/a.md',
      'reports/b.md',
    ]);
  });

  it('drains list pagination across cursors', async () => {
    // #given — a bucket that pages one object at a time
    const backing = new InMemoryArtifactBucket();
    const paging: ArtifactBucket = {
      put: (key, value, options) => backing.put(key, value, options),
      get: (key) => backing.get(key),
      delete: (key) => backing.delete(key),
      list: async (options): Promise<ArtifactBucketListResult> => {
        const { objects } = await backing.list({ prefix: options?.prefix });
        const start = options?.cursor ? Number(options.cursor) : 0;
        const page = objects.slice(start, start + 1);
        const truncated = start + 1 < objects.length;
        return truncated
          ? { objects: page, truncated, cursor: String(start + 1) }
          : { objects: page, truncated };
      },
    };
    const store = new R2ArtifactStore(paging);
    await store.put({ ...REF, name: 'a' }, '1');
    await store.put({ ...REF, name: 'b' }, '2');
    await store.put({ ...REF, name: 'c' }, '3');

    // #when
    const records = await store.list({ workflowId: 'wf', runId: 'run-1' });

    // #then
    expect(records.map((r) => r.name)).toEqual(['a', 'b', 'c']);
  });

  it('delete removes one artifact; deleteRun clears the run and counts', async () => {
    // #given
    const store = makeStore();
    await store.put({ ...REF, name: 'a' }, '1');
    await store.put({ ...REF, name: 'b' }, '2');
    await store.put({ ...REF, runId: 'run-2', name: 'keep' }, '3');

    // #when
    await store.delete({ ...REF, name: 'a' });
    const afterDelete = await store.list({ workflowId: 'wf', runId: 'run-1' });
    const purged = await store.deleteRun('wf', 'run-1');

    // #then
    expect(afterDelete.map((r) => r.name)).toEqual(['b']);
    expect(purged).toBe(1);
    expect(await store.list({ workflowId: 'wf', runId: 'run-1' })).toEqual([]);
    expect(
      (await store.list({ workflowId: 'wf', runId: 'run-2' })).map(
        (r) => r.name,
      ),
    ).toEqual(['keep']);
  });

  it('scopes keys under keyPrefix and still parses names', async () => {
    // #given
    const bucket = new InMemoryArtifactBucket();
    const store = new R2ArtifactStore(bucket, { keyPrefix: 'artifacts' });

    // #when
    const record = await store.put(REF, 'x');
    const listed = await store.list({ workflowId: 'wf', runId: 'run-1' });

    // #then
    expect(record.key).toBe('artifacts/wf/run-1/reports/summary.md');
    expect(listed.map((r) => r.name)).toEqual(['reports/summary.md']);
    expect(await (await store.get(REF))?.text()).toBe('x');
  });
});

describe('run-scope invariant: NO workflow-level enumeration', () => {
  // The R2 key is `[prefix/]workflowId/runId/name`. A workflow id is shared by
  // every run, so every read/delete demands the FULL (workflowId, runId) pair.
  // A "list all artifacts for workflow W" API would silently widen a
  // run-scoped surface into deployment-wide enumeration. This suite pins the
  // narrower contract.

  it('every public read/delete surface requires the full (workflowId, runId) pair', () => {
    // #given
    const store = new R2ArtifactStore(new InMemoryArtifactBucket());

    // #then — compile-time pins inside a NEVER-CALLED closure (the calls
    // would throw at runtime; only their types are under test). An unused
    // ts-expect-error directive is itself an error, so tsc exit 0 proves
    // each pin fails to compile.
    const typePins = (s: R2ArtifactStore): void => {
      // @ts-expect-error list() must not accept a workflow-only scope
      void s.list({ workflowId: 'wf' });

      // @ts-expect-error deleteRun() must not accept a workflow alone
      void s.deleteRun('wf');

      // @ts-expect-error get() addresses one artifact, never a workflow
      void s.get({ workflowId: 'wf' });
    };
    void typePins;

    expect(typeof store.list).toBe('function');
  });

  it('runtime belt: list() rejects an empty runId rather than widening the prefix', async () => {
    // #given — if a caller defeats the types with a cast, an empty runId
    // would make the R2 prefix `wf/` — the exact workflow-level enumeration
    const store = new R2ArtifactStore(new InMemoryArtifactBucket());

    // #when / #then
    await expect(
      store.list({ workflowId: 'wf', runId: '' } as never),
    ).rejects.toThrow();
  });
});
