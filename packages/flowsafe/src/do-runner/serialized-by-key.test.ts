// SPDX-License-Identifier: Apache-2.0
// The keyed queue's failure isolation, entry removal and async context. Every
// owner keeps its map private, and no owner's suite queues a call behind a
// failure or reads the context inside a queued call.

import { AsyncLocalStorage } from 'node:async_hooks';

import { describe, expect, it } from 'vitest';

import { serializedByKey } from './serialized-by-key.js';

const KEY = 'run-1';

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

describe('serializedByKey', () => {
  it('runs a call queued behind a rejected call and gives each caller its own outcome', async () => {
    // #given a second call queued behind a first call whose work is pending
    const tails = new Map<string, Promise<unknown>>();
    const firstWork = deferred<string>();
    const first = serializedByKey(tails, KEY, () => firstWork.promise);
    const second = serializedByKey(tails, KEY, async () => 'second result');
    const outcomes = Promise.allSettled([first, second]);

    // #when the first call's work rejects
    firstWork.reject(new Error('first failed'));

    // #then the first caller receives that rejection and the second caller
    // receives the result of its own work
    expect(await outcomes).toEqual([
      { status: 'rejected', reason: new Error('first failed') },
      { status: 'fulfilled', value: 'second result' },
    ]);
  });

  it('removes the key entry once its last call resolves', async () => {
    // #given one call in flight on the key
    const tails = new Map<string, Promise<unknown>>();
    const work = deferred<string>();
    const call = serializedByKey(tails, KEY, () => work.promise);

    // #when its work resolves and the call settles
    work.resolve('done');
    await call;

    // #then the map holds no entry for the key
    expect([...tails.keys()]).toEqual([]);
  });

  it('removes the key entry once its last call rejects', async () => {
    // #given one call in flight on the key
    const tails = new Map<string, Promise<unknown>>();
    const work = deferred<string>();
    const call = serializedByKey(tails, KEY, () => work.promise);
    const outcome = Promise.allSettled([call]);

    // #when its work rejects and the call settles
    work.reject(new Error('work failed'));
    await outcome;

    // #then the map holds no entry for the key
    expect([...tails.keys()]).toEqual([]);
  });

  it('keeps the key entry while a successor call is queued', async () => {
    // #given a second call queued behind a first call
    const tails = new Map<string, Promise<unknown>>();
    const firstWork = deferred();
    const secondWork = deferred();
    const first = serializedByKey(tails, KEY, () => firstWork.promise);
    const second = serializedByKey(tails, KEY, () => secondWork.promise);

    // #when the first call settles while the second is still pending
    firstWork.resolve();
    await first;

    // #then the map still holds the key
    expect([...tails.keys()]).toEqual([KEY]);

    secondWork.resolve();
    await second;
  });

  it("runs each call's work in the async context of the caller that made it", async () => {
    // #given two callers on one key, each inside its own async-context value
    const context = new AsyncLocalStorage<string>();
    const tails = new Map<string, Promise<unknown>>();
    const seen: Record<string, string | undefined> = {};
    const firstCall = context.run('first caller', () =>
      serializedByKey(tails, KEY, async () => {
        seen.first = context.getStore();
      }),
    );
    const secondCall = context.run('second caller', () =>
      serializedByKey(tails, KEY, async () => {
        seen.second = context.getStore();
      }),
    );

    // #when both calls run, the second after the first releases the key
    await Promise.all([firstCall, secondCall]);

    // #then each work read its own caller's value, including the work that ran
    // after the first call released the key
    expect(seen).toEqual({ first: 'first caller', second: 'second caller' });
  });
});
