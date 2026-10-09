// SPDX-License-Identifier: Apache-2.0
// Keyed FIFO queue.
//
// It lives here, not in fenced-workflows-d1.ts, so runtime.ts can share it
// without loading `@mastra/cloudflare-d1` and `node:async_hooks`.

/**
 * @internal Runs `work` once every earlier call with the same `key` on `tails`
 * has settled, in call order; a failed call does not block the next. An entry
 * leaves `tails` when its last call settles. Each owner keeps its own `tails`:
 * a caller that holds its queue across a call into another owner's would
 * deadlock on a shared one.
 */
export function serializedByKey<T>(
  tails: Map<string, Promise<unknown>>,
  key: string,
  work: () => Promise<T>,
): Promise<T> {
  const previous = tails.get(key) ?? Promise.resolve();
  const current = previous.then(work, work);
  const settled = current.then(
    () => undefined,
    () => undefined,
  );
  tails.set(key, settled);
  return current.finally(() => {
    if (tails.get(key) === settled) tails.delete(key);
  });
}
