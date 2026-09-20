// SPDX-License-Identifier: Apache-2.0

// The credentialed CLI entries load the package's unawaited body release
// through this leaf.
//
// This leaf reaches dist through a dynamic `import()`; credentialed-conformance
// loads its own dist modules dynamically. The relative static import/export
// graphs are checked by direct-credentialed-provider.test.ts. An unbuilt
// checkout can reach the direct runtime's `dist-missing` refusal. A run needs
// `pnpm build` first, which the `test:credentialed:direct` script performs.
//
// The load starts at module evaluation and a rejection is caught there, so an
// unbuilt checkout raises no unhandled rejection and the promise resolves to
// `undefined`. A release issued before the load resolves is deferred to it;
// once resolved, the cached callable runs in the caller's turn. The release is
// not awaited: a hostile source can hang its own cancel.
const home = import('../dist/database-export-store.js').catch(() => undefined);
let release;

export function cancelBodyWithoutAwait(body, reason) {
  try {
    if (release) {
      release(body, reason);
      return;
    }
    void home
      .then((module) => {
        const cancel = module?.cancelBodyWithoutAwait;
        if (typeof cancel !== 'function') return;
        release = cancel;
        release(body, reason);
      })
      .catch(() => undefined);
  } catch {
    /* Best effort: a caller at a refusing exit cannot act on a failure. */
  }
}
