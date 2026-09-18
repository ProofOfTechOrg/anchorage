// SPDX-License-Identifier: Apache-2.0

// `cancelBodyWithoutAwait` is the package's home for the unawaited body
// release and it ships in the built package, so the credentialed CLI entries
// read it through this leaf: `scripts/direct-credentialed-conformance.mjs`
// and `scripts/credentialed-conformance.mjs`. The `direct-` prefix does not
// limit the leaf to the direct entry's modules.
//
// The load is a dynamic `import()`, so the module graph of a bare
// `node scripts/direct-credentialed-conformance.mjs` carries no `dist/` edge:
// an unbuilt checkout reaches the runtime's own `dist-missing` refusal rather
// than a module-resolution failure. A run still needs `pnpm build` first,
// which the `test:credentialed:direct` script does.
//
// The load starts at module evaluation and a rejection is caught there, so an
// unbuilt checkout raises no unhandled rejection and the promise resolves to
// `undefined`. A release issued before the load resolves is deferred to it; a
// release issued after it has resolved is issued in the caller's own turn,
// which is what a refusing exit about to end the process needs. The release
// is not awaited: a hostile source can hang its own cancel.
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
