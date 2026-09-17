// SPDX-License-Identifier: Apache-2.0

// `cancelBodyWithoutAwait` is the package's home for the unawaited body
// release and it ships in the built package, so the direct CLI family reads it
// through this leaf. The load is deferred to the first call, which keeps the
// module graph of a bare `node scripts/direct-credentialed-conformance.mjs`
// free of any `dist/` edge: an unbuilt checkout reaches the runtime's own
// `dist-missing` refusal rather than a module-resolution failure. A run still
// needs `pnpm build` first, which the `test:credentialed:direct` script does.
//
// Only the first call waits on that load. It holds the resolved function, so
// every release after it is issued in the caller's own turn, which is what a
// refusing exit about to end the process needs.
let home;
let release;

export function cancelBodyWithoutAwait(body, reason) {
  try {
    if (release) {
      release(body, reason);
      return;
    }
    home ??= import('../dist/database-export-store.js');
    void home
      .then((module) => {
        const cancel = module.cancelBodyWithoutAwait;
        if (typeof cancel !== 'function') return;
        release = cancel;
        release(body, reason);
      })
      .catch(() => undefined);
  } catch {
    /* Best effort: a caller at a refusing exit cannot act on a failure. */
  }
}
