---
"@proofoftech/breakwater": patch
---

`runtime.fetch` and `egressFetch()` now refuse an `init.redirect` that is not `'follow'`, `'manual'` or `'error'` with `EGRESS_INPUT_INVALID` before any request, and pass the base fetch the redirect mode they checked. In every redirect mode, the guard forwards request-init members inherited by a class instance, an `Object.create` object or a `Request` passed as `init`; copying only own enumerable members had dropped values such as method, headers, body and signal.

Security: the guard compared `init.redirect` with the string `'follow'`, then handed the caller's `init` to the base fetch, which read and converted `redirect` again. A value the base fetch converts to `'follow'`, such as the JSON array `["follow"]`, a `String` object or an object with its own `toString`, or a getter that answered `'manual'` to the guard and `'follow'` to the base, made the base fetch follow redirects itself with no hop check, so a redirect from an allowed host could reach any host. This affects every release with the runtime guard.

Migration: pass `redirect` as one of the strings `'follow'`, `'manual'` or `'error'`, or omit it.
