---
"@proofoftech/breakwater": patch
---

`redirect: 'error'` on the guarded fetch now works on Cloudflare Workers. The base fetch receives `redirect: 'manual'` in every mode. In `'error'` mode, a redirect response rejects with `TypeError('fetch failed')` whose cause is `Error('unexpected redirect')`, as on Node. Any other status, including 304, passes through unchanged. No migration is needed.
