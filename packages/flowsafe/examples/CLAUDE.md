# examples/

Runnable, self-verifying examples that execute an Anchorage workflow end-to-end
on the real seams (no Cloudflare/wrangler, in one Node process): a Mastra
workflow suspends at an approval gate, the flowsafe approval queue decides,
`approvalGrantProvider` mints the breakwater grant on resume, and a real
`createConnector` write gate admits the gated step only with that grant. Unlike
`docs/examples/*.ts` (illustrative, not runnable), these actually run.

They are delivered as `vitest` specs because the toolchain has no `tsx`/`ts-node`;
`vitest` (esbuild) is the only TS runner present. Each file both narrates its run
(`console.log`) and asserts the outcome, so it doubles as a regression guard under
`pnpm --filter @proofoftech/flowsafe test`.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `gtm-outbound.e2e.test.ts` | The `docs/examples/gtm-outbound.ts` sketch made to run: serial pipeline → suspend gate → grant-gated send, with a fail-closed (no-grant) case | Seeing the full suspend→approve→grant→gated-write loop in one Node process |

## Run

```bash
pnpm --filter @proofoftech/flowsafe example:gtm   # narrated run + assertions
```
