# showcase

## 0.0.21

### Patch Changes

- 8f4daae: Require `@mastra/core` 1.53.0 exactly (previously 1.50.0). The peer is exact, so every consumer must move to 1.53.0 as well; this is breaking for consumers pinned to 1.50.0. 1.53.0 is the newest release whose published output still bundles for Cloudflare Workers and Vite: 1.54.0 through 1.60.0 inline Node-only dynamic imports (`execa`, `@ast-grep/napi`) that fail to bundle (mastra-ai/mastra#20638). `@mastra/cloudflare-d1` stays at 1.1.1. FlowSafe's `@proofoftech/breakwater` peer floor rises to `>=0.13.0` in step, that being the first Breakwater release built against the same core.

  FlowSafe's durable agent runner now refuses every inherited entry point that can drive execution outside `RunnerRuntime`, mint a run id below the caller, or hand back runs the caller does not own: the run-recovery entry points 1.53.0 adds to `DurableAgent` (`recover`, `recoverActiveRuns`, `listActiveRuns`); the resume family (`resume`, `resumeStream`, `resumeGenerate`, `approveToolCall`, `declineToolCall`, `approveToolCallGenerate`, `declineToolCallGenerate`), which since 1.53.0 rehydrate from snapshot storage on a run-registry miss; the agent-level discovery member `listSuspendedRuns`; the network family (`network`, `resumeNetwork`, `approveNetworkToolCall`, `declineNetworkToolCall`), which drives the multi-agent loop's own workflow on the default engine; the AI SDK v4 legacy pair (`generateLegacy`, `streamLegacy`), which runs the agent's tools while skipping the authorization check every supported entry point calls; and `sendToolApproval`, whose continuation branch starts a run under a generated run id rather than resuming. `deleteRunSnapshots` is refused on a separate ground: the snapshot rows it deletes belong to deployment-scoped retention rather than to any caller. Nineteen entry points in all. That leaves `resumeViaRuntime` as the only resume path and the guarded `stream`/`generate`/`prepare` as the only execution entry points. Surface tripwires now classify every `DurableAgent` prototype member and every inherited `Agent` member, so a future peer bump surfaces new entry points on either.

  This is a behavior change for any consumer that called those methods on a FlowSafe durable agent: they now throw instead of executing. Their TYPE signatures narrow too — the overridden members return `Promise<never>`, and the generic overloads several of them carried (`network`, `generateLegacy`, `streamLegacy`, `sendToolApproval`) collapse to a single refusing signature, so a call that no longer type-checks is the intended signal rather than a regression. Nothing in the supported agent-host surface reaches them — route clients through the agent-host run routes.

- Updated dependencies [80a801c]
- Updated dependencies [b85a872]
- Updated dependencies [fa0d11d]
- Updated dependencies [0447466]
- Updated dependencies [da6a0aa]
- Updated dependencies [8f4daae]
- Updated dependencies [66c19f1]
- Updated dependencies [5cbe01d]
  - @proofoftech/flowsafe@0.19.0
  - @proofoftech/breakwater@0.13.0

## 0.0.20

### Patch Changes

- Updated dependencies [e7fb658]
  - @proofoftech/flowsafe@0.18.0

## 0.0.19

### Patch Changes

- Updated dependencies [37175fa]
  - @proofoftech/breakwater@0.12.0
  - @proofoftech/flowsafe@0.17.0

## 0.0.18

### Patch Changes

- Updated dependencies [296207f]
  - @proofoftech/flowsafe@0.16.1

## 0.0.17

### Patch Changes

- Updated dependencies [1f6a13a]
  - @proofoftech/flowsafe@0.16.0

## 0.0.16

### Patch Changes

- Updated dependencies [2c097d8]
- Updated dependencies [34e8ae0]
  - @proofoftech/flowsafe@0.15.0

## 0.0.15

### Patch Changes

- Updated dependencies [fa12c05]
  - @proofoftech/flowsafe@0.14.0

## 0.0.14

### Patch Changes

- Updated dependencies [a16ed60]
- Updated dependencies [352b38c]
  - @proofoftech/breakwater@0.11.1
  - @proofoftech/flowsafe@0.13.1

## 0.0.13

### Patch Changes

- Forward Breakwater's explicit idempotency-key migration boundary through the
  workflow connectors. Fresh in-memory runtimes acknowledge it automatically;
  injected durable stores require the host's drained-writer acknowledgement.

- Updated dependencies [4f0fc9d]
- Updated dependencies [4f0fc9d]
  - @proofoftech/flowsafe@0.13.0
  - @proofoftech/breakwater@0.11.0

## 0.0.12

### Patch Changes

- ff641f8: Bind the production showcase to its dedicated D1 database and single-tenant Worker, disable alternate public ingress, and retain the former deployment as the rollback bundle.

## 0.0.11

### Patch Changes

- Updated dependencies [3276c2a]
- Updated dependencies [b3b4b55]
- Updated dependencies [af29901]
  - @proofoftech/flowsafe@0.12.0
  - @proofoftech/breakwater@0.10.0

## 0.0.10

### Patch Changes

- Updated dependencies [52d6836]
- Updated dependencies [d78e779]
- Updated dependencies [d78e779]
  - @proofoftech/flowsafe@0.11.0
  - @proofoftech/breakwater@0.9.0

## 0.0.9

### Patch Changes

- Updated dependencies [f654696]
- Updated dependencies [cb0f861]
  - @proofoftech/flowsafe@0.10.0
  - @proofoftech/breakwater@0.8.0

## 0.0.8

### Patch Changes

- Updated dependencies [3a259b8]
  - @proofoftech/breakwater@0.7.0
  - @proofoftech/flowsafe@0.9.0

## 0.0.7

### Patch Changes

- Updated dependencies [6670285]
- Updated dependencies [09a4406]
  - @proofoftech/flowsafe@0.8.0
  - @proofoftech/breakwater@0.6.0

## 0.0.6

### Patch Changes

- Updated dependencies [def3b37]
- Updated dependencies [def3b37]
  - @proofoftech/breakwater@0.5.0
  - @proofoftech/flowsafe@0.7.0

## 0.0.5

### Patch Changes

- Updated dependencies [8e3562f]
- Updated dependencies [eca3b6e]
- Updated dependencies [97cb097]
- Updated dependencies [d54d2be]
- Updated dependencies [0f4f70a]
- Updated dependencies [6c80e92]
  - @proofoftech/flowsafe@0.6.0

## 0.0.4

### Patch Changes

- Updated dependencies [281c6d1]
- Updated dependencies [4ea35fd]
- Updated dependencies [15d4ec3]
- Updated dependencies [4b953d4]
  - @proofoftech/flowsafe@0.5.0
  - @proofoftech/breakwater@0.4.0

## 0.0.3

### Patch Changes

- Updated dependencies [0c108fa]
- Updated dependencies [dbe6a93]
  - @proofoftech/flowsafe@0.4.0
  - @proofoftech/breakwater@0.3.1

## 0.0.2

### Patch Changes

- Updated dependencies [19ad5c4]
- Updated dependencies [df413da]
- Updated dependencies [4fbc0be]
- Updated dependencies [5011013]
- Updated dependencies [85a1ec8]
- Updated dependencies [5f0a57e]
  - @proofoftech/flowsafe@0.3.0
  - @proofoftech/breakwater@0.3.0

## 0.0.1

### Patch Changes

- Updated dependencies [3bed052]
- Updated dependencies [94d6b84]
  - @proofoftech/breakwater@0.2.0
  - @proofoftech/flowsafe@0.2.0
