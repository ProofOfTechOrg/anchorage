# anchorage-agent-starter

## 0.0.17

### Patch Changes

- Updated dependencies [a086f24]
  - @proofoftech/flowsafe@0.20.1

## 0.0.16

### Patch Changes

- Updated dependencies [1212ba5]
  - @proofoftech/flowsafe@0.20.0

## 0.0.15

### Patch Changes

- b85a872: Add a supported connector invocation boundary for trusted hosts and workflows. Direct calls now preserve Mastra validation and Breakwater grants without fabricated tool contexts, and validation failures expose no rejected values or schema messages.
- 8f4daae: Require `@mastra/core` 1.53.0 exactly (previously 1.50.0). The peer is exact, so every consumer must move to 1.53.0 as well; this is breaking for consumers pinned to 1.50.0. 1.53.0 is the newest release whose published output still bundles for Cloudflare Workers and Vite: 1.54.0 through 1.60.0 inline Node-only dynamic imports (`execa`, `@ast-grep/napi`) that fail to bundle (mastra-ai/mastra#20638). `@mastra/cloudflare-d1` stays at 1.1.1. FlowSafe's `@proofoftech/breakwater` peer floor rises to `>=0.13.0` in step, that being the first Breakwater release built against the same core.

  FlowSafe's durable agent runner now refuses every inherited entry point that can drive execution outside `RunnerRuntime`, mint a run id below the caller, or hand back runs the caller does not own: the run-recovery entry points 1.53.0 adds to `DurableAgent` (`recover`, `recoverActiveRuns`, `listActiveRuns`); the resume family (`resume`, `resumeStream`, `resumeGenerate`, `approveToolCall`, `declineToolCall`, `approveToolCallGenerate`, `declineToolCallGenerate`), which since 1.53.0 rehydrate from snapshot storage on a run-registry miss; the agent-level discovery member `listSuspendedRuns`; the network family (`network`, `resumeNetwork`, `approveNetworkToolCall`, `declineNetworkToolCall`), which drives the multi-agent loop's own workflow on the default engine; the AI SDK v4 legacy pair (`generateLegacy`, `streamLegacy`), which runs the agent's tools while skipping the authorization check every supported entry point calls; and `sendToolApproval`, whose continuation branch starts a run under a generated run id rather than resuming. `deleteRunSnapshots` is refused on a separate ground: the snapshot rows it deletes belong to deployment-scoped retention rather than to any caller. Nineteen entry points in all. That leaves `resumeViaRuntime` as the only resume path and the guarded `stream`/`generate`/`prepare` as the only execution entry points. Surface tripwires now classify every `DurableAgent` prototype member and every inherited `Agent` member, so a future peer bump surfaces new entry points on either.

  This is a behavior change for any consumer that called those methods on a FlowSafe durable agent: they now throw instead of executing. Their TYPE signatures narrow too — the overridden members return `Promise<never>`, and the generic overloads several of them carried (`network`, `generateLegacy`, `streamLegacy`, `sendToolApproval`) collapse to a single refusing signature, so a call that no longer type-checks is the intended signal rather than a regression. Nothing in the supported agent-host surface reaches them — route clients through the agent-host run routes.

- 5cbe01d: Align the package and documented Node.js runtime floor with the required `@mastra/core` peer dependency.
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

## 0.0.14

### Patch Changes

- Updated dependencies [e7fb658]
  - @proofoftech/flowsafe@0.18.0

## 0.0.13

### Patch Changes

- Updated dependencies [37175fa]
  - @proofoftech/breakwater@0.12.0
  - @proofoftech/flowsafe@0.17.0

## 0.0.12

### Patch Changes

- Updated dependencies [296207f]
  - @proofoftech/flowsafe@0.16.1

## 0.0.11

### Patch Changes

- Updated dependencies [1f6a13a]
  - @proofoftech/flowsafe@0.16.0

## 0.0.10

### Patch Changes

- Updated dependencies [2c097d8]
- Updated dependencies [34e8ae0]
  - @proofoftech/flowsafe@0.15.0

## 0.0.9

### Patch Changes

- Updated dependencies [fa12c05]
  - @proofoftech/flowsafe@0.14.0

## 0.0.8

### Patch Changes

- Updated dependencies [a16ed60]
- Updated dependencies [352b38c]
  - @proofoftech/breakwater@0.11.1
  - @proofoftech/flowsafe@0.13.1

## 0.0.7

### Patch Changes

- Updated dependencies [4f0fc9d]
- Updated dependencies [4f0fc9d]
  - @proofoftech/flowsafe@0.13.0
  - @proofoftech/breakwater@0.11.0

## 0.0.6

### Patch Changes

- Updated dependencies [3276c2a]
- Updated dependencies [b3b4b55]
- Updated dependencies [af29901]
  - @proofoftech/flowsafe@0.12.0
  - @proofoftech/breakwater@0.10.0

## 0.0.5

### Patch Changes

- Updated dependencies [52d6836]
- Updated dependencies [d78e779]
- Updated dependencies [d78e779]
  - @proofoftech/flowsafe@0.11.0
  - @proofoftech/breakwater@0.9.0

## 0.0.4

### Patch Changes

- Updated dependencies [f654696]
- Updated dependencies [cb0f861]
  - @proofoftech/flowsafe@0.10.0
  - @proofoftech/breakwater@0.8.0

## 0.0.3

### Patch Changes

- 3a259b8: Add first-class execution principals so automated work stops impersonating people.

  Every automated path previously fabricated a human to satisfy the one identity the platform had: the schedule tick, cron SLA maintenance, signal-provider delivery, and the suspension-reconcile bridge all minted `role: 'operator'`. That lost provenance and gave autonomous execution an operator's authority.

  Breakwater's `Actor` gains an optional `kind` (`human` | `service` | `agent` | `system`, absent meaning human), and both `RBACMiddleware` and `createGuardedAgent` gain `allowedPrincipalKinds`, defaulting to `['human']`. The gate checks kind before role and does not consult the role allowlist for a non-human kind, because an automated principal carries a role only to satisfy the required field — consulting it would either admit whatever role the host projected, or force hosts to allow that role and thereby admit real humans holding it. Both the processor gate and the direct-call gate enforce it. **An existing agent therefore denies every automated principal without a config change.**

  Flowsafe adds `ExecutionPrincipal`, with `purpose` required on every automated kind, and persists it in agent-run state and approval resume targets. `AgentMeta.allowedAutomation` declares which principal kinds may enter on which entry paths; absent or empty denies all automated entry, and an optional host authorizer can only narrow it further. `ApprovalActor` is unchanged and still means an authenticated human at the HTTP boundary or a reviewer deciding an approval — a human approval never transfers the decider's authority into the resumed run.

  The `@proofoftech/flowsafe/agent-host` entry point exports its automation policy types, including `AgentAutomationRule`, `AutomationCheck`, `AutomatedEntryRequest`, and `AutomatedEntryAuthorizer`, so public catalog and host signatures never require deep imports.

  `ApprovalService` gains `createAsPrincipal` and `supersedeStaleAsPrincipal` for trusted platform bridges. They replace the human role gate with a kind-and-tenant check rather than widening it. There is deliberately no principal-taking `decide`, `claim`, or `delegate`.

  `trustAutomationPrincipal()` returns a branded, frozen canonical clone rather than the caller's own object. Validating a principal and handing the same reference back left the vouch time-of-check/time-of-use: the caller kept a mutable alias and could rewrite a vouched `system` principal into `{kind:'human', role:'admin'}` before the service read `kind`. The trusted entries now recheck the own brand, the automated shape, the kind, and that every field is a plain data property — an accessor survives `Object.freeze` and would reopen the same hole — instead of trusting a parameter type that does not exist at runtime. `ExecutionPrincipal` fields are `readonly`.

  `AutomatedExecutionPrincipal` is added for duties that want provenance but derive no authority from the principal, so the trust brand is demanded only where it is read. `sweepSLA` and `SlaSweepMaintenanceOptions` take it, and `sweepSLA` refuses a human or malformed principal outright: it writes across every tenant, and a human there would stamp `principalKind: 'human'` onto cron escalations. `TRUSTED_AUTOMATION` is not on the package barrel — `trustAutomationPrincipal` is the sanctioned constructor.

  Audit correlation now carries `principalKind`, `principalId`, `purpose`, and `delegatedBy` alongside the existing tenant, run, thread, and entry-path fields.

  `x-flowsafe-actor` and `x-flowsafe-role` are retired from the wire. The principal is now the sole identity channel: a thread Durable Object projects `scope.actor` from it, so a host's separate `TenantContext.actor` can no longer disagree with what executes. Both header constants are removed from `@proofoftech/flowsafe/do-runner`; the topology strips the names on send and forward, and `createTenantResolver` still refuses them on inbound requests so a mixed-version client fails loudly.

  `queueApprovalForSuspension`, `reconcileApprovalsForSummary`, and `resumeRunWithRequeue` take a `systemActorId` string instead of a principal, and mint their own bookkeeping identity against the service's tenant binding. Hosts no longer perform a trust assertion for the platform's own bookkeeping. `ApprovalService` exposes its `tenantId` for that.

  The principal travels to a Durable Object in a trusted `x-flowsafe-principal` header that `createThreadTopology` stamps on every send and forward. A thread DO refuses a request that carries none rather than treating the caller as a human, and `createTenantResolver` refuses the header on inbound requests exactly as it does the tenant, actor, and role headers.

  BREAKING for in-flight state, deliberately and without an upgrade path: `AgentRunRecord` is version 2 and `agent-thread` resume targets now store an `ExecutionPrincipal`. Records written by the previous release fail closed, so a suspended agent run started before this upgrade cannot resume. A version-1 record cannot be upgraded honestly — a `schedule.fire` run stored `role: 'operator'`, so reading it back as a human would launder exactly the authority this change removes. Flowsafe's breakwater peer floor moves to `>=0.7.0`. `rejectReservedAgentContext` is removed from `@proofoftech/flowsafe/agent-host`; it was exported but never called on any path, and every real caller uses `sanitizeStoredAgentContext`.

  A thread Durable Object now requires the principal header on every request, so a deployment whose Worker and Durable Object resolve different `@proofoftech/flowsafe` versions returns 403 until both sides ship this release. Cloudflare's single-bundle model makes that skew unlikely, but there is no negotiation.

- Updated dependencies [3a259b8]
  - @proofoftech/breakwater@0.7.0
  - @proofoftech/flowsafe@0.9.0

## 0.0.2

### Patch Changes

- Updated dependencies [6670285]
- Updated dependencies [09a4406]
  - @proofoftech/flowsafe@0.8.0
  - @proofoftech/breakwater@0.6.0

## 0.0.1

### Patch Changes

- Updated dependencies [def3b37]
- Updated dependencies [def3b37]
  - @proofoftech/breakwater@0.5.0
  - @proofoftech/flowsafe@0.7.0
