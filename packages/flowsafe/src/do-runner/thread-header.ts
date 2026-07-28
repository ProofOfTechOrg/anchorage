// SPDX-License-Identifier: Apache-2.0
// The thread DO's tenant header — its own leaf because THREE layers hold one
// end of it each and none may depend on another's module: do-runner's
// ThreadDurableObject VERIFIES it, host-kit's createThreadTopology MINTS it, and
// approval-api's createTenantResolver REFUSES it inbound. Same reasoning that
// puts RESERVED_TENANT_IDS in path-safe-id.ts and the breakwater key literals in
// breakwater-keys.ts: a constant two layers enforce lives below both of them,
// never in whichever one happened to declare it first.

/**
 * The header the trusted Worker stamps with the AUTHENTICATED tenant on every
 * request it forwards to a thread DO. ONE literal for all three enforcement
 * points — a typo on any of them must fail closed (the DO's assertion rejects),
 * and two spellings would silently disable the check on the day one side
 * changed.
 *
 * SERVER-STAMPED, never client-supplied. A thread DO is reached only through a
 * stub fetch from `createThreadTopology`, which never traverses the Worker's
 * request pipeline, so no legitimate inbound request carries this header — which
 * is why `createTenantResolver` refuses one that does, before any store binds.
 */
export const THREAD_TENANT_HEADER = 'x-flowsafe-tenant';

/** The trusted Worker-stamped actor that caused a thread operation. */
export const THREAD_ACTOR_HEADER = 'x-flowsafe-actor';

/** The trusted Worker-stamped role of the actor causing a thread operation. */
export const THREAD_ACTOR_ROLE_HEADER = 'x-flowsafe-role';

/**
 * The trusted Worker-stamped EXECUTION PRINCIPAL — WHO is executing, which the
 * id/role/tenant headers cannot express.
 *
 * REQUIRED. `createThreadTopology` stamps it on every `send` and `forward`, the
 * only sanctioned way to reach a thread DO, and `ThreadDurableObject` refuses a
 * request without it. There is deliberately no human default: the role header
 * for an automated principal carries the least-privileged projected role, so a
 * dropped principal header would have made automation arrive as a human whose
 * entry the agent host never authorized against `allowedAutomation`.
 *
 * Forging it is exactly as hard as forging the role header: `createTenantResolver`
 * refuses any inbound request that carries either, before a store binds.
 */
export const THREAD_PRINCIPAL_HEADER = 'x-flowsafe-principal';
