// SPDX-License-Identifier: Apache-2.0
// Trusted execution identity shared by authority-bearing DO entry points. Its
// own leaf keeps producers, consumers, and the inbound-header rejection seam
// from depending on one another's modules.

/**
 * The trusted Worker-stamped EXECUTION PRINCIPAL — WHO is executing.
 *
 * REQUIRED, and the SOLE identity channel consumed by protected DO routes, so
 * there is no separate actor or role header to disagree with what executes.
 * Trusted host dispatch stamps it before a request reaches a DO, and consumers
 * refuse a request without it. There is deliberately no human default — a
 * dropped principal header fails rather than admitting automation as a human.
 *
 * `createActorResolver` refuses any inbound request that carries this header,
 * or the retired `x-flowsafe-actor` / `x-flowsafe-role` / `x-flowsafe-tenant`
 * names, before any request scope binds — so a mixed-version client fails
 * loudly instead of being trusted.
 */
export const EXECUTION_PRINCIPAL_HEADER = 'x-flowsafe-principal';
