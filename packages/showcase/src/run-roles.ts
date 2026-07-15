// Roles allowed to START any workflow — the host's coarse start-role gate,
// applied to POST /runs before any per-workflow allowedRoles check. Mirrors
// RUN_START_ROLES in ../../flowsafe/src/approval-api/contract.ts BY VALUE: the
// app consumes the approval-ui (browser) subpackage and does not reach into the
// approval-api (server) subpackage's internal modules, so this is a necessary
// mirror. ONE copy for both the launcher and the control room's wire panel — a
// server-side change drifts one source, not two.
export const RUN_START_ROLES: readonly string[] = [
  'admin',
  'operator',
  'builder',
];
