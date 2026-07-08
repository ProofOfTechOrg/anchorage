// host-kit — workflow-agnostic host glue shared by the showcase Worker and the
// dev backend. It is intentionally NOT a generic host: the two resume topologies
// (DO-stub fetch vs in-process) stay in their hosts; this kit only supplies the
// pieces both duplicate.

export {
  queueApprovalForSuspension,
  requestedConnectors,
  type ResumeRunFn,
  resumeRunWithRequeue,
} from './approval-bridge.js';
export type {
  WorkflowMeta,
  WorkflowModule,
  WorkflowModuleContext,
} from './module.js';
