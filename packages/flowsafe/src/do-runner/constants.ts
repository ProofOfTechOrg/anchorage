// SPDX-License-Identifier: Apache-2.0

export type {
  SuspensionTimeoutEnvelope,
  SuspensionTimeoutResumeData,
} from './suspension-deadline.js';
export {
  isArmableSuspensionDeadlineMs,
  isSuspensionTimeoutResumeData,
  MAX_SUSPENSION_DEADLINE_MS,
  MIN_SUSPENSION_DEADLINE_MS,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
  SUSPENSION_TIMEOUT_RESUME_KEY,
} from './suspension-deadline.js';
