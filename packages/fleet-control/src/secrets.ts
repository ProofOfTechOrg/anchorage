// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from 'node:crypto';

import type { DeploymentSecrets } from './types.js';

export function generateDeploymentSecrets(): DeploymentSecrets {
  return {
    deploymentIdentity: randomBytes(32).toString('base64url'),
    maintenanceAdmin: randomBytes(32).toString('base64url'),
  };
}
