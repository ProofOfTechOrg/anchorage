// SPDX-License-Identifier: Apache-2.0

import { Buffer } from 'node:buffer';
import { randomBytes } from 'node:crypto';

import type { DeploymentSecrets } from './types.js';

export function generateDeploymentSecrets(): DeploymentSecrets {
  return {
    deploymentIdentity: Buffer.from(randomBytes(32)).toString('base64url'),
    maintenanceAdmin: Buffer.from(randomBytes(32)).toString('base64url'),
  };
}
