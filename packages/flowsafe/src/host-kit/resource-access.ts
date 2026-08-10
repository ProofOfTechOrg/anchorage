// SPDX-License-Identifier: Apache-2.0

import type {
  ActorContext,
  ResourceAccess,
  ResourceKind,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/index.js';
import { RunRouteError } from './run-route-error.js';

/** Validate an opaque id and enforce deployment-local ownership as one 404. */
export async function requireResourceAccess(
  context: ActorContext,
  kind: ResourceKind,
  resourceId: string,
  access: ResourceAccess,
  label = kind,
): Promise<string> {
  if (
    !isPathSafeId(resourceId) ||
    !(await context.canAccessResource(kind, resourceId, access))
  ) {
    throw new RunRouteError(404, `${label} not found`);
  }
  return resourceId;
}
