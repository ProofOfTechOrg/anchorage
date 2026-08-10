// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import type { ActorContext } from '../approval-api/index.js';
import { requireResourceAccess } from './resource-access.js';

describe('requireResourceAccess', () => {
  it('rejects a numeric id before consulting ownership', async () => {
    const canAccessResource = vi.fn(async () => true);
    const context = { canAccessResource } as unknown as ActorContext;

    await expect(
      requireResourceAccess(context, 'run', 123 as unknown as string, 'read'),
    ).rejects.toMatchObject({ status: 404, message: 'run not found' });
    expect(canAccessResource).not.toHaveBeenCalled();
  });
});
