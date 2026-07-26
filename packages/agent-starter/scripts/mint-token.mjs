// SPDX-License-Identifier: Apache-2.0

import { APPROVAL_ROLES } from '@proofoftech/flowsafe/approval-api';
import { mintHmacToken } from '@proofoftech/flowsafe/host-kit';

const [tenantId = 'acme', role = 'operator', actorId = 'local-operator'] =
  process.argv.slice(2);
const secret = process.env.AUTH_HMAC_SECRET;
const issuer = process.env.AUTH_JWT_ISSUER ?? 'anchorage-agent-starter';
const audience = process.env.AUTH_JWT_AUDIENCE ?? 'anchorage-agent-starter-api';

if (!secret) {
  throw new Error('AUTH_HMAC_SECRET is required');
}
if (!APPROVAL_ROLES.includes(role)) {
  throw new Error(`role must be one of: ${APPROVAL_ROLES.join(', ')}`);
}

const token = await mintHmacToken({
  secret,
  kid: 'primary',
  issuer,
  audience,
  actor: { id: actorId, role, tenantId },
  ttlSeconds: 60 * 60,
});

process.stdout.write(`${token}\n`);
