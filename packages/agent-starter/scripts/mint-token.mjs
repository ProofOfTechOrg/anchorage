// SPDX-License-Identifier: Apache-2.0

import { realpathSync } from 'node:fs';
import { isAbsolute, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isMainThread } from 'node:worker_threads';

import { APPROVAL_ROLES } from '@proofoftech/flowsafe/approval-api';
import { mintHmacToken, toApprovalActor } from '@proofoftech/flowsafe/host-kit';

export async function mintStarterToken({
  role,
  actorId,
  secret,
  issuer,
  audience,
}) {
  if (!secret) {
    throw new Error('AUTH_HMAC_SECRET is required');
  }
  if (!APPROVAL_ROLES.includes(role)) {
    throw new Error(`role must be one of: ${APPROVAL_ROLES.join(', ')}`);
  }

  const actor = toApprovalActor({ id: actorId, role });
  if (!actor) {
    throw new Error('actorId must be non-empty');
  }

  return mintHmacToken({
    secret,
    kid: 'primary',
    issuer,
    audience,
    actor,
    ttlSeconds: 60 * 60,
  });
}

async function main() {
  const [role = 'operator', actorId = 'local-operator'] = process.argv.slice(2);
  const token = await mintStarterToken({
    role,
    actorId,
    secret: process.env.AUTH_HMAC_SECRET,
    issuer: process.env.AUTH_JWT_ISSUER ?? 'anchorage-agent-starter',
    audience: process.env.AUTH_JWT_AUDIENCE ?? 'anchorage-agent-starter-api',
  });

  process.stdout.write(`${token}\n`);
}

// The repository's `scripts/entry-point.mjs` holds the root form; it is not
// published, so this module carries its own.
function isInvokedAsEntryPoint(moduleUrl) {
  const entry = process.argv[1];
  if (!entry) return false;
  if (
    entry === '-' ||
    (!isMainThread && !isAbsolute(entry)) ||
    (isMainThread &&
      process.execArgv.some((argument) =>
        /^(?:-[ep]|--(?:eval|print)(?:=|$))/u.test(argument),
      ))
  )
    return false;
  let invokedFilePath;
  try {
    invokedFilePath = realpathSync(resolve(entry));
  } catch (error) {
    if (error?.code === 'ENOENT' || error?.code === 'ENOTDIR') return false;
    throw error;
  }
  return invokedFilePath === realpathSync(fileURLToPath(moduleUrl));
}

if (isInvokedAsEntryPoint(import.meta.url)) {
  await main();
}
