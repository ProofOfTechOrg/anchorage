// SPDX-License-Identifier: Apache-2.0

import { directLineCarries } from './direct-credentialed-conformance-runtime.mjs';
import {
  DIRECT_PURGE_OUTPUT_PREFIX,
  DIRECT_PURGE_USAGE,
  parseDirectPurgeArgs,
  runDirectCredentialedPurge,
} from './direct-credentialed-purge-runtime.mjs';

const parsed = parseDirectPurgeArgs(process.argv.slice(2));
const credentials =
  parsed?.mode === 'list' || parsed?.mode === 'delete'
    ? [process.env.CLOUDFLARE_API_TOKEN]
    : [];
let transcript = '';
function write(stream, line) {
  const values = credentials.filter(
    (value) => typeof value === 'string' && value.length > 0,
  );
  if (
    directLineCarries(transcript + line, values) ||
    directLineCarries(line + transcript, values)
  )
    return false;
  transcript += line;
  stream.write(line);
  return true;
}

runDirectCredentialedPurge({
  parsed,
  configPath: process.env.FLEET_DIRECT_CONFORMANCE_CONFIG,
  env: process.env,
})
  .then((result) => {
    process.exitCode = result.exitCode;
    if (!write(process.stdout, result.stdoutLine)) process.exitCode = 3;
  })
  .catch(() => {
    process.exitCode = 3;
    write(
      process.stdout,
      `${DIRECT_PURGE_OUTPUT_PREFIX}${JSON.stringify({
        mode: parsed?.mode ?? null,
        code: 'internal-error',
        usage: parsed === null ? DIRECT_PURGE_USAGE : undefined,
      })}\n`,
    );
  });
