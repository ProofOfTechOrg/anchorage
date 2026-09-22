// SPDX-License-Identifier: Apache-2.0

import { directLineCarries } from './direct-credentialed-conformance-runtime.mjs';
import {
  DIRECT_PURGE_EXIT_CODES,
  DIRECT_PURGE_OUTPUT_PREFIX,
  parseDirectPurgeArgs,
  runDirectCredentialedPurge,
} from './direct-credentialed-purge-runtime.mjs';

const exits = DIRECT_PURGE_EXIT_CODES;
let terminal = null;
let credentials = [];
let transcript = '';
function setExitCode(next) {
  if (terminal !== exits.internalError) terminal = next;
  process.exitCode = terminal;
}
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
function internalError() {
  setExitCode(exits.internalError);
  write(
    process.stdout,
    `${DIRECT_PURGE_OUTPUT_PREFIX}${JSON.stringify({ code: 'internal-error' })}\n`,
  );
}
process.on('unhandledRejection', internalError);
process.on('uncaughtException', internalError);
const parsed = parseDirectPurgeArgs(process.argv.slice(2));
credentials =
  parsed?.mode === 'list' || parsed?.mode === 'delete'
    ? [process.env.CLOUDFLARE_API_TOKEN]
    : [];

runDirectCredentialedPurge({
  parsed,
  configPath: process.env.FLEET_DIRECT_CONFORMANCE_CONFIG,
  env: process.env,
})
  .then((result) => {
    setExitCode(result.exitCode);
    if (!write(process.stdout, result.stdoutLine))
      setExitCode(exits.providerFailed);
  })
  .catch(internalError);
