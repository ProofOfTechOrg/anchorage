// SPDX-License-Identifier: Apache-2.0

import {
  DIRECT_CONFORMANCE_EXIT_CODES,
  DIRECT_CREDENTIAL_VARIABLES,
  DIRECT_INTERNAL_ERROR_DIAGNOSTIC,
  DIRECT_USAGE_DIAGNOSTIC,
  directLineCarries,
  directWritesStderr,
  isDirectLiveMode,
  parseDirectConformanceArgs,
  resolveDirectExitCode,
  runDirectConformance,
} from './direct-credentialed-conformance-runtime.mjs';

const exits = DIRECT_CONFORMANCE_EXIT_CODES;
let terminal = null;
const secrets = new Map();
let transcript = '';
function setExitCode(next) {
  terminal = resolveDirectExitCode(terminal, next);
  process.exitCode = terminal;
}
/**
 * Returns whether the entry wrote the line; a suppressed required stdout
 * summary resolves to exit 5, while a suppressed stderr copy or fixed
 * diagnostic preserves the resolved code. A reader sees the two descriptors in
 * either order, so both concatenations are scanned. `secrets` stays empty
 * outside a live mode, where the CLI reads no credential, so nothing is
 * withheld there.
 */
function write(stream, line) {
  const values = [...secrets.values()];
  if (
    directLineCarries(transcript + line, values) ||
    directLineCarries(line + transcript, values)
  )
    return false;
  transcript += line;
  stream.write(line);
  return true;
}
// Suppression covers every line this entry writes in a live mode, the internal
// error diagnostic and the argv usage line included.
function internalError() {
  setExitCode(exits.failed);
  write(process.stderr, DIRECT_INTERNAL_ERROR_DIAGNOSTIC);
}
// Registered before the argv parse and the trap construction, so a fault in
// either is reported through the same guarded diagnostic.
process.on('unhandledRejection', internalError);
process.on('uncaughtException', internalError);
const mode = parseDirectConformanceArgs(process.argv.slice(2));
const live = isDirectLiveMode(mode);
// A live mode reads its credentials through this trap, which records each one
// for the guard above. A local mode is specified to run without credentials and
// is handed an empty environment; `help` reads no configuration path either.
const env = live
  ? new Proxy(process.env, {
      get(target, variable) {
        if (!DIRECT_CREDENTIAL_VARIABLES.includes(variable))
          return Reflect.get(target, variable);
        if (!secrets.has(variable)) secrets.set(variable, target[variable]);
        return secrets.get(variable);
      },
    })
  : {};
/**
 * Records every live credential, so a refusal the runtime returns before its
 * own credential read is still scanned against the full list.
 */
const armGuard = () => {
  for (const variable of DIRECT_CREDENTIAL_VARIABLES) void env[variable];
};
if (mode === null) {
  setExitCode(exits.invalidInput);
  write(process.stderr, DIRECT_USAGE_DIAGNOSTIC);
} else {
  runDirectConformance({
    mode,
    configPath:
      mode === 'help' ? undefined : process.env.FLEET_DIRECT_CONFORMANCE_CONFIG,
    env,
  })
    .then((result) => {
      armGuard();
      setExitCode(result.exitCode);
      if (
        result.stdoutLine !== null &&
        !write(process.stdout, result.stdoutLine)
      )
        setExitCode(exits.evidenceFailed);
      if (directWritesStderr(result)) write(process.stderr, result.stderrLine);
    })
    .catch(internalError);
}
