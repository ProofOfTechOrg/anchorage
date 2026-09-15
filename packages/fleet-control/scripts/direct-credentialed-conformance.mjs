// SPDX-License-Identifier: Apache-2.0

import {
  DIRECT_INTERNAL_ERROR_DIAGNOSTIC,
  DIRECT_USAGE_DIAGNOSTIC,
  parseDirectConformanceArgs,
  resolveDirectExitCode,
  runDirectConformance,
} from './direct-credentialed-conformance-runtime.mjs';

const mode = parseDirectConformanceArgs(process.argv.slice(2));
const live = mode === 'run' || mode === 'resume';
let terminal = null;
const setExitCode = (next) => {
  terminal = resolveDirectExitCode(terminal, next);
  process.exitCode = terminal;
};
const secrets = new Map();
const env = live
  ? new Proxy(process.env, {
      get(target, variable) {
        if (
          variable !== 'CLOUDFLARE_API_TOKEN' &&
          variable !== 'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET'
        )
          return Reflect.get(target, variable);
        if (!secrets.has(variable)) secrets.set(variable, target[variable]);
        return secrets.get(variable);
      },
    })
  : {};
let transcript = '';
/** Returns whether the entry wrote the line; a suppressed required stdout summary resolves to exit 5, while a suppressed stderr copy or fixed diagnostic preserves the resolved code. */
const writeGuarded = (stream, line) => {
  const next = transcript + line;
  if (
    [...secrets.values()].some(
      (secret) =>
        typeof secret === 'string' &&
        secret.length > 0 &&
        next.includes(secret),
    )
  )
    return false;
  transcript = next;
  stream.write(line);
  return true;
};
const write = live
  ? writeGuarded
  : (stream, line) => {
      stream.write(line);
      return true;
    };
const internalError = () => {
  setExitCode(1);
  write(process.stderr, DIRECT_INTERNAL_ERROR_DIAGNOSTIC);
};
process.on('unhandledRejection', internalError);
process.on('uncaughtException', internalError);
if (mode === null) {
  setExitCode(2);
  write(process.stderr, DIRECT_USAGE_DIAGNOSTIC);
} else {
  runDirectConformance({
    mode,
    configPath:
      mode === 'help' ? undefined : process.env.FLEET_DIRECT_CONFORMANCE_CONFIG,
    env,
  })
    .then((result) => {
      setExitCode(result.exitCode);
      if (
        result.stdoutLine !== null &&
        !write(process.stdout, result.stdoutLine)
      )
        setExitCode(5);
      if (
        result.stderrLine !== null &&
        (result.stderrOnly || (result.exitCode !== 0 && result.exitCode !== 3))
      )
        write(process.stderr, result.stderrLine);
    })
    .catch(internalError);
}
