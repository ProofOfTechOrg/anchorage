// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';

/** The `file:` URL of one direct CLI script, for a child that imports it. */
export const directModuleUrl = (name: string) =>
  new URL(`../../scripts/${name}.mjs`, import.meta.url).href;

/**
 * The preamble a child script runs before it touches the provider: every
 * request the run makes is rewritten onto the harness bridge, the origins it
 * may reach are fixed, and the global `fetch` is closed so nothing else leaves
 * the process. `countRequests` adds the `requests` counter the acceptance
 * driver reports.
 */
export const directBridgePreamble = ({
  bridgeUrl,
  workerOrigin,
  countRequests = false,
}: Readonly<{
  bridgeUrl: string;
  workerOrigin: string;
  countRequests?: boolean;
}>) => `const originalFetch = globalThis.fetch;
${countRequests ? 'let requests = 0;\n' : ''}const fetch = async (input, init) => {
  const request = new Request(input, init);
  const url = new URL(request.url);
  if (url.origin !== 'https://api.cloudflare.com' && url.origin !== ${JSON.stringify(workerOrigin)})
    throw new Error('unexpected child origin');
${countRequests ? '  requests += 1;\n' : ''}  const headers = new Headers(request.headers);
  headers.set('X-Direct-Fixture-Url', request.url);
  return originalFetch(${JSON.stringify(bridgeUrl)}, { method: request.method, headers, body: request.body, signal: request.signal, redirect: 'manual', duplex: 'half' });
};
globalThis.fetch = async () => { throw new Error('unexpected child network'); };
`;

/**
 * Runs one child under the current executable and collects both descriptors.
 * The child is killed at `timeoutMs` so a hung run is reported with its own
 * output rather than as a suite timeout.
 */
export async function spawnDirectChild(
  args: readonly string[],
  options: Readonly<{ env?: NodeJS.ProcessEnv; timeoutMs: number }>,
) {
  const child = spawn(process.execPath, [...args], {
    env: { PATH: process.env.PATH, ...options.env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += String(chunk);
  });
  child.stderr.on('data', (chunk) => {
    stderr += String(chunk);
  });
  const status = await new Promise<number | null>((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
    }, options.timeoutMs);
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
  return { status, stdout, stderr };
}
