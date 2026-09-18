// SPDX-License-Identifier: Apache-2.0
// Spawn seam contract — the process result and the injectable runner.
//
// A type-only leaf, so default-exec.ts implements the seam without importing
// the barrel that imports it back.

/** Raw process result returned by an injected {@link AgentCliExec}. */
export interface AgentCliExecResult {
  /** Standard output captured from the process. */
  stdout: string;
  /** Standard error captured from the process. */
  stderr: string;
  /** Integer process exit code. */
  exitCode: number;
}

/** Spawn seam — inject in tests or to sandbox/containerize execution. */
export type AgentCliExec = (
  command: string,
  args: readonly string[],
  options: { cwd?: string; timeoutMs: number },
) => Promise<AgentCliExecResult>;
