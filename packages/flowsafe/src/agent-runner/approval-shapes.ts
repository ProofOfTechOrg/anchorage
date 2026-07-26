// SPDX-License-Identifier: Apache-2.0
// Durable-agent approval-suspend shape parsing (validation finding R-003).
//
// The durable-agentic-loop's tool-call step suspends for approval in TWO
// payload shapes, both validated against @mastra/core 1.50.0 dist
// (agent/durable/index.js):
//
//   FLAT   (pre-exec gate,  :3283): { type:'approval', toolCallId, toolName, args }
//   NESTED (mid-exec,       :3385): { type:'approval', requireToolApproval:{ toolCallId, toolName, args } }
//
// Both carry `resumeLabel: toolCallId` and a `{ approved: boolean }` resume
// schema. A workflow STEP gate declares its own grants in an explicit
// `connectors` array; an AGENT gate instead names the tool the model wants to
// call by its provider-visible `toolName`. For a breakwater connector this must
// be the SAME provider-safe id (`[A-Za-z0-9_-]+`) the write gate checks.
// Providers rewrite punctuation-bearing ids such as `salesforce.createContact`
// before returning a tool call, and that rewrite is not reversible in the
// suspend payload. Hosts using automatic agent-gate grants must therefore give
// those connectors provider-safe ids. agentGateConnectors() mints [toolName],
// and the runtime's approvalGrantProvider derives that exact grant from the
// APPROVED record on resume.
//
// Pure and dependency-free (no @mastra import): it inspects a plain payload
// object only, so the record-creation/bridge path (host-kit/approval-bridge.ts)
// can call it without dragging the durable Agent — and therefore @mastra's
// Node built-ins — into any browser-reachable module.

/** The suspend-payload `type` discriminator core stamps on an approval gate. */
export const AGENT_APPROVAL_SUSPEND_TYPE = 'approval';

const PROVIDER_SAFE_TOOL_NAME = /^[A-Za-z0-9_-]+$/;

/** The tool-call identity parsed out of an agent approval suspend payload. */
export interface AgentApprovalSuspend {
  toolCallId?: string;
  toolName?: string;
  args?: Record<string, unknown>;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object'
    ? (value as Record<string, unknown>)
    : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

/**
 * Parse a durable-agent approval suspend payload — a PER-STEP payload, already
 * unwrapped from RunSummary.suspendPayload's step-keyed map — into its tool-call
 * identity, reading both supported shapes: the nested `requireToolApproval` object
 * when present, else the FLAT top-level fields.
 *
 * Returns undefined for any payload that is NOT an agent tool-call gate: a
 * non-object, a non-'approval' `type`, or a workflow-step gate that names no
 * tool (it declares its own `connectors` instead — the caller keeps its
 * existing handling for those). Fail-closed: a payload naming no tool at all
 * yields undefined so agentGateConnectors() invents no grant.
 */
export function parseAgentApprovalSuspend(
  payload: unknown,
): AgentApprovalSuspend | undefined {
  const record = asRecord(payload);
  if (record === undefined) return undefined;
  if (record.type !== AGENT_APPROVAL_SUSPEND_TYPE) return undefined;
  // The two shapes are disjoint: NESTED carries a `requireToolApproval` object
  // (mid-exec) and FLAT carries the fields at top level (pre-exec). Resolve the
  // source as a UNIT — whichever shape is present — rather than field-by-field,
  // so a malformed payload can never produce a mixed-provenance result.
  const source = asRecord(record.requireToolApproval) ?? record;
  const toolCallId = asString(source.toolCallId);
  const toolName = asString(source.toolName);
  const args = asRecord(source.args);
  // An 'approval' payload that names neither a tool nor a call is not an agent
  // tool-call gate we can round-trip — treat it as "not an agent gate" so the
  // caller falls back to its workflow-step handling and no grant is invented.
  if (toolName === undefined && toolCallId === undefined) return undefined;
  const parsed: AgentApprovalSuspend = {};
  if (toolCallId !== undefined) parsed.toolCallId = toolCallId;
  if (toolName !== undefined) parsed.toolName = toolName;
  if (args !== undefined) parsed.args = args;
  return parsed;
}

/**
 * The breakwater connector ids an approved agent gate should grant: the single
 * provider-safe tool id the model called (== the connector id the write gate
 * checks; see the file header's provider-rewrite constraint). Requires
 * BOTH a non-empty `toolName` AND a non-empty `toolCallId` — a real
 * durable-agent gate always carries both (toolCallId is the suspend's
 * `resumeLabel`), so demanding both never rejects a real gate while narrowing
 * the surface where a workflow-step gate COINCIDENTALLY shaped `{type:'approval',
 * toolName}` would mint an unintended grant (a workflow author's own convention
 * is an explicit `connectors` array, which requestedConnectors takes first).
 * Returns [] otherwise, so a decision never invents a grant the suspension did
 * not request (fail-closed).
 */
export function agentGateConnectors(payload: unknown): string[] {
  const parsed = parseAgentApprovalSuspend(payload);
  return parsed?.toolName !== undefined &&
    parsed.toolName.length > 0 &&
    PROVIDER_SAFE_TOOL_NAME.test(parsed.toolName) &&
    parsed.toolCallId !== undefined &&
    parsed.toolCallId.length > 0
    ? [parsed.toolName]
    : [];
}
