// SPDX-License-Identifier: Apache-2.0
// R-003: the durable-agent approval gate suspends in TWO payload shapes and the
// record-creation/bridge path must round-trip BOTH. These pin the pure parser
// the bridge shares.

import { describe, expect, it } from 'vitest';

import {
  agentGateConnectors,
  parseAgentApprovalSuspend,
} from './approval-shapes.js';

// The two shapes, verbatim from @mastra/core 1.50.0 dist (agent/durable/index.js):
const FLAT = {
  type: 'approval',
  toolCallId: 'call-1',
  toolName: 'blog-publisher',
  args: { topic: 'launch' },
};
const NESTED = {
  type: 'approval',
  requireToolApproval: {
    toolCallId: 'call-1',
    toolName: 'blog-publisher',
    args: { topic: 'launch' },
  },
};

describe('parseAgentApprovalSuspend', () => {
  it('parses the FLAT (pre-exec gate) shape', () => {
    // #when / #then
    expect(parseAgentApprovalSuspend(FLAT)).toEqual({
      toolCallId: 'call-1',
      toolName: 'blog-publisher',
      args: { topic: 'launch' },
    });
  });

  it('parses the NESTED (mid-exec) shape', () => {
    // #then — same identity via requireToolApproval.X ?? X
    expect(parseAgentApprovalSuspend(NESTED)).toEqual({
      toolCallId: 'call-1',
      toolName: 'blog-publisher',
      args: { topic: 'launch' },
    });
  });

  it('prefers the nested fields when both are present (mid-exec wins)', () => {
    // #given a payload carrying both a nested and a stale flat toolName
    const mixed = {
      type: 'approval',
      toolName: 'flat-tool',
      requireToolApproval: { toolCallId: 'c', toolName: 'nested-tool' },
    };
    // #then the nested object is read as a UNIT — its toolName wins
    expect(parseAgentApprovalSuspend(mixed)?.toolName).toBe('nested-tool');
  });

  it('reads the nested block as a unit — an empty requireToolApproval does not mix in flat fields', () => {
    // #given a (malformed) nested shape whose requireToolApproval is empty, with
    // flat fields also present
    const payload = {
      type: 'approval',
      toolCallId: 'flat-call',
      toolName: 'flat-tool',
      requireToolApproval: {},
    };
    // #then the empty nested object is the source (no per-field fallback to
    // flat), so nothing parses — fail closed, no mixed-provenance result
    expect(parseAgentApprovalSuspend(payload)).toBeUndefined();
  });

  it('returns undefined for a non-object payload', () => {
    expect(parseAgentApprovalSuspend(null)).toBeUndefined();
    expect(parseAgentApprovalSuspend('approval')).toBeUndefined();
    expect(parseAgentApprovalSuspend(undefined)).toBeUndefined();
  });

  it('returns undefined for a non-approval suspend type', () => {
    // #given a plain tool-driven suspension (type 'suspension', not 'approval')
    expect(
      parseAgentApprovalSuspend({ type: 'suspension', reason: 'waiting' }),
    ).toBeUndefined();
  });

  it('returns undefined for a workflow-step gate that names no tool', () => {
    // #given a workflow gate declaring its own connectors, no toolName/toolCallId
    expect(
      parseAgentApprovalSuspend({ type: 'approval', connectors: ['x'] }),
    ).toBeUndefined();
    // a bare reason payload is likewise not an agent tool-call gate
    expect(
      parseAgentApprovalSuspend({ type: 'approval', reason: 'go' }),
    ).toBeUndefined();
  });

  it('ignores a non-string toolName (fail closed)', () => {
    expect(
      parseAgentApprovalSuspend({ type: 'approval', toolName: 42 }),
    ).toBeUndefined();
  });
});

describe('agentGateConnectors', () => {
  it('derives [toolName] from the FLAT shape', () => {
    expect(agentGateConnectors(FLAT)).toEqual(['blog-publisher']);
  });

  it('derives [toolName] from the NESTED shape', () => {
    expect(agentGateConnectors(NESTED)).toEqual(['blog-publisher']);
  });

  it('mints nothing for a non-agent payload', () => {
    expect(
      agentGateConnectors({ type: 'approval', connectors: ['x'] }),
    ).toEqual([]);
    expect(agentGateConnectors({ reason: 'go' })).toEqual([]);
    expect(agentGateConnectors(null)).toEqual([]);
  });

  it('mints nothing when the tool name is empty (fail closed)', () => {
    expect(
      agentGateConnectors({ type: 'approval', toolName: '', toolCallId: 'c' }),
    ).toEqual([]);
  });

  it('mints nothing for a punctuation-bearing tool name that a provider can rewrite', () => {
    expect(
      agentGateConnectors({
        type: 'approval',
        toolName: 'salesforce.createContact',
        toolCallId: 'c',
      }),
    ).toEqual([]);
  });

  it('requires a toolCallId too, narrowing the workflow-gate collision (fail closed)', () => {
    // #given a payload shaped like an agent gate but WITHOUT the toolCallId a
    // real durable-agent suspension always carries (its resumeLabel)
    // #then no grant is derived — a bare {type:'approval', toolName} is not
    // enough to mint (guards against a coincidental workflow-gate collision)
    expect(
      agentGateConnectors({
        type: 'approval',
        toolName: 'looks-like-connector',
      }),
    ).toEqual([]);
  });
});
