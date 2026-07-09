// The catalog/runtime consistency check every host runs at startup. It is the
// only guard tying an advertised WorkflowMeta.id to a committed workflow id, so
// it gets direct coverage rather than relying on each host's happy path.

import { describe, expect, it } from 'vitest';

import { assertWorkflowsRegistered } from './registration.js';
import type { WorkflowMeta } from './workflow-meta.js';

function meta(id: string): WorkflowMeta {
  return { id, title: id, description: '', sampleInput: {} };
}

function runtimeWith(ids: string[]): { workflowIds: () => string[] } {
  return { workflowIds: () => ids };
}

describe('assertWorkflowsRegistered', () => {
  it('passes when every advertised id was committed', () => {
    // #given / #when / #then
    expect(() =>
      assertWorkflowsRegistered(runtimeWith(['a', 'b', 'extra']), [
        meta('a'),
        meta('b'),
      ]),
    ).not.toThrow();
  });

  it('throws naming the workflow advertised but never registered', () => {
    // #given — the id drift a typo in a meta produces; without this check it
    // surfaces later as a mysterious 404 on a workflow the runtime does host
    expect(() =>
      assertWorkflowsRegistered(runtimeWith(['gtm-outbound']), [
        meta('gtm-outbound'),
        meta('gtm-oubtound'),
      ]),
    ).toThrow(/'gtm-oubtound' is advertised but was never registered/);
  });

  it('passes vacuously for an empty catalog', () => {
    // #given / #when / #then — a host may mount the run routes before adding
    // any workflow; that is not an error
    expect(() => assertWorkflowsRegistered(runtimeWith([]), [])).not.toThrow();
  });
});
