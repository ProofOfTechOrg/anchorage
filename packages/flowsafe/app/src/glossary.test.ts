import { describe, expect, it } from 'vitest';

import { GLOSSARY, ROLE_NOTES, WORKFLOW_GUIDES, ZONES } from './glossary.js';

const WORKFLOW_IDS = [
  'gtm-outbound',
  'content-pipeline',
  'lead-generation',
  'product-launch',
  'access-request',
];

describe('glossary completeness', () => {
  it('describes every narration zone', () => {
    for (const zone of ['browser', 'worker', 'do', 'd1', 'cron'] as const) {
      expect(ZONES[zone].label.length).toBeGreaterThan(0);
      expect(ZONES[zone].blurb.length).toBeGreaterThan(0);
    }
  });

  it('has a non-empty entry for every concept key', () => {
    for (const [key, tip] of Object.entries(GLOSSARY)) {
      expect(tip.length, key).toBeGreaterThan(0);
    }
  });

  it('describes all five demo roles', () => {
    for (const role of ['admin', 'operator', 'reviewer', 'viewer', 'builder']) {
      expect(ROLE_NOTES[role]?.length, role).toBeGreaterThan(0);
    }
  });
});

describe('workflow guides', () => {
  it('covers exactly the five showcase workflows', () => {
    expect(Object.keys(WORKFLOW_GUIDES).sort()).toEqual(
      [...WORKFLOW_IDS].sort(),
    );
  });

  it('lists steps in definition order with every gate among them', () => {
    for (const [id, guide] of Object.entries(WORKFLOW_GUIDES)) {
      expect(guide.steps.length, id).toBeGreaterThan(0);
      for (const gate of guide.gateSteps) {
        expect(guide.steps, `${id} gate ${gate}`).toContain(gate);
      }
      expect(guide.note.length, id).toBeGreaterThan(0);
      expect(guide.capabilities.length, id).toBeGreaterThan(0);
      for (const capability of guide.capabilities) {
        expect(capability.label.length).toBeGreaterThan(0);
        expect(capability.tip.length).toBeGreaterThan(0);
      }
    }
  });

  it('pins the showcase gate topology the narration relies on', () => {
    expect(WORKFLOW_GUIDES['product-launch']?.gateSteps).toEqual([
      'approveLaunch',
      'confirmRollout',
    ]);
    expect(WORKFLOW_GUIDES['lead-generation']?.shortCircuitNote).toBeDefined();
  });
});
