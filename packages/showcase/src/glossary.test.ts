import { describe, expect, it } from 'vitest';

import {
  claimableSteps,
  GLOSSARY,
  ROLE_NOTES,
  WORKFLOW_GUIDES,
  ZONES,
} from '@/glossary';

const WORKFLOW_IDS = [
  'wire-transfer',
  'gtm-outbound',
  'content-pipeline',
  'lead-generation',
  'product-launch',
  'access-request',
];

const WORKFLOW_TOPOLOGY = {
  'wire-transfer': {
    steps: ['prepareTransfer', 'approveTransfer', 'releaseFunds'],
    gateSteps: ['approveTransfer'],
  },
  'gtm-outbound': {
    steps: [
      'researchAccounts',
      'enrichContacts',
      'generateOutreach',
      'reviewAndApprove',
      'sendOutreach',
    ],
    gateSteps: ['reviewAndApprove'],
  },
  'content-pipeline': {
    steps: [
      'researchTopic',
      'writeIntro',
      'writeBody',
      'writeConclusion',
      'reviewContent',
      'publishContent',
    ],
    gateSteps: ['reviewContent'],
  },
  'lead-generation': {
    steps: [
      'scoreLeads',
      'fastTrack',
      'nurture',
      'reviewHotLeads',
      'assignLeads',
    ],
    gateSteps: ['reviewHotLeads'],
    conditionalSteps: ['fastTrack', 'nurture'],
  },
  'product-launch': {
    steps: [
      'validateReadiness',
      'approveLaunch',
      'executeLaunch',
      'confirmRollout',
      'completeLaunch',
    ],
    gateSteps: ['approveLaunch', 'confirmRollout'],
  },
  'access-request': {
    steps: ['requestAccess', 'approveAccess', 'grantAccess'],
    gateSteps: ['approveAccess'],
  },
} as const;

describe('glossary completeness', () => {
  it('describes every narration zone', () => {
    for (const zone of ['browser', 'worker', 'do', 'd1', 'alarm'] as const) {
      expect(ZONES[zone].label.length).toBeGreaterThan(0);
      expect(ZONES[zone].blurb.length).toBeGreaterThan(0);
    }
  });

  it('contains no obsolete data-plane scheduled-trigger terminology', () => {
    const sources = import.meta.glob(
      [
        './glossary.ts',
        './narration.ts',
        './architecture-legend.tsx',
        './zone-badge.tsx',
        '../worker/worker.ts',
      ],
      { eager: true, import: 'default', query: '?raw' },
    ) as Record<string, string>;
    for (const [source, contents] of Object.entries(sources)) {
      expect(contents, source).not.toMatch(/\bcron(?:s)?\b/i);
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
  it('covers exactly the six showcase workflows', () => {
    expect(Object.keys(WORKFLOW_GUIDES).sort()).toEqual(
      [...WORKFLOW_IDS].sort(),
    );
  });

  it('pins workflow steps, gates, and conditional branches to the definitions', () => {
    for (const [id, guide] of Object.entries(WORKFLOW_GUIDES)) {
      const expected = WORKFLOW_TOPOLOGY[id as keyof typeof WORKFLOW_TOPOLOGY];
      expect(guide.steps, `${id} steps`).toEqual(expected.steps);
      expect(guide.gateSteps, `${id} gates`).toEqual(expected.gateSteps);
      expect(guide.conditionalSteps, `${id} conditional steps`).toEqual(
        'conditionalSteps' in expected ? expected.conditionalSteps : undefined,
      );
      expect(guide.note.length, id).toBeGreaterThan(0);
      expect(guide.capabilities.length, id).toBeGreaterThan(0);
      for (const capability of guide.capabilities) {
        expect(capability.label.length).toBeGreaterThan(0);
        expect(capability.tip.length).toBeGreaterThan(0);
      }
    }
  });

  it('pins the showcase branch narration', () => {
    expect(WORKFLOW_GUIDES['lead-generation']?.shortCircuitNote).toBeDefined();
    // lead-generation is the one .branch() workflow: its branch targets must
    // be marked conditional or narration over-claims skipped branches.
    for (const [id, guide] of Object.entries(WORKFLOW_GUIDES)) {
      for (const step of guide.conditionalSteps ?? []) {
        expect(guide.steps, `${id} conditional ${step}`).toContain(step);
      }
    }
  });
});

describe('claimableSteps', () => {
  const leadGen = WORKFLOW_GUIDES['lead-generation'];
  const gtm = WORKFLOW_GUIDES['gtm-outbound'];

  it('never claims branch-conditional steps, suspended or not', () => {
    // suspended at the gate: only the unconditional pre-gate step remains
    expect(claimableSteps(leadGen, 'reviewHotLeads')).toEqual(['scoreLeads']);
    // terminal (all-cold short-circuit): branch steps still excluded
    expect(claimableSteps(leadGen, undefined)).toEqual([
      'scoreLeads',
      'reviewHotLeads',
      'assignLeads',
    ]);
  });

  it('claims the full pre-gate slice for pure .then() chains', () => {
    expect(claimableSteps(gtm, 'reviewAndApprove')).toEqual([
      'researchAccounts',
      'enrichContacts',
      'generateOutreach',
    ]);
    expect(claimableSteps(gtm, undefined)).toEqual([...(gtm?.steps ?? [])]);
  });

  it('returns undefined without a guide or when nothing is claimable', () => {
    expect(claimableSteps(undefined, 'x')).toBeUndefined();
    // suspended at the FIRST step ⇒ nothing ran before it to claim
    expect(
      claimableSteps(
        { steps: ['gate'], gateSteps: ['gate'], capabilities: [], note: 'n' },
        'gate',
      ),
    ).toBeUndefined();
    // unknown suspended step (guide drift) ⇒ claim nothing rather than guess
    expect(claimableSteps(gtm, 'notARealStep')).toBeUndefined();
  });
});
