// The 60-second tour dialog. Shows once per browser (dismissal persisted in
// localStorage — a harmless UI flag; tokens stay memory-only), reopenable from
// the header. The copy is the demo's four-beat story: guardrail scenarios run
// the real library in-browser → the wire transfer suspends at a real gate and
// is decided in the queue below (SoD is server-enforced) → the approval
// derives a suspension-bound grant → dangerous effects are simulated,
// machinery is real.

import { Button } from '@astryxdesign/core/Button';
import { Dialog } from '@astryxdesign/core/Dialog';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { VStack } from '@astryxdesign/core/VStack';
import { type ReactElement, useCallback, useState } from 'react';

import { MarkerRow } from '@/marker-row';

const DISMISS_KEY = 'anchorage-tour-dismissed';

function readDismissed(): boolean {
  try {
    return window.localStorage.getItem(DISMISS_KEY) === 'true';
  } catch {
    return false; // storage unavailable (private mode) — just show the tour
  }
}

function persistDismissed(): void {
  try {
    window.localStorage.setItem(DISMISS_KEY, 'true');
  } catch {
    // best effort — the tour re-shows next visit, nothing breaks
  }
}

export interface IntroTour {
  isOpen: boolean;
  /** Header "?" — reopen on demand. */
  open: () => void;
  close: () => void;
}

export function useIntroTour(): IntroTour {
  const [isOpen, setIsOpen] = useState(() => !readDismissed());
  const open = useCallback(() => setIsOpen(true), []);
  const close = useCallback(() => {
    persistDismissed();
    setIsOpen(false);
  }, []);
  return { isOpen, open, close };
}

const TOUR_BULLETS: readonly string[] = [
  'Pick a scenario in the control room. A scripted agent runs straight into the real breakwater enforcement path in your browser — no run budget spent. Switch roles to see the decisions change.',
  "Run the wire transfer scenario: it starts a real durable run that suspends at an approval gate, then decide it in the queue right below. Separation of duties is server-enforced: whoever advanced the run into its gate can't decide it (this demo lets admin self-decide).",
  'The approval derives a capability grant bound to that exact suspension, and the run resumes server-side behind four connector gates.',
  'Want more? Further down, launch the example workflows — product-launch has two gates. Dangerous things are simulated (no live bindings); the machinery is real: grants, RBAC, scoped isolation keys, durable suspend/resume.',
];

export function IntroTourDialog({
  tour,
  onStartTour,
}: {
  tour: IntroTour;
  /** Primary CTA — dismisses and jumps to the control room. */
  onStartTour: () => void;
}): ReactElement {
  return (
    <Dialog
      isOpen={tour.isOpen}
      onOpenChange={(next) => {
        if (!next) tour.close();
      }}
      width={560}
      padding={5}
    >
      <VStack gap={4}>
        <Heading level={2}>A 60-second tour</Heading>
        <VStack gap={2}>
          {TOUR_BULLETS.map((bullet, index) => (
            <MarkerRow key={bullet} marker={`${index + 1}.`}>
              {bullet}
            </MarkerRow>
          ))}
        </VStack>
        <HStack gap={2}>
          <Button
            label="Try a scenario"
            variant="primary"
            onClick={() => {
              tour.close();
              onStartTour();
            }}
          />
          <Button
            label="Just look around"
            variant="ghost"
            onClick={tour.close}
          />
        </HStack>
      </VStack>
    </Dialog>
  );
}
