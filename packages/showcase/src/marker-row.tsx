// The one marker+text row primitive behind the explainer surfaces — the
// landing's demo points, the tour steps, and the reality legend — extracted
// on the third copy so their row styling cannot drift apart.

import { HStack } from '@astryxdesign/core/HStack';
import { Text } from '@astryxdesign/core/Text';
import type { ComponentProps, ReactElement, ReactNode } from 'react';

export function MarkerRow({
  marker,
  markerHidden = false,
  color,
  children,
}: {
  marker: ReactNode;
  /**
   * Hide purely decorative markers (a bullet glyph) from screen readers;
   * ordinals and glyph+title legend markers stay audible.
   */
  markerHidden?: boolean;
  color?: ComponentProps<typeof Text>['color'];
  children: ReactNode;
}): ReactElement {
  return (
    <HStack gap={2} align="start">
      <Text size="sm" weight="semibold" aria-hidden={markerHidden || undefined}>
        {marker}
      </Text>
      <Text size="sm" color={color}>
        {children}
      </Text>
    </HStack>
  );
}
