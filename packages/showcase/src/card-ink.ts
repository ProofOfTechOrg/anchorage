// Toned ink for tinted SelectableCard grids (launcher + control room). The
// y2k card tints keep the SAME light hex in dark mode, but
// --color-text-primary flips to near-white there — so an unstyled card title
// renders near-white on pastel. Re-point the text vars at the variant's
// matched --color-text-<variant> (dark in BOTH modes — the theme's own
// Banner/Token pairing), and set `color` so the hover overlay's currentColor
// tint stays dark-on-pastel too. Custom properties, not a bare color: the
// theme styles Text via `color: var(--color-text-primary)`.

import type { CardVariant } from '@astryxdesign/core/Card';
import type { CSSProperties } from 'react';

/** The Card variants that keep the theme's mode-aware ink (no tint to fight). */
const UNTONED_VARIANTS: readonly CardVariant[] = [
  'default',
  'transparent',
  'muted',
];

function cardInk(variant: CardVariant): CSSProperties | undefined {
  if (UNTONED_VARIANTS.includes(variant)) return undefined;
  const ink = `var(--color-text-${variant})`;
  return {
    color: ink,
    '--color-text-primary': ink,
    '--color-text-secondary': ink,
  } as CSSProperties;
}

/**
 * id→ink map DERIVED from an id→variant map, never hand-repeated: a parallel
 * hand-written tone map would silently drift on a recolor and reinstate the
 * wash-out this fixes.
 */
export function cardInkMap(
  variants: Record<string, CardVariant>,
): Record<string, CSSProperties | undefined> {
  return Object.fromEntries(
    Object.entries(variants).map(([id, variant]) => [id, cardInk(variant)]),
  );
}
