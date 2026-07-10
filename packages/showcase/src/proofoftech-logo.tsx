// The Proof of Tech logo mark — the interlocked-squares GEOMETRY from
// proofoftech.org's public/logo-mark.svg, re-parameterized: the source SVG
// paints its notch with the site's canvas color, which only reads as a
// cut-out on that exact background. Here the notch is a real even-odd hole
// (the two squares' overlap), so whatever surface is behind shows through,
// and the ink is currentColor so the mark follows the theme's text color.
// Inlined as JSX: no extra asset request, and aria-hidden because the
// adjacent text names the brand.

import type { ReactElement } from 'react';

export function ProofOfTechMark({
  size = 20,
}: {
  size?: number;
}): ReactElement {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 32 32"
      width={size}
      height={size}
      aria-hidden="true"
    >
      {/* Even-odd: the 4x4 overlap of the two squares self-cancels into
          the notch, so no background-colored third rect is needed. */}
      <path
        fillRule="evenodd"
        fill="currentColor"
        d="M2 14h16v16H2zM14 2h16v16H14z"
      />
    </svg>
  );
}
