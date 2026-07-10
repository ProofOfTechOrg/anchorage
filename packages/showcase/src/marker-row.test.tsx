// @vitest-environment jsdom
// Component smoke test — proves the jsdom + testing-library pipeline works
// end-to-end (aliases, JSX transform, Astryx rendering, jest-dom matchers).
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MarkerRow } from '@/marker-row';

describe('MarkerRow', () => {
  it('renders the marker and the row text', () => {
    render(<MarkerRow marker="1.">Launch a workflow</MarkerRow>);
    expect(screen.getByText('Launch a workflow')).toBeInTheDocument();
    // An ordinal marker stays audible: no aria-hidden.
    expect(screen.getByText('1.')).not.toHaveAttribute('aria-hidden');
  });

  it('hides purely decorative markers from screen readers', () => {
    render(
      <MarkerRow marker="•" markerHidden>
        A decorative bullet row
      </MarkerRow>,
    );
    expect(screen.getByText('•')).toHaveAttribute('aria-hidden', 'true');
  });
});
