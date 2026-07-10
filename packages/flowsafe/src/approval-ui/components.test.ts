// Typechecked by the UI test pass (src/approval-ui/tsconfig.test.json), not
// the package test pass — this file imports the .tsx contract, which the
// workers-typed program cannot compile. No jsdom: the default slots are
// inspected as element objects, never rendered.

import type { ReactElement } from 'react';
import { describe, expect, it } from 'vitest';

import {
  type ApprovalUIComponents,
  htmlComponents,
  type InfoTipProps,
} from './components.js';

interface InfoTipElementProps {
  className: string;
  title: string;
  children: unknown;
}

function renderDefaultInfoTip(props: InfoTipProps): InfoTipElementProps {
  const element = htmlComponents.InfoTip(
    props,
  ) as ReactElement<InfoTipElementProps>;
  expect(element.type).toBe('span');
  return element.props;
}

describe('htmlComponents.InfoTip', () => {
  it('renders a span carrying the tip as a native title attribute', () => {
    const props = renderDefaultInfoTip({ label: 'SLA', tip: 'decide-by' });
    expect(props.title).toBe('decide-by');
    expect(props.children).toBe('SLA');
    expect(props.className).toBe('flowsafe-infotip');
  });

  it('passes an arbitrary ReactNode label through as children', () => {
    const label = htmlComponents.Badge({ label: 'pending' });
    const props = renderDefaultInfoTip({ label, tip: 'status' });
    expect(props.children).toBe(label);
  });
});

describe('htmlComponents.EmptyState', () => {
  interface EmptyStateElementProps {
    className: string;
    children: unknown;
  }

  function renderDefaultEmptyState(props: {
    title: string;
    description?: string;
  }): ReactElement<EmptyStateElementProps>[] {
    // #given / #when — the default slot as an element object (no jsdom)
    const element = htmlComponents.EmptyState(
      props,
    ) as ReactElement<EmptyStateElementProps>;
    expect(element.type).toBe('div');
    expect(element.props.className).toBe('flowsafe-empty');
    const children = element.props
      .children as Array<ReactElement<EmptyStateElementProps> | null>;
    return children.filter(
      (child): child is ReactElement<EmptyStateElementProps> => child !== null,
    );
  }

  it('renders the description as a second paragraph when provided', () => {
    // #when
    const children = renderDefaultEmptyState({
      title: 'Nothing here',
      description: 'Launch something.',
    });

    // #then
    expect(children).toHaveLength(2);
    expect(children[1]?.props.className).toBe('flowsafe-empty-description');
    expect(children[1]?.props.children).toBe('Launch something.');
  });

  it('omits the description paragraph when absent (backward compatible)', () => {
    // #when
    const children = renderDefaultEmptyState({ title: 'Nothing here' });

    // #then
    expect(children).toHaveLength(1);
    expect(children[0]?.props.className).toBe('flowsafe-empty-title');
  });
});

describe('provider merge semantics', () => {
  // Mirrors ApprovalUIProvider's merge ({...htmlComponents, ...components})
  // without mounting the provider — hooks need a renderer, the merge does not.
  it('falls back to the default InfoTip when an adapter omits the slot', () => {
    const partial: Partial<ApprovalUIComponents> = {
      Text: htmlComponents.Text,
    };
    const merged = { ...htmlComponents, ...partial };
    expect(merged.InfoTip).toBe(htmlComponents.InfoTip);
  });

  it('prefers an adapter-supplied InfoTip over the default', () => {
    const custom: ApprovalUIComponents['InfoTip'] = ({ label }) => label;
    const merged = { ...htmlComponents, ...{ InfoTip: custom } };
    expect(merged.InfoTip).toBe(custom);
  });
});
