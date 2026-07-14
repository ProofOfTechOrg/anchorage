// SPDX-License-Identifier: Apache-2.0
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

  it('falls back to default Checkbox/Select when a pre-triage adapter omits them (additive contract)', () => {
    // #given — an adapter written before the triage slots existed supplies a
    // partial map with neither; the merge must keep it working unmodified.
    // (The defaults use useId, like TextField, so they are pinned here by
    // identity + presence, not by direct invocation — hooks need a renderer.)
    const preTriageAdapter: Partial<ApprovalUIComponents> = {
      Text: htmlComponents.Text,
      Button: htmlComponents.Button,
    };

    // #when
    const merged = { ...htmlComponents, ...preTriageAdapter };

    // #then
    expect(merged.Checkbox).toBe(htmlComponents.Checkbox);
    expect(merged.Select).toBe(htmlComponents.Select);
    expect(typeof merged.Checkbox).toBe('function');
    expect(typeof merged.Select).toBe('function');
  });

  it('falls back to default Toast/PresenceIndicator when an adapter omits the M-007 slots (additive contract)', () => {
    // #given — an adapter written before the live-streaming slots existed
    const preStreamAdapter: Partial<ApprovalUIComponents> = {
      Text: htmlComponents.Text,
      Banner: htmlComponents.Banner,
    };

    // #when
    const merged = { ...htmlComponents, ...preStreamAdapter };

    // #then — the merge fills both optional slots from the HTML defaults
    expect(merged.Toast).toBe(htmlComponents.Toast);
    expect(merged.PresenceIndicator).toBe(htmlComponents.PresenceIndicator);
    expect(typeof merged.Toast).toBe('function');
    expect(typeof merged.PresenceIndicator).toBe('function');
  });

  it('prefers an adapter-supplied InfoTip over the default', () => {
    const custom: ApprovalUIComponents['InfoTip'] = ({ label }) => label;
    const merged = { ...htmlComponents, ...{ InfoTip: custom } };
    expect(merged.InfoTip).toBe(custom);
  });
});

describe('htmlComponents.Toast', () => {
  interface ToastElementProps {
    role: string;
    className: string;
    children: unknown;
  }

  it('renders a status region carrying the tone class and title', () => {
    // #when
    const element = htmlComponents.Toast({
      tone: 'warning',
      title: 'clash',
    }) as ReactElement<ToastElementProps>;

    // #then
    expect(element.type).toBe('div');
    expect(element.props.role).toBe('status');
    expect(element.props.className).toBe(
      'flowsafe-toast flowsafe-tone-warning',
    );
  });

  it('renders a dismiss button wired to onDismiss when provided', () => {
    // #given
    const onDismiss = (): void => {};

    // #when
    const element = htmlComponents.Toast({
      tone: 'danger',
      title: 'x',
      onDismiss,
    }) as ReactElement<{
      children: Array<ReactElement<{ onClick: () => void }> | null>;
    }>;

    // #then — the second child is the dismiss button, calling back onDismiss
    const button = element.props.children[1];
    expect(button?.type).toBe('button');
    expect(button?.props.onClick).toBe(onDismiss);
  });

  it('omits the dismiss button when onDismiss is absent', () => {
    // #when
    const element = htmlComponents.Toast({
      tone: 'info',
      title: 'x',
    }) as ReactElement<{ children: unknown[] }>;

    // #then
    expect(element.props.children[1]).toBeNull();
  });
});

describe('htmlComponents.PresenceIndicator', () => {
  interface PresenceElementProps {
    className: string;
    'aria-label': string;
    children: unknown;
  }

  it('renders a labelled list with one item per member', () => {
    // #when
    const element = htmlComponents.PresenceIndicator({
      members: [
        { actorId: 'ada', role: 'admin' },
        { actorId: 'ray', role: 'reviewer' },
      ],
    }) as ReactElement<PresenceElementProps>;

    // #then
    expect(element.type).toBe('ul');
    expect(element.props.className).toBe('flowsafe-presence');
    expect(element.props['aria-label']).toBe('Reviewers online');
    expect((element.props.children as unknown[]).length).toBe(2);
  });

  it('renders an empty list for an empty roster', () => {
    // #when
    const element = htmlComponents.PresenceIndicator({
      members: [],
    }) as ReactElement<PresenceElementProps>;

    // #then
    expect((element.props.children as unknown[]).length).toBe(0);
  });
});
