// SPDX-License-Identifier: Apache-2.0
import { createRoot } from 'react-dom/client';

import { App, type ApprovalDashboardProps } from './App.js';
import { type ApprovalUIComponents, ApprovalUIProvider } from './components.js';

export interface CreateApprovalDashboardOptions extends ApprovalDashboardProps {
  /**
   * Design-system adapter. Supply a full or partial slot map to style the
   * dashboard with your components; omitted slots (or omitting this entirely)
   * fall back to the built-in unstyled HTML adapter, which you can style via
   * the `flowsafe-*` class hooks. If you pass a CSS-based adapter (e.g. Astryx),
   * remember to load that library's stylesheet in your app entry.
   */
  components?: Partial<ApprovalUIComponents>;
}

/**
 * Mounts the dashboard into a host element:
 *
 *   const client = new ApprovalApiClient({ headers: { authorization: token } });
 *   const dashboard = createApprovalDashboard(el, { client, components });
 *   // later: dashboard.unmount();
 */
export function createApprovalDashboard(
  container: Element,
  { components, ...props }: CreateApprovalDashboardOptions,
): { unmount: () => void } {
  const root = createRoot(container);
  root.render(
    <ApprovalUIProvider components={components}>
      <App {...props} />
    </ApprovalUIProvider>,
  );
  return { unmount: () => root.unmount() };
}
