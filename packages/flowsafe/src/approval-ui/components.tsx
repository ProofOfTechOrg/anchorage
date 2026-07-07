// The styling contract. approval-ui renders every visual primitive through
// these slots, so an importer supplies their own design-system components
// (Astryx, MUI, Chakra, …) via <ApprovalUIProvider>. A plain-HTML default
// (`htmlComponents`) ships below — the dashboard works unstyled with zero
// config, and CSS-only consumers can target the `flowsafe-*` class hooks. This
// is the react-select / react-markdown / MDX `components` pattern.

import {
  createContext,
  type JSX,
  type ReactNode,
  useContext,
  useId,
  useMemo,
} from 'react';

import type { ApprovalRecord } from '../approval-api/types.js';

export type Tone = 'neutral' | 'info' | 'success' | 'warning' | 'danger';
export type StackDirection = 'vertical' | 'horizontal';
export type StackGap = 'sm' | 'md' | 'lg';
export type ButtonVariant = 'primary' | 'secondary' | 'danger' | 'ghost';

export interface StackProps {
  direction?: StackDirection;
  gap?: StackGap;
  children: ReactNode;
}
export interface SectionProps {
  'aria-label'?: string;
  children: ReactNode;
}
export interface HeadingProps {
  level: 1 | 2;
  children: ReactNode;
}
export interface TextProps {
  children: ReactNode;
}
export interface ButtonProps {
  label: string;
  onClick?: () => void;
  disabled?: boolean;
  variant?: ButtonVariant;
  type?: 'button' | 'submit';
  /** Conveys selection to assistive tech for toggle-like buttons. */
  pressed?: boolean;
}
export interface BadgeProps {
  label: ReactNode;
  tone?: Tone;
}
export interface BannerProps {
  tone: Tone;
  title: ReactNode;
}
export interface TextFieldProps {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  /** Renders a multiline control (textarea) when set. */
  rows?: number;
  /** Fires on Enter for single-line inputs. */
  onSubmit?: () => void;
}
export interface MetadataListProps {
  children: ReactNode;
}
export interface MetadataItemProps {
  label: string;
  children: ReactNode;
}
export interface CodeProps {
  code: string;
  language?: string;
}
export interface EmptyStateProps {
  title: string;
}
export interface SpinnerProps {
  label?: string;
}
export interface ApprovalColumn {
  key: string;
  header: string;
  renderCell: (record: ApprovalRecord) => ReactNode;
}
export interface TableProps {
  data: readonly ApprovalRecord[];
  columns: readonly ApprovalColumn[];
  /** Property naming each row's stable id (React keys). */
  idKey: keyof ApprovalRecord & string;
  emptyState?: ReactNode;
  /** Accessible name for the table (a `<table>` supports naming via its role). */
  'aria-label'?: string;
}

/**
 * Every slot approval-ui renders through. Supply a full or partial map to
 * <ApprovalUIProvider>; omitted slots fall back to `htmlComponents`. The Table
 * is typed to ApprovalRecord rows on purpose — this contract styles one
 * dashboard, it is not a general-purpose UI kit.
 */
export interface ApprovalUIComponents {
  Stack: (props: StackProps) => ReactNode;
  Section: (props: SectionProps) => ReactNode;
  Heading: (props: HeadingProps) => ReactNode;
  Text: (props: TextProps) => ReactNode;
  Button: (props: ButtonProps) => ReactNode;
  Badge: (props: BadgeProps) => ReactNode;
  Banner: (props: BannerProps) => ReactNode;
  TextField: (props: TextFieldProps) => ReactNode;
  MetadataList: (props: MetadataListProps) => ReactNode;
  MetadataItem: (props: MetadataItemProps) => ReactNode;
  Code: (props: CodeProps) => ReactNode;
  EmptyState: (props: EmptyStateProps) => ReactNode;
  Spinner: (props: SpinnerProps) => ReactNode;
  Table: (props: TableProps) => ReactNode;
}

// ---- Plain-HTML default adapter -------------------------------------------
// Semantic markup + `flowsafe-*` class hooks, no CSS. Functional out of the
// box; style via CSS, or replace slots with a design-system adapter.

function HtmlTable({
  data,
  columns,
  idKey,
  emptyState,
  'aria-label': ariaLabel,
}: TableProps): JSX.Element {
  if (data.length === 0 && emptyState !== undefined) {
    return <>{emptyState}</>;
  }
  return (
    <table className="flowsafe-table" aria-label={ariaLabel}>
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column.key} scope="col">
              {column.header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((record) => (
          <tr key={String(record[idKey])}>
            {columns.map((column) => (
              <td key={column.key}>{column.renderCell(record)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export const htmlComponents: ApprovalUIComponents = {
  Stack: ({ direction = 'vertical', gap = 'md', children }) => (
    <div
      className={`flowsafe-stack flowsafe-stack-${direction} flowsafe-gap-${gap}`}
    >
      {children}
    </div>
  ),
  Section: ({ 'aria-label': ariaLabel, children }) => (
    <section className="flowsafe-section" aria-label={ariaLabel}>
      {children}
    </section>
  ),
  Heading: ({ level, children }) =>
    level === 1 ? (
      <h1 className="flowsafe-heading">{children}</h1>
    ) : (
      <h2 className="flowsafe-heading">{children}</h2>
    ),
  Text: ({ children }) => <p className="flowsafe-text">{children}</p>,
  Button: ({
    label,
    onClick,
    disabled,
    variant = 'secondary',
    type = 'button',
    pressed,
  }) => (
    <button
      type={type}
      className={`flowsafe-button flowsafe-button-${variant}`}
      onClick={onClick}
      disabled={disabled}
      aria-pressed={pressed}
    >
      {label}
    </button>
  ),
  Badge: ({ label, tone = 'neutral' }) => (
    <span className={`flowsafe-badge flowsafe-tone-${tone}`}>{label}</span>
  ),
  Banner: ({ tone, title }) => (
    <p role="alert" className={`flowsafe-banner flowsafe-tone-${tone}`}>
      {title}
    </p>
  ),
  TextField: ({
    label,
    value,
    onChange,
    placeholder,
    disabled,
    rows,
    onSubmit,
  }) => {
    // useId associates the label with the control (satisfies a11y and lets
    // the ternary pick textarea vs input without breaking the association).
    const id = useId();
    return (
      <div className="flowsafe-field">
        <label htmlFor={id}>{label}</label>
        {rows !== undefined ? (
          <textarea
            id={id}
            className="flowsafe-textarea"
            value={value}
            rows={rows}
            disabled={disabled}
            placeholder={placeholder}
            onChange={(event) => onChange(event.currentTarget.value)}
          />
        ) : (
          <input
            id={id}
            className="flowsafe-input"
            value={value}
            disabled={disabled}
            placeholder={placeholder}
            onChange={(event) => onChange(event.currentTarget.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && onSubmit) {
                event.preventDefault();
                onSubmit();
              }
            }}
          />
        )}
      </div>
    );
  },
  MetadataList: ({ children }) => (
    <dl className="flowsafe-metadata">{children}</dl>
  ),
  MetadataItem: ({ label, children }) => (
    <div className="flowsafe-metadata-item">
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  ),
  Code: ({ code }) => <pre className="flowsafe-code">{code}</pre>,
  EmptyState: ({ title }) => <p className="flowsafe-empty">{title}</p>,
  Spinner: ({ label }) => (
    <output className="flowsafe-spinner">{label ?? 'Loading…'}</output>
  ),
  Table: HtmlTable,
};

// ---- Injection seam --------------------------------------------------------

const ApprovalUIContext = createContext<ApprovalUIComponents>(htmlComponents);

export interface ApprovalUIProviderProps {
  /** Override any subset of slots; the rest fall back to `htmlComponents`. */
  components?: Partial<ApprovalUIComponents>;
  children: ReactNode;
}

export function ApprovalUIProvider({
  components,
  children,
}: ApprovalUIProviderProps): JSX.Element {
  const merged = useMemo(
    () => ({ ...htmlComponents, ...components }),
    [components],
  );
  return (
    <ApprovalUIContext.Provider value={merged}>
      {children}
    </ApprovalUIContext.Provider>
  );
}

export function useApprovalUIComponents(): ApprovalUIComponents {
  return useContext(ApprovalUIContext);
}
