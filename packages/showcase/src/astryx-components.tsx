// Astryx adapter for approval-ui's slot contract. This is the ONLY place the
// app maps the library's design-agnostic slots onto @astryxdesign/core
// components — the library itself has no Astryx dependency. Swap this module
// for an MUI/Chakra/etc. adapter to restyle the whole dashboard.

import {
  Badge as AstryxBadge,
  type BadgeVariant,
} from '@astryxdesign/core/Badge';
import {
  Banner as AstryxBanner,
  type BannerStatus,
} from '@astryxdesign/core/Banner';
import {
  Button as AstryxButton,
  type ButtonVariant as AstryxButtonVariant,
} from '@astryxdesign/core/Button';
import { CheckboxInput } from '@astryxdesign/core/CheckboxInput';
import { CodeBlock } from '@astryxdesign/core/CodeBlock';
import { EmptyState as AstryxEmptyState } from '@astryxdesign/core/EmptyState';
import { Heading as AstryxHeading } from '@astryxdesign/core/Heading';
import {
  MetadataList as AstryxMetadataList,
  MetadataListItem,
} from '@astryxdesign/core/MetadataList';
import { Section as AstryxSection } from '@astryxdesign/core/Section';
import { Selector } from '@astryxdesign/core/Selector';
import { Spinner as AstryxSpinner } from '@astryxdesign/core/Spinner';
import { Stack as AstryxStack } from '@astryxdesign/core/Stack';
import {
  Table as AstryxTable,
  type TableColumn as AstryxTableColumn,
  proportional,
} from '@astryxdesign/core/Table';
import { Text as AstryxText } from '@astryxdesign/core/Text';
import { TextArea } from '@astryxdesign/core/TextArea';
import { TextInput } from '@astryxdesign/core/TextInput';
import { Tooltip as AstryxTooltip } from '@astryxdesign/core/Tooltip';

import type { ApprovalRecord } from '@flowsafe/approval-api/types';
import type {
  ApprovalUIComponents,
  ButtonVariant,
  StackGap,
  Tone,
} from '@flowsafe/approval-ui/components';

const BADGE_VARIANT: Record<Tone, BadgeVariant> = {
  neutral: 'neutral',
  info: 'info',
  success: 'success',
  warning: 'warning',
  danger: 'error',
};

const BANNER_STATUS: Record<Tone, BannerStatus> = {
  neutral: 'info',
  info: 'info',
  success: 'success',
  warning: 'warning',
  danger: 'error',
};

const BUTTON_VARIANT: Record<ButtonVariant, AstryxButtonVariant> = {
  primary: 'primary',
  secondary: 'secondary',
  danger: 'destructive',
  ghost: 'ghost',
};

// 2/3/4 are valid Astryx SpacingStep values (imported implicitly via the prop).
const GAP: Record<StackGap, 2 | 3 | 4> = { sm: 2, md: 3, lg: 4 };

// Astryx's Table requires `T extends Record<string, unknown>`; ApprovalRecord is
// an interface without an index signature. The cast lives here in the adapter
// (not the library), and Table only reads rows.
type AstryxRow = ApprovalRecord & Record<string, unknown>;

export const astryxComponents: ApprovalUIComponents = {
  Stack: ({ direction = 'vertical', gap = 'md', children }) => (
    // Horizontal rows must reflow: the library's FilterBar puts six fields +
    // two buttons in one Stack, which unwrapped overflows past the page's
    // overflow-x clip and leaves the tail controls unreachable.
    <AstryxStack
      direction={direction}
      gap={GAP[gap]}
      wrap={direction === 'horizontal' ? 'wrap' : undefined}
    >
      {children}
    </AstryxStack>
  ),
  Section: ({ 'aria-label': ariaLabel, children }) => (
    <AstryxSection variant="section" padding={4} aria-label={ariaLabel}>
      {children}
    </AstryxSection>
  ),
  Heading: ({ level, children }) => (
    <AstryxHeading level={level}>{children}</AstryxHeading>
  ),
  Text: ({ children }) => <AstryxText>{children}</AstryxText>,
  Button: ({
    label,
    onClick,
    disabled,
    variant = 'secondary',
    type,
    pressed,
  }) => (
    <AstryxButton
      label={label}
      variant={BUTTON_VARIANT[variant]}
      isDisabled={disabled}
      type={type}
      onClick={onClick}
      aria-pressed={pressed}
    />
  ),
  Badge: ({ label, tone = 'neutral' }) => (
    <AstryxBadge variant={BADGE_VARIANT[tone]} label={label} />
  ),
  Banner: ({ tone, title }) => (
    <AstryxBanner status={BANNER_STATUS[tone]} title={title} />
  ),
  TextField: ({
    label,
    value,
    onChange,
    placeholder,
    disabled,
    rows,
    onSubmit,
  }) =>
    rows !== undefined ? (
      <TextArea
        label={label}
        value={value}
        onChange={(next) => onChange(next)}
        rows={rows}
        isDisabled={disabled}
        placeholder={placeholder}
      />
    ) : (
      <TextInput
        label={label}
        value={value}
        onChange={(next) => onChange(next)}
        placeholder={placeholder}
        isDisabled={disabled}
        onEnter={onSubmit}
      />
    ),
  // Label hidden visually (a11y-only, the SelectableCard/StatusDot pattern):
  // in a queue row the record title next to the box would be duplicate noise.
  Checkbox: ({ label, checked, onChange, disabled }) => (
    <CheckboxInput
      label={label}
      isLabelHidden
      value={checked}
      onChange={(next) => onChange(next)}
      isDisabled={disabled}
    />
  ),
  Select: ({ label, value, options, onChange, disabled }) => (
    <Selector
      label={label}
      value={value}
      options={options.map((option) => ({
        value: option.value,
        label: option.label,
      }))}
      onChange={(next) => onChange(next)}
      isDisabled={disabled}
    />
  ),
  MetadataList: ({ children }) => (
    <AstryxMetadataList columns="multi">{children}</AstryxMetadataList>
  ),
  MetadataItem: ({ label, children }) => (
    <MetadataListItem label={label}>{children}</MetadataListItem>
  ),
  Code: ({ code, language }) => (
    <CodeBlock code={code} language={language} isWrapped hasCopyButton />
  ),
  EmptyState: ({ title, description }) => (
    <AstryxEmptyState title={title} description={description} />
  ),
  Spinner: ({ label }) => <AstryxSpinner label={label} />,
  // Real hover/focus tooltip (vs the HTML default's title attribute). A dotted
  // underline marks the term as explorable without inventing a new component.
  InfoTip: ({ label, tip }) => (
    <AstryxTooltip content={tip}>
      <span
        style={{
          textDecorationLine: 'underline',
          textDecorationStyle: 'dotted',
          textUnderlineOffset: '3px',
          cursor: 'help',
        }}
      >
        {label}
      </span>
    </AstryxTooltip>
  ),
  Table: ({ data, columns, idKey, emptyState, 'aria-label': ariaLabel }) => {
    const astryxColumns: AstryxTableColumn<AstryxRow>[] = columns.map(
      (column) => ({
        key: column.key,
        header: column.header,
        width:
          column.key === 'title' || column.key === 'run'
            ? proportional(2)
            : proportional(1),
        renderCell: (row) => column.renderCell(row),
      }),
    );
    // Astryx's Table puts aria-* on a role-less wrapper div (ignored by AT), so
    // name the queue with a native <section> landmark instead.
    return (
      <section aria-label={ariaLabel}>
        <AstryxTable
          data={data as unknown as AstryxRow[]}
          columns={astryxColumns}
          idKey={idKey}
          hasHover
          emptyState={emptyState}
        />
      </section>
    );
  },
};
