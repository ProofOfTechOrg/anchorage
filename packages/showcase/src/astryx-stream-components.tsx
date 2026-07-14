// Astryx adapters for approval-ui's live-streaming slots (Toast +
// PresenceIndicator), the M-007 counterparts of the core astryx-components.tsx
// map. Injected through the SAME ApprovalUIProvider override, so the library
// views render live surfaces in the Astryx look while published approval-ui
// consumers still pull zero Astryx. Kept in its own module (per M-008) so the
// streaming slots stay separable from the base adapter.

import { Avatar } from '@astryxdesign/core/Avatar';
import { AvatarGroup } from '@astryxdesign/core/AvatarGroup';
import { Banner, type BannerStatus } from '@astryxdesign/core/Banner';
import { HStack } from '@astryxdesign/core/HStack';
import { Text } from '@astryxdesign/core/Text';

import type {
  ApprovalUIComponents,
  Tone,
} from '@flowsafe/approval-ui/components';

const TOAST_STATUS: Record<Tone, BannerStatus> = {
  neutral: 'info',
  info: 'info',
  success: 'success',
  warning: 'warning',
  danger: 'error',
};

// Cap the avatar row so a busy tenant does not overflow the header; the count
// text always states the true total.
const MAX_PRESENCE_AVATARS = 5;

export const astryxStreamComponents: Required<
  Pick<ApprovalUIComponents, 'Toast' | 'PresenceIndicator'>
> = {
  // A live surface (a decision conflict, a transient stream event): an Astryx
  // Banner rendered INLINE — this is a render slot, not the imperative toast
  // viewport — dismissable when onDismiss is supplied.
  Toast: ({ tone, title, onDismiss }) => (
    <Banner
      status={TOAST_STATUS[tone]}
      title={title}
      isDismissable={onDismiss !== undefined}
      onDismiss={onDismiss}
    />
  ),
  // Reviewers currently on the tenant's live stream, as overlapping avatars
  // (initials from the actor id) plus a count. a11y: each avatar's alt names its
  // actor and role (Avatar surfaces alt on hover and to screen readers).
  PresenceIndicator: ({ members }) => {
    if (members.length === 0) return null;
    const shown = members.slice(0, MAX_PRESENCE_AVATARS);
    return (
      <HStack gap={2} align="center" wrap="wrap">
        <AvatarGroup size="small">
          {shown.map((member) => (
            <Avatar
              key={`${member.actorId}:${member.role}`}
              name={member.actorId}
              alt={`${member.actorId}, ${member.role}`}
              size="small"
            />
          ))}
        </AvatarGroup>
        <Text size="sm" color="secondary">
          {members.length === 1
            ? '1 reviewer online'
            : `${members.length} reviewers online`}
        </Text>
      </HStack>
    );
  },
};
