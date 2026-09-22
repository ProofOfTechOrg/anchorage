// SPDX-License-Identifier: Apache-2.0

import { z } from 'zod';

export const continuationChallenge = z
  .string()
  .length(64)
  .regex(/^[a-f0-9]{64}$/u);
export const continuationInputSchema = z
  .object({ challenge: continuationChallenge })
  .strict();
export const continuationResumeSchema = z
  .object({ proceed: z.literal(true) })
  .strict();
export const continuationSuspendSchema = z
  .object({ reason: z.literal('awaiting-resume') })
  .strict();
export const continuationOutputSchema = z
  .object({
    challenge: continuationChallenge,
    release: z.enum(['1', '2']),
  })
  .strict();
