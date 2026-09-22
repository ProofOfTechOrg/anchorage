// SPDX-License-Identifier: Apache-2.0

import type { z } from 'zod';

export const continuationChallenge: z.ZodString;
export const continuationInputSchema: z.ZodObject<{
  challenge: z.ZodString;
}>;
export const continuationResumeSchema: z.ZodObject<{
  proceed: z.ZodLiteral<true>;
}>;
export const continuationSuspendSchema: z.ZodObject<{
  reason: z.ZodLiteral<'awaiting-resume'>;
}>;
export const continuationOutputSchema: z.ZodObject<{
  challenge: z.ZodString;
  release: z.ZodEnum<{ 1: '1'; 2: '2' }>;
}>;
