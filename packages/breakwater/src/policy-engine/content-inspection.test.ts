// SPDX-License-Identifier: Apache-2.0
import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import { MessageList } from '@mastra/core/agent/message-list';
import type {
  OutputResult,
  ProcessOutputResultArgs,
  ProcessOutputStreamArgs,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { ChunkFrom, type ChunkType } from '@mastra/core/stream';
import { describe, expect, it } from 'vitest';

import { AuditLogger } from '../audit/index.js';
import { HIGH_ENTROPY_CANDIDATE_RE } from './content-inspection.js';
import {
  type ClassifierPolicyOptions,
  classifierPolicy,
  type PiiSecretsDetectorId,
  type PiiSecretsOptions,
  type PolicyContext,
  PolicyEngine,
  piiSecrets,
  policyDenialReason,
} from './index.js';
import type { PolicyDecision } from './tool-policy.js';

class Tripwire extends Error {}

// A real array whose own container methods and iterator answer with
// `answer`, never with its indexed entries.
function answeringList<T>(entries: readonly T[], answer: readonly unknown[]) {
  return Object.assign([...entries], {
    map: () => [...answer],
    some: () => answer.length > 0,
    every: () => true,
    includes: () => answer.length > 0,
    [Symbol.iterator]: function* () {
      yield* answer;
    },
  }) as T[];
}

function abortThrowing(reason?: string): never {
  throw new Tripwire(reason ?? 'aborted');
}

function context(overrides: Partial<PolicyContext> = {}): PolicyContext {
  return {
    phase: 'output',
    channel: 'answer',
    messages: [],
    text: '',
    ...overrides,
  };
}

function textDelta(text: string, id = 'out'): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'text-delta',
    payload: { id, text },
  };
}

function reasoningDelta(text: string): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'reasoning-delta',
    payload: { id: 'reason', text },
  };
}

// 'object' chunks carry the parsed value on `.object`, not `.payload` —
// mirrors policy-engine.test.ts's helper.
function objectChunk(partial: unknown): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'object',
    object: partial,
  } as unknown as ChunkType;
}

function objectResult(object: unknown): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'object-result',
    object,
  } as unknown as ChunkType;
}

function makeStreamArgs(
  streamParts: ChunkType[],
  state: Record<string, unknown> = {},
): ProcessOutputStreamArgs {
  const part = streamParts.at(-1);
  if (!part) throw new Error('makeStreamArgs needs at least one chunk');
  return {
    part,
    streamParts,
    state,
    retryCount: 0,
    requestContext: new RequestContext(),
    abort: abortThrowing,
  };
}

function textOf(chunk: ChunkType | null | undefined): string {
  const text = (chunk as { payload?: { text?: unknown } } | null | undefined)
    ?.payload?.text;
  return typeof text === 'string' ? text : '';
}

let messageSeq = 0;

function makeMessage(text: string): MastraDBMessage {
  return {
    id: `msg-${++messageSeq}`,
    role: 'assistant',
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
}

function makeOutputArgs(resultText: string): ProcessOutputResultArgs {
  const result: OutputResult = {
    text: resultText,
    usage: {} as OutputResult['usage'],
    finishReason: 'stop',
    steps: [],
  };
  return {
    messages: [makeMessage(resultText)],
    messageList: new MessageList(),
    state: {},
    retryCount: 0,
    requestContext: new RequestContext(),
    abort: abortThrowing,
    result,
  };
}

describe('piiSecrets', () => {
  describe('email detector', () => {
    it('denies text containing an email address', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['email'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'contact test@example.com for help' }),
      );

      // #then
      expect(decision).toMatchObject({
        allowed: false,
        reason: expect.stringContaining('email'),
      });
    });

    it('allows text with no email address', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['email'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'no email here' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });
  });

  describe('ssn detector', () => {
    it('denies text containing an SSN', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['ssn'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'ssn is 123-45-6789' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('allows digits that are not SSN-shaped', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['ssn'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'order number 12345' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });
  });

  describe('phone detector', () => {
    it('denies an international-format phone number', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['phone'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'call +14155552671 now' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('denies a local-format phone number', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['phone'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'call 415-555-2671 now' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('allows text with no phone number', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['phone'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'call me sometime, ok?' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });
  });

  describe('creditCard detector (Luhn)', () => {
    it('denies a Luhn-valid card number', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['creditCard'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'card 4111111111111111 on file' }),
      );

      // #then
      expect(decision).toMatchObject({
        allowed: false,
        reason: expect.stringContaining('creditCard'),
      });
    });

    it('allows a card number with an invalid Luhn check digit', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['creditCard'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'card 4111111111111112 on file' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });

    it('handles dash separators in a Luhn-valid card number', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['creditCard'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'card 4111-1111-1111-1111 on file' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('handles space separators in a Luhn-valid card number', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['creditCard'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'card 4111 1111 1111 1111 on file' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });
  });

  describe('awsAccessKey detector', () => {
    it('denies a well-formed AWS access key', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['awsAccessKey'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'key is AKIAIOSFODNN7EXAMPLE in the config' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('allows an AKIA-prefixed string that is too short', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['awsAccessKey'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'key is AKIA1234 in the config' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });
  });

  describe('privateKey detector', () => {
    it('denies a PEM private key header', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['privateKey'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: '-----BEGIN RSA PRIVATE KEY-----' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('allows an unrelated PEM header', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['privateKey'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: '-----BEGIN CERTIFICATE-----' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });
  });

  describe('jwt detector', () => {
    const jwt =
      'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U';

    it('denies a JWT-shaped token', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['jwt'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: `auth header carries ${jwt} for the session` }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('allows text with no JWT-shaped token', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['jwt'] });

      // #when
      const decision = await evaluator.evaluate(context({ text: 'not.a.jwt' }));

      // #then
      expect(decision).toEqual({ allowed: true });
    });
  });

  describe('secretAssignment detector', () => {
    it('denies a key: value-shaped assignment', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['secretAssignment'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'apiKey: "abcdefghijklmnopqrstuvwxy12"' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('allows prose that merely mentions the keyword', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['secretAssignment'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'just talking about api keys in general' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });
  });

  describe('highEntropy detector', () => {
    // Verified via direct Shannon-entropy computation: ~4.954 bits/char.
    const randomToken = 'aB3xQ9mK7pL2vN8fR4tY6wZ1cX5jH0e';
    // Same shape gate (digit + mixed case) but ~4.314 bits/char — below the
    // default 4.5 threshold, so this exercises the entropy math itself, not
    // just the digit/case pre-filter.
    const englishSentence =
      'ThisIsALongEnglishSentenceAboutNothingInParticular1';

    it('denies a high-entropy token', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['highEntropy'] });

      // #when
      const decision = await evaluator.evaluate(context({ text: randomToken }));

      // #then
      expect(decision).toMatchObject({
        allowed: false,
        reason: expect.stringContaining('highEntropy'),
      });
    });

    it('allows a long, low-entropy English string', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['highEntropy'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: englishSentence }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });

    it('respects an entropyThreshold override', async () => {
      // #given — raising the bar above the token's own ~4.954 flips it to allowed
      const evaluator = piiSecrets({
        detectors: ['highEntropy'],
        entropyThreshold: 5.5,
      });

      // #when
      const decision = await evaluator.evaluate(context({ text: randomToken }));

      // #then
      expect(decision).toEqual({ allowed: true });
    });

    it('does not match a 20-char candidate — below the length floor where the 4.5-bit threshold is first reachable', () => {
      // #given — a 20-char base64-class run. Shannon entropy ceilings at
      // log2(20) ~= 4.32 < the 4.5-bit default, so a length-20 candidate can
      // never reach the threshold; the {23} floor drops it as a candidate.
      const twentyCharCandidate = 'aB3xQ9mK7pL2vN8fR4tY';

      // #when / #then — white-box: the candidate regex finds no match
      expect(twentyCharCandidate.match(HIGH_ENTROPY_CANDIDATE_RE)).toBeNull();
    });

    it('still flags a maximally-random length-23 candidate (byte-identical length-23+ behavior)', async () => {
      // #given — 23 DISTINCT base64-class chars => Shannon entropy = log2(23)
      // ~= 4.52 >= 4.5, and the shape gate passes (digit + mixed case): the
      // shortest length at which the detector can fire.
      const evaluator = piiSecrets({ detectors: ['highEntropy'] });
      const twentyThreeCharCandidate = 'aBcDeFgHiJkLmNoPqRsTuV7';

      // #when
      const decision = await evaluator.evaluate(
        context({ text: twentyThreeCharCandidate }),
      );

      // #then
      expect(decision).toMatchObject({
        allowed: false,
        reason: expect.stringContaining('highEntropy'),
      });
    });

    it('extracts and flags a 20-char maximally-random candidate when entropyThreshold is lowered to 4.0', async () => {
      // #given — at a 4.0-bit bar the candidate-length floor derives to
      // max(20, ceil(2^4.0)=16) = 20, so 20..22-char candidates are extracted
      // by this threshold. A hardcoded {23} floor misses this candidate.
      // This 20-char run is all-
      // distinct base64-class chars, so its Shannon entropy is exactly
      // log2(20) ~= 4.32 >= 4.0 (not flaky), and the shape gate passes (digits
      // + mixed case).
      const evaluator = piiSecrets({
        detectors: ['highEntropy'],
        entropyThreshold: 4.0,
      });
      const twentyCharCandidate = 'aB3xQ9mK7pL2vN8fR4tY';

      // #when
      const decision = await evaluator.evaluate(
        context({ text: twentyCharCandidate }),
      );

      // #then
      expect(decision).toMatchObject({
        allowed: false,
        reason: expect.stringContaining('highEntropy'),
      });
    });

    // Every character of the candidate class once: the highest entropy a
    // candidate can reach.
    const everyCandidateCharacter =
      'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/_=-';
    // The entropy the detector computes for that candidate. log2(67) rounds a
    // few units in the last place above it.
    const maxEntropy = 6.066089190457767;

    it.each<[string, unknown, string]>([
      ["the string 'high'", 'high', '"high"'],
      ["the string '4.5', without coercing it", '4.5', '"4.5"'],
      ['a plain object', {}, 'object'],
      ['NaN', Number.NaN, 'NaN'],
      ['Infinity', Number.POSITIVE_INFINITY, 'Infinity'],
      [
        'log2(67), which the computed entropy never reaches',
        Math.log2(67),
        String(Math.log2(67)),
      ],
      ['a value just above log2(67)', 6.0661, '6.0661'],
      ['7', 7, '7'],
      ['0', 0, '0'],
      ['a negative number', -1, '-1'],
      ['false', false, 'boolean'],
    ])('refuses %s as entropyThreshold', (_label, entropyThreshold, got) => {
      // #when / #then — above the maximum the detector never fires
      expect(() =>
        piiSecrets({ entropyThreshold: entropyThreshold as number }),
      ).toThrow(
        new TypeError(
          `piiSecrets: entropyThreshold must be a number greater than 0 and at most ${maxEntropy} (got ${got})`,
        ),
      );
    });

    it('detects the every-character candidate at the maximum accepted threshold', async () => {
      // #given
      const evaluator = piiSecrets({
        detectors: ['highEntropy'],
        entropyThreshold: maxEntropy,
      });
      // #when
      const decision = await evaluator.evaluate(
        context({ text: `token ${everyCandidateCharacter} end` }),
      );
      // #then
      expect(decision).toMatchObject({
        allowed: false,
        reason: expect.stringContaining('highEntropy'),
      });
    });

    it('detects the every-character candidate at a threshold near the maximum', async () => {
      // #given
      const evaluator = piiSecrets({
        detectors: ['highEntropy'],
        entropyThreshold: 6,
      });
      // #when
      const decision = await evaluator.evaluate(
        context({ text: everyCandidateCharacter }),
      );
      // #then
      expect(decision).toMatchObject({
        allowed: false,
        reason: expect.stringContaining('highEntropy'),
      });
    });
  });

  describe('allowlist', () => {
    it('exempts a match whose text equals an allowlist string, case-insensitively', async () => {
      // #given
      const evaluator = piiSecrets({
        detectors: ['email'],
        allowlist: ['SUPPORT@EXAMPLE.COM'],
      });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'reach us at support@example.com' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });

    it('still denies a different match while an allowlist entry exempts only its own text', async () => {
      // #given
      const evaluator = piiSecrets({
        detectors: ['email'],
        allowlist: ['support@example.com'],
      });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'reach us at other@example.com' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('exempts a match against an allowlist RegExp', async () => {
      // #given
      const evaluator = piiSecrets({
        detectors: ['email'],
        allowlist: [/^support@/],
      });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'reach us at support@example.com' }),
      );

      // #then
      expect(decision).toEqual({ allowed: true });
    });

    it('does not let a g-flagged allowlist RegExp misbehave across repeated calls', async () => {
      // #given — a naive reuse of a caller's g-flagged RegExp would mutate
      // lastIndex on the first .test() and desync the second call
      const evaluator = piiSecrets({
        detectors: ['email'],
        allowlist: [/^support@/g],
      });
      const input = context({ text: 'reach us at support@example.com' });

      // #when
      const first = await evaluator.evaluate(input);
      const second = await evaluator.evaluate(input);

      // #then
      expect(first).toEqual({ allowed: true });
      expect(second).toEqual({ allowed: true });
    });

    it.each<[string, unknown, string]>([
      ['an object with its own test', { test: () => true }, 'object'],
      [
        'an object with its own exec',
        { exec: () => Object.assign([''], { index: 0, input: '' }) },
        'object',
      ],
      [
        'a RegExp-tagged object',
        {
          [Symbol.toStringTag]: 'RegExp',
          test: () => true,
          source: '.',
          flags: '',
        },
        'object',
      ],
      [
        'an object whose prototype is RegExp.prototype',
        Object.create(RegExp.prototype),
        'object',
      ],
      [
        'RegExp.prototype, whose copy matches everything',
        RegExp.prototype,
        'object',
      ],
      ['a String object', new String('support@example.com'), 'object'],
      ['null', null, 'null'],
    ])('refuses %s as an allowlist entry', (_label, entry, got) => {
      // #when / #then — an allowlist entry that answers true exempts every match
      expect(() =>
        piiSecrets({
          detectors: ['email'],
          allowlist: ['support@example.com', entry as string],
        }),
      ).toThrow(
        new TypeError(
          `piiSecrets: allowlist entry 1 must be a string or a RegExp (got ${got})`,
        ),
      );
    });

    it('refuses an allowlist that is not an array', () => {
      // #when / #then
      expect(() =>
        piiSecrets({
          detectors: ['email'],
          allowlist: 'support@example.com' as unknown as string[],
        }),
      ).toThrow(new TypeError('piiSecrets: allowlist must be an array'));
    });

    it.each<[string, () => RegExp]>([
      ['its own test', () => Object.assign(/^support@/, { test: () => true })],
      [
        'its own exec',
        () =>
          Object.assign(/^support@/, {
            exec: () => Object.assign([''], { index: 0, input: '' }),
          }),
      ],
    ])('exempts by the RegExp it copied, not by %s', async (_label, entry) => {
      // #given
      const evaluator = piiSecrets({
        detectors: ['email'],
        allowlist: [entry()],
      });
      // #when / #then — only the copied pattern decides the exemption
      expect(
        await evaluator.evaluate(context({ text: 'mail other@example.com' })),
      ).toMatchObject({ allowed: false });
    });

    it('keeps its copy when test is assigned to the caller RegExp after construction', async () => {
      // #given
      const entry = /^support@/;
      const evaluator = piiSecrets({
        detectors: ['email'],
        allowlist: [entry],
      });
      entry.test = () => true;
      // #when / #then
      expect(
        await evaluator.evaluate(context({ text: 'mail other@example.com' })),
      ).toMatchObject({ allowed: false });
    });

    it('exempts from the entries it read by index, not the list its own map answers', async () => {
      // #given — own map answers with an entry that exempts everything
      const allowlist = answeringList(['support@example.com'], [/./]);
      const evaluator = piiSecrets({ detectors: ['email'], allowlist });
      // #when / #then
      expect(
        await evaluator.evaluate(context({ text: 'mail other@example.com' })),
      ).toMatchObject({ allowed: false });
    });
  });

  describe('detectors', () => {
    it.each<[string, unknown, string]>([
      ['an empty list', [], 'piiSecrets: detectors must not be empty'],
      ['a string', 'email', 'piiSecrets: detectors must be an array'],
      [
        "the inherited name 'constructor'",
        ['constructor'],
        'piiSecrets: detectors entry 0 must be a PII_SECRETS_DETECTOR_IDS member (got "constructor")',
      ],
      [
        'an unknown id',
        ['email', 'Email'],
        'piiSecrets: detectors entry 1 must be a PII_SECRETS_DETECTOR_IDS member (got "Email")',
      ],
      [
        'a String object',
        [new String('email')],
        'piiSecrets: detectors entry 0 must be a PII_SECRETS_DETECTOR_IDS member (got object)',
      ],
    ])('refuses %s', (_label, detectors, message) => {
      // #when / #then — a detector list that selects nothing never denies
      expect(() =>
        piiSecrets({ detectors: detectors as PiiSecretsDetectorId[] }),
      ).toThrow(new TypeError(message));
    });

    it('runs the detectors it read by index, not the list its own map answers', async () => {
      // #given — own map and iterator answer with no detector
      const detectors = answeringList<PiiSecretsDetectorId>(['email'], []);
      const evaluator = piiSecrets({ detectors });
      // #when / #then
      expect(
        await evaluator.evaluate(context({ text: 'mail other@example.com' })),
      ).toMatchObject({ allowed: false });
    });
  });

  describe('reason format', () => {
    it('names the detector id and match index, never the matched secret text', async () => {
      // #given
      const evaluator = piiSecrets({ detectors: ['ssn'] });

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'my ssn is 123-45-6789, ok' }),
      );

      // #then
      expect(decision.allowed).toBe(false);
      const reason = decision.allowed === false ? decision.reason : '';
      expect(reason).toContain('ssn');
      expect(reason).toMatch(/match index \d+/);
      expect(reason).not.toContain('123-45-6789');
    });
  });

  describe('defaults', () => {
    it('gates all three output channels by default', () => {
      // #given / #when / #then
      expect(piiSecrets().channels).toEqual(['answer', 'reasoning', 'object']);
    });

    it('enables email detection by default', async () => {
      // #given
      const evaluator = piiSecrets();

      // #when
      const decision = await evaluator.evaluate(
        context({ text: 'contact test@example.com please' }),
      );

      // #then
      expect(decision).toMatchObject({ allowed: false });
    });

    it('computes holdBackChars as maxEnabledSpan - 1 for the enabled detector set', () => {
      // #given / #when / #then — ssn's own maxSpan is 11
      expect(piiSecrets({ detectors: ['ssn'] }).holdBackChars).toBe(10);
    });

    it('honors an explicit holdBackChars override', () => {
      // #given / #when / #then
      expect(
        piiSecrets({ detectors: ['ssn'], holdBackChars: 999 }).holdBackChars,
      ).toBe(999);
    });

    it.each<[string, unknown, string]>([
      ['an empty string', '', '""'],
      ['an empty list', [], 'object'],
      ['false', false, 'boolean'],
      ['a negative number', -1, '-1'],
    ])('refuses %s as holdBackChars, which would release text unseen', (_label, holdBackChars, got) => {
      // #when / #then
      expect(() =>
        piiSecrets({ holdBackChars: holdBackChars as number }),
      ).toThrow(
        new TypeError(
          `piiSecrets: holdBackChars must be a number of at least 0, or Infinity (got ${got})`,
        ),
      );
    });
  });

  describe('options', () => {
    it.each<[string, unknown]>([
      ['entropythreshold', 4],
      ['channel', ['reasoning']],
    ])('refuses the misspelled option %s', (key, value) => {
      // #when / #then — a misspelled option would fall to its default
      expect(() => piiSecrets({ [key]: value } as PiiSecretsOptions)).toThrow(
        new TypeError(
          `piiSecrets: options has unknown field ${JSON.stringify(key)} (valid fields: name, detectors, allowlist, entropyThreshold, phases, channels, holdBackChars)`,
        ),
      );
    });

    it('constructs with every declared option', () => {
      // #given — the Required type fails to compile while a declared option is
      // missing here
      const options: Required<PiiSecretsOptions> = {
        name: 'pii',
        detectors: ['highEntropy'],
        allowlist: ['build2024ReleaseCandidate7x'],
        entropyThreshold: 4,
        phases: ['output'],
        channels: ['answer'],
        holdBackChars: 300,
      };
      // #when / #then
      expect(piiSecrets(options)).toMatchObject({
        name: 'pii',
        holdBackChars: 300,
      });
    });
  });

  describe('streaming (via a real PolicyEngine)', () => {
    it('denies a spaced card that completes after the rescan window start', async () => {
      // #given — the second delta's window starts at the card's first digit.
      const policy = piiSecrets({ detectors: ['creditCard'] });
      const maxEnabledSpan = (policy.holdBackChars ?? 0) + 1;
      const engine = new PolicyEngine({ policies: [policy] });
      const state: Record<string, unknown> = {};
      const card = '4000000000000000006'.split('').join(' ');
      const text = '9 '.repeat(19) + card;
      const first = textDelta(
        text.slice(0, text.length - card.length + maxEnabledSpan - 1),
      );
      const second = textDelta('6');

      // #when
      await expect(
        engine.processOutputStream(makeStreamArgs([first], state)),
      ).resolves.toStrictEqual(first);

      // #then
      await expect(
        engine.processOutputStream(makeStreamArgs([first, second], state)),
      ).rejects.toThrowError(policyDenialReason('pii-secrets', 'output'));
    });

    it('denies a high-entropy token that starts at the rescan window start', async () => {
      // #given — the second delta's window starts at the digit after the A run.
      const policy = piiSecrets({ detectors: ['highEntropy'] });
      const maxEnabledSpan = (policy.holdBackChars ?? 0) + 1;
      const engine = new PolicyEngine({ policies: [policy] });
      const state: Record<string, unknown> = {};
      const secret =
        '0' +
        'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.repeat(9) +
        'ABCDEFGHIJKLMNOPQRST' +
        'a';
      const first = textDelta(
        'A'.repeat(maxEnabledSpan) + secret.slice(0, maxEnabledSpan - 1),
      );
      const second = textDelta(secret.slice(maxEnabledSpan - 1));

      // #when
      await expect(
        engine.processOutputStream(makeStreamArgs([first], state)),
      ).resolves.toStrictEqual(first);

      // #then
      await expect(
        engine.processOutputStream(makeStreamArgs([first, second], state)),
      ).rejects.toThrowError(policyDenialReason('pii-secrets', 'output'));
    });

    it('does not deny an SSN the rescan window cuts from a longer word when ssn is the only detector', async () => {
      // #given — the second delta's window starts right after the leading a.
      const policy = piiSecrets({ detectors: ['ssn'] });
      const maxEnabledSpan = (policy.holdBackChars ?? 0) + 1;
      const engine = new PolicyEngine({ policies: [policy] });
      const state: Record<string, unknown> = {};
      const text = 'a123-45-6789 ok';
      const first = textDelta(text.slice(0, maxEnabledSpan));
      const second = textDelta(text.slice(maxEnabledSpan));

      // #when
      await expect(
        engine.processOutputStream(makeStreamArgs([first], state)),
      ).resolves.toStrictEqual(first);

      // #then
      await expect(
        engine.processOutputStream(makeStreamArgs([first, second], state)),
      ).resolves.toStrictEqual(second);
    });

    it('does not deny an AWS access key the rescan window cuts from a longer word when awsAccessKey is the only detector', async () => {
      // #given — the second delta's window starts right after the leading x.
      const policy = piiSecrets({ detectors: ['awsAccessKey'] });
      const maxEnabledSpan = (policy.holdBackChars ?? 0) + 1;
      const engine = new PolicyEngine({ policies: [policy] });
      const state: Record<string, unknown> = {};
      const text = `xAKIA${'A'.repeat(16)} ok`;
      const first = textDelta(text.slice(0, maxEnabledSpan));
      const second = textDelta(text.slice(maxEnabledSpan));

      // #when
      await expect(
        engine.processOutputStream(makeStreamArgs([first], state)),
      ).resolves.toStrictEqual(first);

      // #then
      await expect(
        engine.processOutputStream(makeStreamArgs([first, second], state)),
      ).resolves.toStrictEqual(second);
    });

    it('denies a cut SSN when another detector is enabled', async () => {
      // #given — the second delta's window starts right after the V.
      const policy = piiSecrets();
      const maxEnabledSpan = (policy.holdBackChars ?? 0) + 1;
      const engine = new PolicyEngine({ policies: [policy] });
      const state: Record<string, unknown> = {};
      const first = textDelta('INV123-45-6789'.padEnd(3 + maxEnabledSpan - 1));
      const second = textDelta(' more');

      // #when
      await expect(
        engine.processOutputStream(makeStreamArgs([first], state)),
      ).resolves.toStrictEqual(first);

      // #then
      await expect(
        engine.processOutputStream(makeStreamArgs([first, second], state)),
      ).rejects.toThrowError(policyDenialReason('pii-secrets', 'output'));
    });

    it('catches a secret split across 1-char stream chunks on the completing chunk', async () => {
      // #given — ssn only, so maxEnabledSpan=11 keeps the rescan window
      // narrow enough to meaningfully exercise the windowing arithmetic; the
      // SSN sits at the very end so its final digit is also the stream's
      // final char. The filler is a non-word char ('.') rather than a
      // letter/digit — a word-char filler abutting the SSN's leading digit
      // would erase the \b the pattern requires there.
      const engine = new PolicyEngine({
        policies: [piiSecrets({ detectors: ['ssn'] })],
      });
      const state: Record<string, unknown> = {};
      const fullText = '.....123-45-6789';
      const parts: ChunkType[] = [];

      // #when — every char but the last passes...
      for (let i = 0; i < fullText.length - 1; i++) {
        parts.push(textDelta(fullText[i] ?? ''));
        await engine.processOutputStream(makeStreamArgs([...parts], state));
      }

      // #then — the chunk completing the SSN's final digit aborts
      parts.push(textDelta(fullText[fullText.length - 1] ?? ''));
      await expect(
        engine.processOutputStream(makeStreamArgs([...parts], state)),
      ).rejects.toThrowError(policyDenialReason('pii-secrets', 'output'));
    });

    it('rescans the full object-channel snapshot on every call (never incremental)', async () => {
      // #given
      const engine = new PolicyEngine({
        policies: [piiSecrets({ detectors: ['email'] })],
      });
      const state: Record<string, unknown> = {};
      const clean = objectChunk({ note: 'nothing sensitive here' });
      const withEmail = objectResult({ note: 'contact leak@example.com now' });

      // #when — the clean partial passes...
      await expect(
        engine.processOutputStream(makeStreamArgs([clean], state)),
      ).resolves.toStrictEqual(clean);

      // #then — the REPLACEMENT snapshot (not a delta) is fully rescanned
      await expect(
        engine.processOutputStream(makeStreamArgs([withEmail], state)),
      ).rejects.toThrowError(policyDenialReason('pii-secrets', 'output'));
    });

    it('emits no char of a violating span when holdBack is on', async () => {
      // #given — ssn only (maxSpan 11 -> holdBackChars 10), holdBack on; a
      // 12-char non-word filler ('.') then the SSN, split across two
      // chunks. A word-char filler (e.g. 'x') abutting the SSN's leading
      // digit would erase the \b the pattern requires there.
      const engine = new PolicyEngine({
        policies: [piiSecrets({ detectors: ['ssn'] })],
        holdBack: true,
      });
      const state: Record<string, unknown> = {};
      const chunks = [textDelta('.'.repeat(12)), textDelta('123-45-6789')];
      const emitted: string[] = [];

      // #when — the first chunk (12 dots) evaluates clean and releases only
      // text outside the held window (holdBackChars=10, so 2 chars release)...
      emitted.push(
        textOf(
          await engine.processOutputStream(
            makeStreamArgs(chunks.slice(0, 1), state),
          ),
        ),
      );
      // ...the second chunk completes the SSN and aborts
      await expect(
        engine.processOutputStream(makeStreamArgs(chunks, state)),
      ).rejects.toThrowError(policyDenialReason('pii-secrets', 'output'));

      // #then — nothing emitted contains any char of the SSN span
      expect(emitted.join('')).toBe('..');
    });
  });
});

describe('classifierPolicy', () => {
  describe('defaults', () => {
    it('gates only the answer channel by default', () => {
      // #given / #when / #then
      expect(
        classifierPolicy({ classify: () => ({ allowed: true }) }).channels,
      ).toEqual(['answer']);
    });
  });

  describe('options', () => {
    const allow = () => ({ allowed: true }) as const;

    it('refuses a misspelled option, which would fall to the answer-only default', () => {
      // #when / #then
      expect(() =>
        classifierPolicy({
          classify: allow,
          channel: ['reasoning'],
        } as ClassifierPolicyOptions),
      ).toThrow(
        new TypeError(
          'classifierPolicy: options has unknown field "channel" (valid fields: name, classify, phases, channels, evaluateEveryChars, timeoutMs)',
        ),
      );
    });

    it('constructs with every declared option', () => {
      // #given — the Required type fails to compile while a declared option is
      // missing here
      const options: Required<ClassifierPolicyOptions> = {
        name: 'moderation',
        classify: allow,
        phases: ['output'],
        channels: ['answer', 'reasoning'],
        evaluateEveryChars: 64,
        timeoutMs: 1_000,
      };
      // #when / #then
      expect(classifierPolicy(options)).toMatchObject({
        name: 'moderation',
        channels: ['answer', 'reasoning'],
      });
    });

    it.each<[string, unknown, string]>([
      [
        'Infinity, which never classifies a stream',
        Number.POSITIVE_INFINITY,
        'Infinity',
      ],
      ['0', 0, '0'],
      ['a negative number', -1, '-1'],
      ['a fraction', 1.5, '1.5'],
      ['NaN', Number.NaN, 'NaN'],
      ['a numeric string', '512', '"512"'],
    ])('refuses %s as evaluateEveryChars', (_label, evaluateEveryChars, got) => {
      // #when / #then
      expect(() =>
        classifierPolicy({
          classify: allow,
          evaluateEveryChars: evaluateEveryChars as number,
        }),
      ).toThrow(
        new TypeError(
          `classifierPolicy: evaluateEveryChars must be a positive safe integer (got ${got})`,
        ),
      );
    });
  });

  describe('streaming cadence', () => {
    it('does not classify per chunk; classifies once the cadence threshold is crossed', async () => {
      // #given
      const calls: string[] = [];
      const classify = async (text: string): Promise<PolicyDecision> => {
        calls.push(text);
        return { allowed: true };
      };
      const engine = new PolicyEngine({
        policies: [classifierPolicy({ classify, evaluateEveryChars: 10 })],
      });
      const state: Record<string, unknown> = {};

      // #when — nine 1-char chunks (9 total chars, below the 10-char cadence)...
      for (const ch of 'abcdefghi') {
        await engine.processOutputStream(
          makeStreamArgs([textDelta(ch)], state),
        );
      }
      expect(calls).toHaveLength(0);

      // #when — the 10th char crosses the threshold...
      await engine.processOutputStream(makeStreamArgs([textDelta('j')], state));

      // #then — classified exactly once, with the full accumulated text
      expect(calls).toEqual(['abcdefghij']);
    });

    it('always classifies at the result phase, even below the streaming cadence', async () => {
      // #given — a cadence the short result text would never cross
      const calls: string[] = [];
      const classify = async (text: string): Promise<PolicyDecision> => {
        calls.push(text);
        return { allowed: true };
      };
      const engine = new PolicyEngine({
        policies: [classifierPolicy({ classify, evaluateEveryChars: 10_000 })],
      });

      // #when
      await engine.processOutputResult(makeOutputArgs('short'));

      // #then
      expect(calls).toEqual(['short']);
    });

    it('classifies every object-channel snapshot regardless of cadence', async () => {
      // #given
      const calls: string[] = [];
      const classify = async (text: string): Promise<PolicyDecision> => {
        calls.push(text);
        return { allowed: true };
      };
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify,
            channels: ['object'],
            evaluateEveryChars: 10_000,
          }),
        ],
        // The D1 guard: an object-only policy needs a sink to record a
        // fail-closed result-phase coverage error when no object is observed.
        audit: new AuditLogger(),
      });
      const state: Record<string, unknown> = {};

      // #when
      await engine.processOutputStream(
        makeStreamArgs([objectChunk({ a: 1 })], state),
      );
      await engine.processOutputStream(
        makeStreamArgs([objectResult({ a: 1, b: 2 })], state),
      );

      // #then — both snapshots classified despite a cadence neither would cross
      expect(calls).toHaveLength(2);
    });

    it('tracks the classification cadence independently per channel', async () => {
      // #given
      const calls: Array<{ channel: string; text: string }> = [];
      const classify = async (
        text: string,
        info: { channel: string },
      ): Promise<PolicyDecision> => {
        calls.push({ channel: info.channel, text });
        return { allowed: true };
      };
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify,
            evaluateEveryChars: 5,
            channels: ['answer', 'reasoning'],
          }),
        ],
      });
      const state: Record<string, unknown> = {};

      // #when — 4 chars on each channel: neither alone crosses the 5-char
      // cadence, proving the cursors are not summed together
      await engine.processOutputStream(
        makeStreamArgs([textDelta('abcd')], state),
      );
      await engine.processOutputStream(
        makeStreamArgs([reasoningDelta('wxyz')], state),
      );
      expect(calls).toHaveLength(0);

      // #then — one more char on 'answer' alone crosses its own cadence (the
      // channel accumulator keeps appending, so this delta lands on top of
      // the earlier 'abcd': total accumulated answer text is 'abcde')
      await engine.processOutputStream(makeStreamArgs([textDelta('e')], state));
      expect(calls).toEqual([{ channel: 'answer', text: 'abcde' }]);
    });
  });

  describe('deny aborts', () => {
    it('aborts the stream when classify denies', async () => {
      // #given
      const marker = 'flagged as unsafe';
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: async () => ({
              allowed: false,
              reason: marker,
            }),
            evaluateEveryChars: 1,
          }),
        ],
      });

      // #when / #then
      const failure = await engine
        .processOutputStream(makeStreamArgs([textDelta('x')]))
        .catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(Error);
      expect((failure as Error).message).toBe(
        policyDenialReason('classifier', 'output'),
      );
      expect((failure as Error).message).not.toContain(marker);
    });
  });

  describe('fail-closed', () => {
    it.each([
      [
        'synchronous',
        (message: string): PolicyDecision => {
          throw new Error(message);
        },
      ],
      [
        'asynchronous',
        async (message: string): Promise<PolicyDecision> => {
          throw new Error(message);
        },
      ],
    ] as const)('discards %s classifier exception details at the final result', async (_label, classify) => {
      // #given
      const sentinel = 'sk_live_sentinel';
      const message = 'classifier exploded';
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: () => classify(`${message} on ${sentinel}`),
          }),
        ],
      });

      // #when / #then
      const thrown = await engine
        .processOutputResult(makeOutputArgs('anything'))
        .catch((error: unknown) => error);
      expect(thrown).toBeInstanceOf(Error);
      const failure = thrown as Error;
      for (const text of [
        failure.message,
        String(failure),
        String(failure.cause),
      ]) {
        expect(text).not.toContain(sentinel);
        expect(text).not.toContain(message);
      }
      expect(failure.cause).toBeUndefined();
      expect(failure.message).toBe('policy evaluation failed');
    });

    it('fails closed when classify exceeds timeoutMs', async () => {
      // #given — never settles on its own
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: () => new Promise<PolicyDecision>(() => {}),
            timeoutMs: 20,
          }),
        ],
      });

      // #when / #then
      await expect(
        engine.processOutputResult(makeOutputArgs('anything')),
      ).rejects.toThrow('policy evaluation failed');
    });

    it('does not crash when a slow classify eventually rejects after its own timeout already fired', async () => {
      // #given
      let rejectClassify!: (reason: Error) => void;
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: () =>
              new Promise<PolicyDecision>((_resolve, reject) => {
                rejectClassify = reject;
              }),
            timeoutMs: 5,
          }),
        ],
      });

      // #when / #then
      await expect(
        engine.processOutputResult(makeOutputArgs('anything')),
      ).rejects.toThrow('policy evaluation failed');

      rejectClassify(new Error('late failure'));
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
  });
});

describe('content evaluator text', () => {
  const SECRET = 'contact john.doe@example.com now';

  it.each<[string, unknown, string]>([
    ['an undefined text', undefined, 'undefined'],
    ['an object text', { format: 2, parts: [SECRET] }, 'object'],
    ['a text list', [SECRET], 'object'],
  ])('piiSecrets refuses %s', async (_label, text, got) => {
    // #given
    const evaluator = piiSecrets();

    // #when / #then
    await expect(async () =>
      evaluator.evaluate(context({ text: text as string })),
    ).rejects.toThrow(
      new TypeError(`piiSecrets: text must be a string (got ${got})`),
    );
  });

  it('classifierPolicy refuses a non-string text before calling classify', async () => {
    // #given
    const seen: unknown[] = [];
    const evaluator = classifierPolicy({
      classify: (text) => {
        seen.push(text);
        return { allowed: true };
      },
    });

    // #when / #then
    await expect(async () =>
      evaluator.evaluate(context({ text: [SECRET] as unknown as string })),
    ).rejects.toThrow(
      new TypeError('classifierPolicy: text must be a string (got object)'),
    );
    expect(seen).toEqual([]);
  });

  it('still denies the string text', async () => {
    // #when / #then
    expect(
      await piiSecrets().evaluate(context({ text: SECRET })),
    ).toMatchObject({ allowed: false });
    expect(
      await classifierPolicy({
        classify: (text) =>
          text.includes('john.doe')
            ? { allowed: false, reason: 'pii' }
            : { allowed: true },
      }).evaluate(context({ text: SECRET })),
    ).toMatchObject({ allowed: false });
  });
});
