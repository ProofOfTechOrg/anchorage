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
import {
  classifierPolicy,
  type PolicyContext,
  PolicyEngine,
  piiSecrets,
} from './index.js';
import type { PolicyDecision } from './tool-policy.js';

class Tripwire extends Error {}

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

    it('enables every detector by default', async () => {
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
  });

  describe('streaming (via a real PolicyEngine)', () => {
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
      ).rejects.toThrow(/ssn detected/);
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
      ).resolves.toBe(clean);

      // #then — the REPLACEMENT snapshot (not a delta) is fully rescanned
      await expect(
        engine.processOutputStream(makeStreamArgs([withEmail], state)),
      ).rejects.toThrow(/email detected/);
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
      ).rejects.toThrow(/ssn detected/);

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
        // The D1 guard: an object-only policy needs a sink to carry the
        // one-time non-streaming-result coverage warning.
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
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: async () => ({
              allowed: false,
              reason: 'flagged as unsafe',
            }),
            evaluateEveryChars: 1,
          }),
        ],
      });

      // #when / #then
      await expect(
        engine.processOutputStream(makeStreamArgs([textDelta('x')])),
      ).rejects.toThrow(/classifier: flagged as unsafe/);
    });
  });

  describe('fail-closed', () => {
    it('fails closed when classify throws synchronously', async () => {
      // #given
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: () => {
              throw new Error('classifier exploded');
            },
          }),
        ],
      });

      // #when / #then
      await expect(
        engine.processOutputResult(makeOutputArgs('anything')),
      ).rejects.toThrow('classifier exploded');
    });

    it('fails closed when classify rejects', async () => {
      // #given
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: async () => {
              throw new Error('async classifier failure');
            },
          }),
        ],
      });

      // #when / #then
      await expect(
        engine.processOutputResult(makeOutputArgs('anything')),
      ).rejects.toThrow('async classifier failure');
    });

    it('fails closed when classify exceeds timeoutMs', async () => {
      // #given — never settles on its own
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: () => new Promise<PolicyDecision>(() => {}),
            timeoutMs: 20,
            name: 'slow-classifier',
          }),
        ],
      });

      // #when / #then
      await expect(
        engine.processOutputResult(makeOutputArgs('anything')),
      ).rejects.toThrow(/slow-classifier timed out after 20ms/);
    });

    it('does not crash when a slow classify eventually rejects after its own timeout already fired', async () => {
      // #given — the timeout (5ms) wins the race; classify's own later
      // rejection (15ms) must still be handled, never an unhandled rejection
      const engine = new PolicyEngine({
        policies: [
          classifierPolicy({
            classify: () =>
              new Promise<PolicyDecision>((_resolve, reject) => {
                setTimeout(() => reject(new Error('late failure')), 15);
              }),
            timeoutMs: 5,
            name: 'late-classifier',
          }),
        ],
      });

      // #when / #then — the timeout error wins, not the classifier's own reason
      await expect(
        engine.processOutputResult(makeOutputArgs('anything')),
      ).rejects.toThrow(/late-classifier timed out after 5ms/);

      // give the classifier's now-irrelevant rejection a chance to fire;
      // an unhandled rejection here would surface as a process warning
      await new Promise((resolve) => setTimeout(resolve, 30));
    });
  });
});
