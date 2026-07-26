// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import {
  githubResourceAllowed,
  modelConfig,
  UNCONFIGURED_MODEL_ID,
} from '../src/config.js';

describe('starter configuration', () => {
  it('rejects the committed model placeholder before provider access', () => {
    expect(() =>
      modelConfig({ MODEL_ID: UNCONFIGURED_MODEL_ID } as Env),
    ).toThrow('MODEL_ID is not configured');
  });

  it('fails GitHub ownership closed and permits only configured resources', () => {
    const configured = {
      GITHUB_RESOURCE_ALLOWLIST: '{"acme":["github:example/repository"]}',
    } as Env;

    expect(
      githubResourceAllowed(configured, 'acme', 'github:example/repository'),
    ).toBe(true);
    expect(
      githubResourceAllowed(configured, 'acme', 'github:example/repository#42'),
    ).toBe(true);
    expect(
      githubResourceAllowed(configured, 'acme', 'github:other/repository'),
    ).toBe(false);
    expect(
      githubResourceAllowed(
        { GITHUB_RESOURCE_ALLOWLIST: 'invalid' } as Env,
        'acme',
        'github:example/repository',
      ),
    ).toBe(false);
  });
});
