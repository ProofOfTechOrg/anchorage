// SPDX-License-Identifier: Apache-2.0

import { importJWK, jwtVerify, SignJWT } from 'jose';
import { isDeploymentEnvironment } from '../deployment-identity-protocol.js';

export type MaintenanceCapabilityOperation =
  | 'ensure-maintenance'
  | 'maintenance-status';

export interface MaintenanceCapabilityClaims {
  readonly operation: MaintenanceCapabilityOperation;
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly specDigest: string;
  readonly expiresAt: number;
  readonly nonce: string;
}

export interface MintMaintenanceCapabilityOptions {
  readonly secret: string;
  readonly operation: MaintenanceCapabilityOperation;
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly specDigest: string;
  readonly ttlSeconds?: number;
  readonly now?: () => number;
  readonly nonce?: string;
}

export interface VerifyMaintenanceCapabilityOptions {
  readonly secret: string;
  readonly token: string;
  readonly operation: MaintenanceCapabilityOperation;
  readonly tenantTag: string;
  readonly environment: string;
  readonly now?: () => number;
}

export interface VerifyMaintenanceReceiptOptions {
  readonly secret: string;
  readonly token: string;
  readonly capability: MaintenanceCapabilityClaims;
  readonly now?: () => number;
}

export type MaintenanceCapabilityJwk = JsonWebKey & {
  readonly kid: string;
};

export interface MintAsymmetricMaintenanceCapabilityOptions
  extends Omit<MintMaintenanceCapabilityOptions, 'secret'> {
  readonly privateKey: MaintenanceCapabilityJwk;
}

export interface VerifyAsymmetricMaintenanceCapabilityOptions
  extends Omit<VerifyMaintenanceCapabilityOptions, 'secret'> {
  readonly publicKey: MaintenanceCapabilityJwk;
}

const encoder = new TextEncoder();
const CAPABILITY_AUDIENCE = 'flowsafe-maintenance';
const CAPABILITY_TYPE = 'flowsafe-maintenance-capability+jwt';
const CAPABILITY_ASYMMETRIC_TYPE =
  'flowsafe-maintenance-asymmetric-capability+jwt';
// Named after the protocol, like CAPABILITY_AUDIENCE above, deliberately NOT
// after the consuming package: the previous value tracked the fleet-control
// package name, so renaming that package forced a wire break for nothing.
//
// Both mintMaintenanceReceipt and verifyMaintenanceReceipt hard-code this, and
// the issuer runs inside each deployed tenant Worker at whatever Flowsafe that
// artifact bundled, while the verifier runs at the control plane's version. So
// a change here fails closed across live deployments with no other signal, and
// "upgrade both together" is not reachable once a fleet exists. Rotating it
// later means teaching the verifier an accept-set (jwtVerify takes an array for
// `audience`), minting the new value, and dropping the old one once the fleet
// has moved. A test pins the literal on the `aud` claim.
const RECEIPT_AUDIENCE = 'flowsafe-maintenance-receipt';
const RECEIPT_TYPE = 'flowsafe-maintenance-receipt+jwt';
export const MAINTENANCE_RECEIPT_HEADER =
  'x-flowsafe-maintenance-receipt' as const;
const DEFAULT_TTL_SECONDS = 30;
const MAX_TTL_SECONDS = 60;
const SECRET_PATTERN = /^[\x21-\x7e]{32,256}$/;
const TENANT_TAG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const SCRIPT_NAME_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const SPEC_DIGEST_PATTERN = /^[a-f0-9]{64}$/;
const NONCE_PATTERN = /^[A-Za-z0-9_-]{22}$/;

function assertSecret(secret: string): void {
  if (!SECRET_PATTERN.test(secret)) {
    throw new Error(
      'maintenance capability secret must be 32-256 visible ASCII characters',
    );
  }
}

function validOperation(
  value: unknown,
): value is MaintenanceCapabilityOperation {
  return value === 'ensure-maintenance' || value === 'maintenance-status';
}

interface DecodedMaintenanceCapability {
  readonly operation: MaintenanceCapabilityOperation;
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly specDigest: string;
  readonly exp: number;
  readonly nonce: string;
}

function validClaims(
  value: Readonly<Record<string, unknown>>,
): value is Readonly<Record<string, unknown>> & DecodedMaintenanceCapability {
  return (
    validOperation(value.operation) &&
    typeof value.tenantTag === 'string' &&
    TENANT_TAG_PATTERN.test(value.tenantTag) &&
    typeof value.environment === 'string' &&
    isDeploymentEnvironment(value.environment) &&
    typeof value.scriptName === 'string' &&
    SCRIPT_NAME_PATTERN.test(value.scriptName) &&
    typeof value.specDigest === 'string' &&
    SPEC_DIGEST_PATTERN.test(value.specDigest) &&
    typeof value.exp === 'number' &&
    Number.isSafeInteger(value.exp) &&
    typeof value.nonce === 'string' &&
    NONCE_PATTERN.test(value.nonce)
  );
}

function randomNonce(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

function capabilityInput(
  options: Omit<MintMaintenanceCapabilityOptions, 'secret'>,
): Readonly<{
  claims: MaintenanceCapabilityClaims;
  nowSeconds: number;
}> {
  if (!validOperation(options.operation)) {
    throw new Error('maintenance capability operation is invalid');
  }
  if (!TENANT_TAG_PATTERN.test(options.tenantTag)) {
    throw new Error('maintenance capability tenant tag is invalid');
  }
  if (!isDeploymentEnvironment(options.environment)) {
    throw new Error('maintenance capability environment is invalid');
  }
  if (!SCRIPT_NAME_PATTERN.test(options.scriptName)) {
    throw new Error('maintenance capability script name is invalid');
  }
  if (!SPEC_DIGEST_PATTERN.test(options.specDigest)) {
    throw new Error('maintenance capability specification digest is invalid');
  }
  const ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  if (
    !Number.isSafeInteger(ttlSeconds) ||
    ttlSeconds < 1 ||
    ttlSeconds > MAX_TTL_SECONDS
  ) {
    throw new Error(
      `maintenance capability lifetime must be 1-${MAX_TTL_SECONDS} seconds`,
    );
  }
  const nonce = options.nonce ?? randomNonce();
  if (!NONCE_PATTERN.test(nonce)) {
    throw new Error('maintenance capability nonce is invalid');
  }
  const nowSeconds = Math.floor((options.now ?? Date.now)() / 1_000);
  return {
    nowSeconds,
    claims: {
      operation: options.operation,
      tenantTag: options.tenantTag,
      environment: options.environment,
      scriptName: options.scriptName,
      specDigest: options.specDigest,
      expiresAt: nowSeconds + ttlSeconds,
      nonce,
    },
  };
}

export async function mintMaintenanceCapability(
  options: MintMaintenanceCapabilityOptions,
): Promise<{ token: string; claims: MaintenanceCapabilityClaims }> {
  assertSecret(options.secret);
  const { claims, nowSeconds } = capabilityInput(options);
  const token = await new SignJWT({
    operation: claims.operation,
    tenantTag: claims.tenantTag,
    environment: claims.environment,
    scriptName: claims.scriptName,
    specDigest: claims.specDigest,
    nonce: claims.nonce,
  })
    .setProtectedHeader({ alg: 'HS256', typ: CAPABILITY_TYPE })
    .setAudience(CAPABILITY_AUDIENCE)
    .setIssuedAt(nowSeconds)
    .setExpirationTime(claims.expiresAt)
    .sign(encoder.encode(options.secret));
  return { token, claims };
}

export async function mintAsymmetricMaintenanceCapability(
  options: MintAsymmetricMaintenanceCapabilityOptions,
): Promise<{ token: string; claims: MaintenanceCapabilityClaims }> {
  if (
    options.privateKey.kty !== 'OKP' ||
    options.privateKey.crv !== 'Ed25519' ||
    options.privateKey.alg !== 'EdDSA' ||
    typeof options.privateKey.kid !== 'string' ||
    !options.privateKey.kid ||
    typeof options.privateKey.x !== 'string' ||
    typeof options.privateKey.d !== 'string'
  ) {
    throw new Error(
      'maintenance capability private key requires Ed25519 signing material and kid',
    );
  }
  const { claims, nowSeconds } = capabilityInput(options);
  const key = await importJWK(options.privateKey, 'EdDSA');
  const token = await new SignJWT({
    operation: claims.operation,
    tenantTag: claims.tenantTag,
    environment: claims.environment,
    scriptName: claims.scriptName,
    specDigest: claims.specDigest,
    nonce: claims.nonce,
  })
    .setProtectedHeader({
      alg: 'EdDSA',
      typ: CAPABILITY_ASYMMETRIC_TYPE,
      kid: options.privateKey.kid,
    })
    .setAudience(CAPABILITY_AUDIENCE)
    .setIssuedAt(nowSeconds)
    .setExpirationTime(claims.expiresAt)
    .sign(key);
  return { token, claims };
}

export async function verifyMaintenanceCapability(
  options: VerifyMaintenanceCapabilityOptions,
): Promise<MaintenanceCapabilityClaims | undefined> {
  try {
    assertSecret(options.secret);
    const nowMs = (options.now ?? Date.now)();
    const nowSeconds = Math.floor(nowMs / 1_000);
    const verified = await jwtVerify(
      options.token,
      encoder.encode(options.secret),
      {
        algorithms: ['HS256'],
        audience: CAPABILITY_AUDIENCE,
        typ: CAPABILITY_TYPE,
        currentDate: new Date(nowMs),
        clockTolerance: 0,
        requiredClaims: ['exp', 'iat'],
      },
    );
    if (!validClaims(verified.payload)) return undefined;
    if (
      verified.payload.operation !== options.operation ||
      verified.payload.tenantTag !== options.tenantTag ||
      verified.payload.environment !== options.environment ||
      typeof verified.payload.iat !== 'number' ||
      !Number.isSafeInteger(verified.payload.iat) ||
      verified.payload.iat > nowSeconds ||
      verified.payload.exp - verified.payload.iat > MAX_TTL_SECONDS
    ) {
      return undefined;
    }
    return {
      operation: verified.payload.operation,
      tenantTag: verified.payload.tenantTag,
      environment: verified.payload.environment,
      scriptName: verified.payload.scriptName,
      specDigest: verified.payload.specDigest,
      expiresAt: verified.payload.exp,
      nonce: verified.payload.nonce,
    };
  } catch {
    return undefined;
  }
}

export async function verifyAsymmetricMaintenanceCapability(
  options: VerifyAsymmetricMaintenanceCapabilityOptions,
): Promise<MaintenanceCapabilityClaims | undefined> {
  try {
    if (
      options.publicKey.kty !== 'OKP' ||
      options.publicKey.crv !== 'Ed25519' ||
      options.publicKey.alg !== 'EdDSA' ||
      typeof options.publicKey.kid !== 'string' ||
      !options.publicKey.kid ||
      typeof options.publicKey.x !== 'string' ||
      options.publicKey.d !== undefined
    ) {
      return undefined;
    }
    const nowMs = (options.now ?? Date.now)();
    const nowSeconds = Math.floor(nowMs / 1_000);
    const verified = await jwtVerify(
      options.token,
      await importJWK(options.publicKey, 'EdDSA'),
      {
        algorithms: ['EdDSA'],
        audience: CAPABILITY_AUDIENCE,
        typ: CAPABILITY_ASYMMETRIC_TYPE,
        currentDate: new Date(nowMs),
        clockTolerance: 0,
      },
    );
    if (
      !validClaims(verified.payload) ||
      typeof options.publicKey.kid !== 'string' ||
      verified.protectedHeader.kid !== options.publicKey.kid ||
      verified.payload.operation !== options.operation ||
      verified.payload.tenantTag !== options.tenantTag ||
      verified.payload.environment !== options.environment ||
      !Number.isSafeInteger(verified.payload.iat) ||
      Number(verified.payload.iat) > nowSeconds ||
      verified.payload.exp - Number(verified.payload.iat) > MAX_TTL_SECONDS
    ) {
      return undefined;
    }
    return {
      operation: verified.payload.operation,
      tenantTag: verified.payload.tenantTag,
      environment: verified.payload.environment,
      scriptName: verified.payload.scriptName,
      specDigest: verified.payload.specDigest,
      expiresAt: verified.payload.exp,
      nonce: verified.payload.nonce,
    };
  } catch {
    return undefined;
  }
}

export async function mintMaintenanceReceipt(
  secret: string,
  capability: MaintenanceCapabilityClaims,
  result: unknown,
): Promise<string> {
  assertSecret(secret);
  return new SignJWT({
    operation: capability.operation,
    tenantTag: capability.tenantTag,
    environment: capability.environment,
    scriptName: capability.scriptName,
    specDigest: capability.specDigest,
    nonce: capability.nonce,
    result,
  })
    .setProtectedHeader({ alg: 'HS256', typ: RECEIPT_TYPE })
    .setAudience(RECEIPT_AUDIENCE)
    .setExpirationTime(capability.expiresAt)
    .sign(encoder.encode(secret));
}

export async function verifyMaintenanceReceipt(
  options: VerifyMaintenanceReceiptOptions,
): Promise<unknown | undefined> {
  try {
    assertSecret(options.secret);
    const verified = await jwtVerify(
      options.token,
      encoder.encode(options.secret),
      {
        algorithms: ['HS256'],
        audience: RECEIPT_AUDIENCE,
        typ: RECEIPT_TYPE,
        currentDate: new Date((options.now ?? Date.now)()),
        clockTolerance: 0,
        requiredClaims: ['exp'],
      },
    );
    const claims = verified.payload;
    const expected = options.capability;
    if (
      claims.operation !== expected.operation ||
      claims.tenantTag !== expected.tenantTag ||
      claims.environment !== expected.environment ||
      claims.scriptName !== expected.scriptName ||
      claims.specDigest !== expected.specDigest ||
      claims.nonce !== expected.nonce ||
      claims.exp !== expected.expiresAt ||
      !('result' in claims)
    ) {
      return undefined;
    }
    return claims.result;
  } catch {
    return undefined;
  }
}
