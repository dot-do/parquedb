/**
 * Sync Token Signing Module
 *
 * Provides HMAC-SHA256 signed tokens for secure upload/download URLs.
 * Tokens are used by sync-routes.ts to authorize file uploads and downloads.
 *
 * Token format: base64url(payload).base64url(hmac-sha256-signature)
 *
 * Security features:
 * - HMAC-SHA256 signature prevents tampering and forgery
 * - Type field prevents upload/download token confusion
 * - Expiration time prevents replay attacks
 * - Base64url encoding ensures URL safety
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Token payload for upload/download signed URLs
 */
export interface TokenPayload {
  databaseId: string
  path: string
  userId: string
  expiresAt: string
}

/**
 * Internal token data including type
 */
interface TokenData extends TokenPayload {
  type: 'upload' | 'download'
}

/**
 * Environment interface for token operations
 * Only requires SYNC_SECRET from the full Env
 */
export interface TokenEnv {
  SYNC_SECRET?: string
}

// =============================================================================
// HMAC Key Management
// =============================================================================

/**
 * Get the HMAC key from the environment
 * @throws Error if SYNC_SECRET is not configured
 */
async function getHmacKey(env: TokenEnv): Promise<CryptoKey> {
  if (!env.SYNC_SECRET) {
    throw new Error('SYNC_SECRET environment variable is required for token signing')
  }

  const encoder = new TextEncoder()
  const keyData = encoder.encode(env.SYNC_SECRET)

  return crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify']
  )
}

// =============================================================================
// HMAC Operations
// =============================================================================

/**
 * Sign data with HMAC-SHA256 and return base64url-encoded signature
 */
async function hmacSign(data: string, key: CryptoKey): Promise<string> {
  const encoder = new TextEncoder()
  const dataBuffer = encoder.encode(data)

  const signature = await crypto.subtle.sign('HMAC', key, dataBuffer)
  const signatureArray = new Uint8Array(signature)

  // Convert to base64url
  let binary = ''
  for (const byte of signatureArray) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

/**
 * Verify HMAC-SHA256 signature
 */
async function hmacVerify(data: string, signature: string, key: CryptoKey): Promise<boolean> {
  try {
    const encoder = new TextEncoder()
    const dataBuffer = encoder.encode(data)

    // Convert base64url signature back to ArrayBuffer
    const base64 = signature.replace(/-/g, '+').replace(/_/g, '/')
    const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4)
    const binary = atob(padded)
    const signatureBuffer = new Uint8Array(binary.length)
    for (let i = 0; i < binary.length; i++) {
      signatureBuffer[i] = binary.charCodeAt(i)
    }

    return crypto.subtle.verify('HMAC', key, signatureBuffer, dataBuffer)
  } catch {
    return false
  }
}

// =============================================================================
// Token Signing
// =============================================================================

/**
 * Sign an upload token using HMAC-SHA256
 * Token format: base64url(payload).base64url(signature)
 */
export async function signUploadToken(payload: TokenPayload, env: TokenEnv): Promise<string> {
  const data = JSON.stringify({
    ...payload,
    type: 'upload',
  })

  const key = await getHmacKey(env)
  const payloadEncoded = btoa(data).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  const signature = await hmacSign(data, key)

  return `${payloadEncoded}.${signature}`
}

/**
 * Sign a download token using HMAC-SHA256
 * Token format: base64url(payload).base64url(signature)
 */
export async function signDownloadToken(payload: TokenPayload, env: TokenEnv): Promise<string> {
  const data = JSON.stringify({
    ...payload,
    type: 'download',
  })

  const key = await getHmacKey(env)
  const payloadEncoded = btoa(data).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  const signature = await hmacSign(data, key)

  return `${payloadEncoded}.${signature}`
}

// =============================================================================
// Token Verification
// =============================================================================

/**
 * Verify an upload token
 * Returns the payload if valid, null otherwise
 */
export async function verifyUploadToken(token: string, env: TokenEnv): Promise<TokenPayload | null> {
  return verifyToken(token, 'upload', env)
}

/**
 * Verify a download token
 * Returns the payload if valid, null otherwise
 */
export async function verifyDownloadToken(token: string, env: TokenEnv): Promise<TokenPayload | null> {
  return verifyToken(token, 'download', env)
}

/**
 * Internal token verification
 */
async function verifyToken(
  token: string,
  expectedType: 'upload' | 'download',
  env: TokenEnv
): Promise<TokenPayload | null> {
  try {
    const parts = token.split('.')
    if (parts.length !== 2) {
      return null
    }

    const [payloadEncoded, signature] = parts as [string, string]

    // Decode payload
    const base64 = payloadEncoded.replace(/-/g, '+').replace(/_/g, '/')
    const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4)
    const jsonStr = atob(padded)
    const data = JSON.parse(jsonStr) as TokenData

    // Verify type
    if (data.type !== expectedType) {
      return null
    }

    // Check expiration
    if (new Date(data.expiresAt) < new Date()) {
      return null
    }

    // Verify HMAC signature
    const key = await getHmacKey(env)
    const isValid = await hmacVerify(jsonStr, signature, key)
    if (!isValid) {
      return null
    }

    return {
      databaseId: data.databaseId,
      path: data.path,
      userId: data.userId,
      expiresAt: data.expiresAt,
    }
  } catch {
    return null
  }
}
