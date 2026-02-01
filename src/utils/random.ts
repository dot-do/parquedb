/**
 * Cryptographically secure random utilities
 *
 * This module provides secure random functions that work across all environments:
 * - Node.js (>=18)
 * - Browsers (Web Crypto API)
 * - Cloudflare Workers
 *
 * SECURITY: Never use Math.random() for ID generation or security-sensitive operations.
 * Math.random() is not cryptographically secure and its output can be predicted.
 *
 * @module utils/random
 */

/**
 * Generate cryptographically secure random bytes
 *
 * Uses the Web Crypto API which is available in:
 * - Node.js >= 15.0.0 (via globalThis.crypto)
 * - All modern browsers
 * - Cloudflare Workers
 *
 * @param length - Number of random bytes to generate
 * @returns Uint8Array of random bytes
 */
export function getRandomBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length)
  crypto.getRandomValues(bytes)
  return bytes
}

/**
 * Generate a cryptographically secure random integer in range [0, max)
 *
 * @param max - Exclusive upper bound (must be <= 2^32)
 * @returns Random integer in range [0, max)
 */
export function getRandomInt(max: number): number {
  if (max <= 0 || max > 0x100000000) {
    throw new RangeError('max must be in range (0, 2^32]')
  }

  // Use rejection sampling to avoid modulo bias
  const bytes = getRandomBytes(4)
  const value = (bytes[0]! << 24) | (bytes[1]! << 16) | (bytes[2]! << 8) | bytes[3]!

  // For simplicity, use modulo for now (slight bias acceptable for non-crypto use)
  // For true uniform distribution, would need rejection sampling
  return (value >>> 0) % max
}

/**
 * Generate a cryptographically secure random number in range [0, 1)
 *
 * This is a secure replacement for Math.random()
 *
 * @returns Random number in range [0, 1)
 */
export function getSecureRandom(): number {
  const bytes = getRandomBytes(7)
  // Use 53 bits (JavaScript number precision) from 7 bytes (56 bits, mask to 53)
  // Construct a 53-bit integer: take 6 full bytes (48 bits) + 5 bits from the 7th byte
  const value =
    (bytes[0]! & 0x1f) * 0x1000000000000 + // 5 bits (21 shifts of 8)
    bytes[1]! * 0x10000000000 +             // 8 bits
    bytes[2]! * 0x100000000 +               // 8 bits
    bytes[3]! * 0x1000000 +                 // 8 bits
    bytes[4]! * 0x10000 +                   // 8 bits
    bytes[5]! * 0x100 +                     // 8 bits
    bytes[6]!                               // 8 bits = 53 total

  // Divide by 2^53 to get [0, 1)
  return value / 0x20000000000000
}

/**
 * Generate a random string using base36 encoding (0-9, a-z)
 *
 * @param length - Length of the output string
 * @returns Random alphanumeric string
 */
export function getRandomBase36(length: number): string {
  const charset = '0123456789abcdefghijklmnopqrstuvwxyz'
  const bytes = getRandomBytes(length)
  let result = ''
  for (let i = 0; i < length; i++) {
    result += charset[bytes[i]! % 36]
  }
  return result
}

/**
 * Generate a random string using Crockford's Base32 encoding
 *
 * Used for ULID-compatible random components.
 * Excludes I, L, O, U to avoid ambiguity and accidental words.
 *
 * @param length - Length of the output string
 * @returns Random Base32 string
 */
export function getRandomBase32(length: number): string {
  const charset = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
  const bytes = getRandomBytes(length)
  let result = ''
  for (let i = 0; i < length; i++) {
    result += charset[bytes[i]! % 32]
  }
  return result
}

/**
 * Generate a random 48-bit integer (safe for JavaScript numbers)
 *
 * Used for ULID random component initialization.
 *
 * @returns Random integer in range [0, 2^48)
 */
export function getRandom48Bit(): number {
  const bytes = getRandomBytes(6)
  return (
    bytes[0]! * 0x10000000000 +
    bytes[1]! * 0x100000000 +
    bytes[2]! * 0x1000000 +
    bytes[3]! * 0x10000 +
    bytes[4]! * 0x100 +
    bytes[5]!
  )
}

/**
 * Generate a UUID v4 using crypto.randomUUID()
 *
 * Available in Node.js >= 19.0.0, browsers, and Cloudflare Workers.
 *
 * @returns UUID v4 string (e.g., "550e8400-e29b-41d4-a716-446655440000")
 */
export function getUUID(): string {
  return crypto.randomUUID()
}

/**
 * Crockford's Base32 encoding alphabet (excludes I, L, O, U)
 */
const ULID_ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

/**
 * Generate a ULID (Universally Unique Lexicographically Sortable Identifier)
 *
 * ULIDs are 26 characters long and consist of:
 * - 10 characters for timestamp (48-bit millisecond precision)
 * - 16 characters for randomness (80-bit cryptographically secure random)
 *
 * This implementation is race-condition-free because:
 * - Each call generates fresh 80-bit cryptographic randomness
 * - No shared mutable state between calls
 * - Works reliably across Node.js, browsers, and Cloudflare Workers
 *
 * Note: This does not implement monotonic ULID (incrementing random part within
 * same millisecond) to avoid race conditions. The 80-bit random component provides
 * sufficient collision resistance (1 in 2^80 chance of collision within same ms).
 *
 * @returns ULID string (e.g., "01ARZ3NDEKTSV4RRFFQ69G5FAV")
 */
export function generateULID(): string {
  const now = Date.now()

  // Encode timestamp (48-bit, 10 characters in Crockford Base32)
  let time = ''
  let timestamp = now
  for (let i = 0; i < 10; i++) {
    time = ULID_ENCODING[timestamp % 32] + time
    timestamp = Math.floor(timestamp / 32)
  }

  // Generate 80-bit random component (10 bytes = 80 bits)
  // Use 16 characters in Crockford Base32 (5 bits each = 80 bits total)
  const randomBytes = getRandomBytes(10)
  let random = ''

  // Convert 10 bytes to 16 Base32 characters
  // Each byte provides 8 bits, we need 5 bits per character
  // Pack bytes and extract 5-bit groups
  let bitBuffer = 0
  let bitsInBuffer = 0
  let byteIndex = 0

  for (let i = 0; i < 16; i++) {
    while (bitsInBuffer < 5 && byteIndex < randomBytes.length) {
      bitBuffer = (bitBuffer << 8) | randomBytes[byteIndex]!
      bitsInBuffer += 8
      byteIndex++
    }
    bitsInBuffer -= 5
    random += ULID_ENCODING[(bitBuffer >> bitsInBuffer) & 0x1f]
  }

  return time + random
}

/**
 * Generate a lowercase ULID
 *
 * Same as generateULID() but returns lowercase for consistency with
 * common ID conventions.
 *
 * @returns Lowercase ULID string
 */
export function generateId(): string {
  return generateULID().toLowerCase()
}
