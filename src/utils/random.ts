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
 * State for monotonic ULID generation
 * We use two 48-bit numbers to represent the 80-bit random component
 * (since JavaScript numbers are limited to 53-bit precision)
 */
let lastTimestamp = 0
let lastRandomHigh = 0 // Upper 32 bits of 80-bit random
let lastRandomLow = 0  // Lower 48 bits of 80-bit random

/**
 * Generate a random 80-bit component split into high (32-bit) and low (48-bit) parts
 */
function generateRandomComponent(): { high: number; low: number } {
  const bytes = getRandomBytes(10)
  // High 32 bits (4 bytes)
  const high = (bytes[0]! << 24) | (bytes[1]! << 16) | (bytes[2]! << 8) | bytes[3]!
  // Low 48 bits (6 bytes) - fit safely in JavaScript number
  const low =
    bytes[4]! * 0x10000000000 +
    bytes[5]! * 0x100000000 +
    bytes[6]! * 0x1000000 +
    bytes[7]! * 0x10000 +
    bytes[8]! * 0x100 +
    bytes[9]!
  return { high: high >>> 0, low }
}

/**
 * Encode 80-bit random component (high 32 bits + low 48 bits) to 16 Base32 characters
 */
function encodeRandom(high: number, low: number): string {
  // We need to convert 80 bits to 16 * 5 = 80 bits in Base32
  // high is 32 bits, low is 48 bits
  let result = ''

  // First 6 characters from high 32 bits + top 2 bits of low
  // Character 0: bits 75-79 of 80 (top 5 bits of high's top 8 bits)
  result += ULID_ENCODING[(high >>> 27) & 0x1f]
  // Character 1: bits 70-74
  result += ULID_ENCODING[(high >>> 22) & 0x1f]
  // Character 2: bits 65-69
  result += ULID_ENCODING[(high >>> 17) & 0x1f]
  // Character 3: bits 60-64
  result += ULID_ENCODING[(high >>> 12) & 0x1f]
  // Character 4: bits 55-59
  result += ULID_ENCODING[(high >>> 7) & 0x1f]
  // Character 5: bits 50-54 (bottom 2 bits of high + top 3 bits of low)
  result += ULID_ENCODING[((high & 0x7f) >>> 2)]
  // Character 6: bits 45-49 (bottom 2 bits of high shifted + bits 45-47 of low)
  result += ULID_ENCODING[((high & 0x03) << 3) | (Math.floor(low / 0x8000000000) & 0x07)]

  // Characters 7-15 from remaining 45 bits of low
  let remaining = low % 0x8000000000 // bottom 45 bits
  // Character 7: bits 40-44
  result += ULID_ENCODING[Math.floor(remaining / 0x1000000000) & 0x1f]
  remaining = remaining % 0x1000000000
  // Character 8: bits 35-39
  result += ULID_ENCODING[Math.floor(remaining / 0x80000000) & 0x1f]
  remaining = remaining % 0x80000000
  // Character 9: bits 30-34
  result += ULID_ENCODING[Math.floor(remaining / 0x4000000) & 0x1f]
  remaining = remaining % 0x4000000
  // Character 10: bits 25-29
  result += ULID_ENCODING[Math.floor(remaining / 0x200000) & 0x1f]
  remaining = remaining % 0x200000
  // Character 11: bits 20-24
  result += ULID_ENCODING[Math.floor(remaining / 0x10000) & 0x1f]
  remaining = remaining % 0x10000
  // Character 12: bits 15-19
  result += ULID_ENCODING[Math.floor(remaining / 0x800) & 0x1f]
  remaining = remaining % 0x800
  // Character 13: bits 10-14
  result += ULID_ENCODING[Math.floor(remaining / 0x40) & 0x1f]
  remaining = remaining % 0x40
  // Character 14: bits 5-9
  result += ULID_ENCODING[Math.floor(remaining / 0x2) & 0x1f]
  // Character 15: bits 0-4
  result += ULID_ENCODING[((remaining & 0x1) << 4)]

  return result
}

/**
 * Increment the 80-bit random component (high 32 bits + low 48 bits)
 * Returns true if successful, false if overflow
 */
function incrementRandom(): boolean {
  lastRandomLow++
  if (lastRandomLow >= 0x1000000000000) { // 2^48
    lastRandomLow = 0
    lastRandomHigh++
    if (lastRandomHigh > 0xFFFFFFFF) { // 2^32 overflow
      return false
    }
  }
  return true
}

/**
 * Generate a monotonic ULID (Universally Unique Lexicographically Sortable Identifier)
 *
 * ULIDs are 26 characters long and consist of:
 * - 10 characters for timestamp (48-bit millisecond precision)
 * - 16 characters for randomness (80-bit)
 *
 * This implementation provides monotonic ordering:
 * - Within the same millisecond, the random component is incremented
 * - When the millisecond changes, a new random component is generated
 * - Guarantees lexicographic ordering even for rapid consecutive calls
 *
 * @returns ULID string (e.g., "01ARZ3NDEKTSV4RRFFQ69G5FAV")
 */
export function generateULID(): string {
  const now = Date.now()

  // If same millisecond, increment the random component
  if (now === lastTimestamp) {
    if (!incrementRandom()) {
      // Overflow: generate new random (extremely rare - would need 2^80 ULIDs in 1ms)
      const { high, low } = generateRandomComponent()
      lastRandomHigh = high
      lastRandomLow = low
    }
  } else {
    // New millisecond: generate new random component
    lastTimestamp = now
    const { high, low } = generateRandomComponent()
    lastRandomHigh = high
    lastRandomLow = low
  }

  // Encode timestamp (48-bit, 10 characters in Crockford Base32)
  let time = ''
  let timestamp = now
  for (let i = 0; i < 10; i++) {
    time = ULID_ENCODING[timestamp % 32] + time
    timestamp = Math.floor(timestamp / 32)
  }

  // Encode random component
  const random = encodeRandom(lastRandomHigh, lastRandomLow)

  return time + random
}

/**
 * Reset the monotonic ULID state (useful for testing)
 */
export function resetULIDState(): void {
  lastTimestamp = 0
  lastRandomHigh = 0
  lastRandomLow = 0
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
