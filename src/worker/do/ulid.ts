/**
 * ULID Generation
 *
 * Simplified ULID (Universally Unique Lexicographically Sortable Identifier)
 * generation for event IDs.
 */

import { getRandom48Bit } from '../../utils'

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
let lastTime = 0
let lastRandom = 0

/**
 * Generate a ULID string
 *
 * ULIDs are 26 character strings that:
 * - Are lexicographically sortable
 * - Encode a timestamp in the first 10 characters
 * - Are URL-safe
 * - Are case-insensitive (using Crockford's Base32)
 *
 * @returns A new ULID string
 */
export function generateULID(): string {
  let now = Date.now()
  if (now === lastTime) {
    lastRandom++
  } else {
    lastTime = now
    // Use cryptographically secure random for ULID random component
    lastRandom = getRandom48Bit()
  }

  let time = ''
  for (let i = 0; i < 10; i++) {
    time = ENCODING[now % 32] + time
    now = Math.floor(now / 32)
  }

  let random = ''
  let r = lastRandom
  for (let i = 0; i < 16; i++) {
    random = ENCODING[r % 32] + random
    r = Math.floor(r / 32)
  }

  return time + random
}
