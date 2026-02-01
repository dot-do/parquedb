/**
 * Worker-safe Base64 Encoding Utilities
 *
 * These functions work in both Node.js and Cloudflare Workers environments,
 * avoiding Buffer.from() which is not available in Workers.
 *
 * @module utils/base64
 */

const BASE64_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'

/**
 * Encode a Uint8Array to base64 string
 *
 * Works in both Node.js and Workers environments.
 */
export function encodeBase64(bytes: Uint8Array): string {
  let result = ''
  const len = bytes.length

  for (let i = 0; i < len; i += 3) {
    const b1 = bytes[i]!
    const b2 = i + 1 < len ? bytes[i + 1]! : 0
    const b3 = i + 2 < len ? bytes[i + 2]! : 0

    result += BASE64_CHARS[(b1 >> 2) & 0x3f]
    result += BASE64_CHARS[((b1 << 4) | (b2 >> 4)) & 0x3f]
    result += i + 1 < len ? BASE64_CHARS[((b2 << 2) | (b3 >> 6)) & 0x3f] : '='
    result += i + 2 < len ? BASE64_CHARS[b3 & 0x3f] : '='
  }

  return result
}

/**
 * Decode a base64 string to Uint8Array
 *
 * Works in both Node.js and Workers environments.
 */
export function decodeBase64(base64: string): Uint8Array {
  // Remove padding and create lookup
  const cleaned = base64.replace(/=/g, '')
  const lookup = new Map<string, number>()
  for (let i = 0; i < BASE64_CHARS.length; i++) {
    lookup.set(BASE64_CHARS[i]!, i)
  }

  const bytes: number[] = []
  for (let i = 0; i < cleaned.length; i += 4) {
    const c1 = lookup.get(cleaned[i]!) ?? 0
    const c2 = lookup.get(cleaned[i + 1] ?? '') ?? 0
    const c3 = lookup.get(cleaned[i + 2] ?? '') ?? 0
    const c4 = lookup.get(cleaned[i + 3] ?? '') ?? 0

    bytes.push((c1 << 2) | (c2 >> 4))
    if (i + 2 < cleaned.length) {
      bytes.push(((c2 << 4) | (c3 >> 2)) & 0xff)
    }
    if (i + 3 < cleaned.length) {
      bytes.push(((c3 << 6) | c4) & 0xff)
    }
  }

  return new Uint8Array(bytes)
}

/**
 * Encode a UTF-8 string to base64
 *
 * This handles Unicode characters correctly by first encoding to UTF-8 bytes.
 * Works in both Node.js and Workers environments.
 */
export function stringToBase64(str: string): string {
  const encoder = new TextEncoder()
  const bytes = encoder.encode(str)
  return encodeBase64(bytes)
}

/**
 * Decode a base64 string to UTF-8 string
 *
 * Works in both Node.js and Workers environments.
 */
export function base64ToString(base64: string): string {
  const bytes = decodeBase64(base64)
  const decoder = new TextDecoder()
  return decoder.decode(bytes)
}
