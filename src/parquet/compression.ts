/**
 * Compression utilities for ParqueDB
 *
 * Provides compression/decompression functions for Parquet files.
 * LZ4 is the default codec as it works on Cloudflare Workers without WASM.
 *
 * For reading: Uses hyparquet-compressors which includes decompressors for
 * all major codecs (LZ4, Snappy, GZIP, ZSTD, Brotli).
 *
 * For writing: Includes a pure JS LZ4 compressor that works in all environments.
 */

// Re-export patched decompressors (fixes LZ4 bugs)
export { compressors } from './compressors'

// =============================================================================
// LZ4 Compressor (for writing)
// =============================================================================

/**
 * LZ4 block compression (pure JavaScript implementation)
 *
 * Implements the LZ4 block format for parquet files.
 * This is a pure JS implementation that works in all environments
 * including Cloudflare Workers without requiring WASM.
 *
 * The format matches what hyparquet-compressors expects:
 * - Token byte: high 4 bits = literal length, low 4 bits = match length - 4
 * - If literal length is 15, additional bytes follow (each 255 adds to length)
 * - Literal bytes
 * - Offset (2 bytes, little-endian) - refers to output position
 * - If match length - 4 is 15, additional bytes follow
 *
 * NOTE: hyparquet-compressors has a bug in extended match length parsing,
 * so we limit matches to 18 bytes max (token nibble 14, no extension needed).
 * This reduces compression ratio slightly but ensures compatibility.
 *
 * @param input - Uncompressed data
 * @returns LZ4-compressed data
 */
export function compressLz4(input: Uint8Array): Uint8Array {
  if (input.length === 0) {
    return new Uint8Array(0)
  }

  const inputLength = input.length

  // For very small inputs or when we can't find matches, just emit as literals
  if (inputLength < 13) {
    return emitLiteralsOnly(input)
  }

  // LZ4 constants
  const MIN_MATCH = 4
  const HASH_LOG = 14
  const HASH_SIZE = 1 << HASH_LOG

  // Output buffer - worst case is about 1% larger than input
  const maxOutput = inputLength + (inputLength / 255 | 0) + 16
  const output = new Uint8Array(maxOutput)
  let op = 0 // output position

  // Hash table maps hash -> last seen position in input
  const hashTable = new Int32Array(HASH_SIZE).fill(-1)

  // Hash function (same as used in reference implementation)
  const hash4 = (pos: number): number => {
    const v = input[pos] | (input[pos + 1] << 8) | (input[pos + 2] << 16) | (input[pos + 3] << 24)
    return ((v * 2654435761) >>> 0) >>> (32 - HASH_LOG)
  }

  let ip = 0       // input position
  let anchor = 0   // position of last emitted literal

  // Limit: last 12 bytes must be literals (LZ4 spec for end-of-block safety)
  const mfLimit = inputLength - 12

  // Main loop: find matches and emit sequences
  while (ip < mfLimit) {
    const h = hash4(ip)
    const ref = hashTable[h]
    hashTable[h] = ip

    // Check for a valid match
    if (
      ref >= 0 &&
      ip - ref <= 65535 &&
      input[ref] === input[ip] &&
      input[ref + 1] === input[ip + 1] &&
      input[ref + 2] === input[ip + 2] &&
      input[ref + 3] === input[ip + 3]
    ) {
      // We have a match at ref!
      // First, count the match length
      // NOTE: We limit to 18 to avoid hyparquet-compressors bug with extended match lengths
      const MAX_MATCH = 18
      let matchLen = MIN_MATCH
      while (matchLen < MAX_MATCH && ip + matchLen < inputLength - 5 && input[ref + matchLen] === input[ip + matchLen]) {
        matchLen++
      }

      // Emit any literals before the match
      const litLen = ip - anchor

      // Write token
      const tokenPos = op++
      let token = Math.min(litLen, 15) << 4 | Math.min(matchLen - MIN_MATCH, 15)
      output[tokenPos] = token

      // Write extended literal length
      if (litLen >= 15) {
        let len = litLen - 15
        while (len >= 255) {
          output[op++] = 255
          len -= 255
        }
        output[op++] = len
      }

      // Write literals
      for (let j = 0; j < litLen; j++) {
        output[op++] = input[anchor + j]
      }

      // Write offset (little-endian)
      const offset = ip - ref
      output[op++] = offset & 0xff
      output[op++] = (offset >> 8) & 0xff

      // Note: With MAX_MATCH=18, matchLen - MIN_MATCH is at most 14,
      // so we never need extended match length bytes.
      // This avoids a bug in hyparquet-compressors extended match length parsing.

      // Move past the match
      ip += matchLen
      anchor = ip

      // Hash a position within the match to catch future matches
      // Note: Don't hash current ip, as it will be hashed at start of next iteration
      if (ip > 2 && ip < mfLimit) {
        hashTable[hash4(ip - 2)] = ip - 2
      }
    } else {
      ip++
    }
  }

  // Emit last literals (the remaining bytes after the last match)
  const lastLitLen = inputLength - anchor
  if (lastLitLen > 0) {
    // Token with match length = 0
    const tokenPos = op++
    output[tokenPos] = Math.min(lastLitLen, 15) << 4

    // Extended literal length
    if (lastLitLen >= 15) {
      let len = lastLitLen - 15
      while (len >= 255) {
        output[op++] = 255
        len -= 255
      }
      output[op++] = len
    }

    // Write remaining literals
    for (let j = 0; j < lastLitLen; j++) {
      output[op++] = input[anchor + j]
    }
  }

  return output.slice(0, op)
}

/**
 * Emit all data as literals (no compression)
 * Used for very small inputs
 */
function emitLiteralsOnly(input: Uint8Array): Uint8Array {
  const litLen = input.length

  // Calculate output size: token + extra length bytes + literals
  let outputSize = 1
  if (litLen >= 15) {
    outputSize += 1 + Math.floor((litLen - 15) / 255)
  }
  outputSize += litLen

  const output = new Uint8Array(outputSize)
  let op = 0

  // Token (literal length in high nibble, match length 0 in low nibble)
  output[op++] = Math.min(litLen, 15) << 4

  // Extended literal length
  if (litLen >= 15) {
    let len = litLen - 15
    while (len >= 255) {
      output[op++] = 255
      len -= 255
    }
    output[op++] = len
  }

  // Literals
  for (let i = 0; i < litLen; i++) {
    output[op++] = input[i]
  }

  return output
}

/**
 * LZ4 compression in Hadoop frame format
 *
 * Parquet files often use the Hadoop LZ4 frame format which wraps
 * LZ4 blocks with size headers. This function produces compatible output.
 *
 * @param input - Uncompressed data
 * @returns LZ4-compressed data in Hadoop frame format
 */
export function compressLz4Hadoop(input: Uint8Array): Uint8Array {
  if (input.length === 0) {
    return new Uint8Array(0)
  }

  // Compress the data
  const compressed = compressLz4(input)

  // Hadoop frame format: 4 bytes decompressed size + 4 bytes compressed size + data
  const output = new Uint8Array(8 + compressed.length)

  // Decompressed size (big-endian)
  output[0] = (input.length >> 24) & 0xff
  output[1] = (input.length >> 16) & 0xff
  output[2] = (input.length >> 8) & 0xff
  output[3] = input.length & 0xff

  // Compressed size (big-endian)
  output[4] = (compressed.length >> 24) & 0xff
  output[5] = (compressed.length >> 16) & 0xff
  output[6] = (compressed.length >> 8) & 0xff
  output[7] = compressed.length & 0xff

  // Compressed data
  output.set(compressed, 8)

  return output
}

// =============================================================================
// Compressors for Writing
// =============================================================================

/**
 * Write compressors - functions that compress data for writing to Parquet
 *
 * Note: hyparquet-writer expects compressors to take (input: Uint8Array) => Uint8Array
 */
export const writeCompressors = {
  /**
   * LZ4 compressor using Hadoop frame format (compatible with most Parquet readers)
   */
  LZ4: compressLz4Hadoop,

  /**
   * LZ4_RAW compressor (raw LZ4 block without frame header)
   */
  LZ4_RAW: compressLz4,
}
