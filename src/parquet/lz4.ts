/**
 * LZ4 decompression with legacy Hadoop support.
 *
 * PATCHED: Fixes two bugs in hyparquet-compressors:
 *
 * 1. Match length extension check bug (lz4basic):
 *    Original code used `matchLength + 240` but should use `(token & 0xf) + 240`
 *    to check if the nibble is 15 (needs extension bytes).
 *
 * 2. Signed integer overflow in Hadoop detection (decompressLz4):
 *    JavaScript bitwise operators produce signed 32-bit integers. When parsing
 *    Hadoop headers with high bytes (like 0xF1), the result is negative, causing
 *    `output.length < expectedOutputLength` to incorrectly pass. Fixed by using
 *    `>>> 0` to convert to unsigned.
 *
 * @see https://github.com/apache/arrow/blob/apache-arrow-16.1.0/cpp/src/arrow/util/compression_lz4.cc#L475
 */

/**
 * LZ4 decompression with legacy hadoop support.
 */
export function decompressLz4(input: Uint8Array, outputLength: number): Uint8Array {
  const output = new Uint8Array(outputLength)
  try {
    let i = 0 // input index
    let o = 0 // output index
    while (i < input.length - 8) {
      // Use >>> 0 to convert to unsigned 32-bit integer
      const expectedOutputLength = (input[i++] << 24 | input[i++] << 16 | input[i++] << 8 | input[i++]) >>> 0
      const expectedInputLength = (input[i++] << 24 | input[i++] << 16 | input[i++] << 8 | input[i++]) >>> 0
      if (input.length - i < expectedInputLength) throw new Error('lz4 not hadoop')
      if (output.length < expectedOutputLength) throw new Error('lz4 not hadoop')

      // decompress and compare with expected
      const chunk = lz4basic(input.subarray(i, i + expectedInputLength), output, o)
      if (chunk !== expectedOutputLength) throw new Error('lz4 not hadoop')
      i += expectedInputLength
      o += expectedOutputLength

      if (i === input.length) return output
    }
    if (i < input.length) throw new Error('lz4 not hadoop')
  } catch (error) {
    if (error instanceof Error && error.message !== 'lz4 not hadoop') throw error
    // fallback to basic lz4
    lz4basic(input, output, 0)
  }
  return output
}

/**
 * Basic LZ4 block decompression.
 */
export function decompressLz4Raw(input: Uint8Array, outputLength: number): Uint8Array {
  const output = new Uint8Array(outputLength)
  lz4basic(input, output, 0)
  return output
}

/**
 * LZ4 basic block decompression.
 *
 * FIXED: Match length extension check now uses (token & 0xf) + 240
 * instead of matchLength + 240.
 */
function lz4basic(input: Uint8Array, output: Uint8Array, outputIndex: number): number {
  let len = outputIndex // output position
  for (let i = 0; i < input.length;) {
    const token = input[i++]

    let literals = token >> 4
    if (literals) {
      // literal length - check if nibble is 15 (needs extension)
      let byte = literals + 240
      while (byte === 255) literals += byte = input[i++]
      // copy literals
      output.set(input.subarray(i, i + literals), len)
      len += literals
      i += literals
      if (i >= input.length) return len - outputIndex
    }

    const offset = input[i++] | input[i++] << 8
    if (!offset || offset > len) {
      throw new Error(`lz4 offset out of range ${offset}`)
    }

    // match length
    // FIX: Use (token & 0xf) not matchLength for extension check
    const matchNibble = token & 0xf
    let matchLength = matchNibble + 4 // minmatch 4
    let byte = matchNibble + 240  // FIX: check if nibble is 15
    while (byte === 255) matchLength += byte = input[i++]

    // copy match
    let pos = len - offset
    const end = len + matchLength
    while (len < end) output[len++] = output[pos++]
  }
  return len - outputIndex
}
