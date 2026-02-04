/**
 * Parquet compressors with Worker-compatible Snappy.
 *
 * IMPORTANT: This module does NOT import from 'hyparquet-compressors' directly
 * because that package uses 'hysnappy' which instantiates WebAssembly synchronously.
 * Cloudflare Workers require async WebAssembly instantiation for modules > 4KB.
 *
 * Instead, we:
 * 1. Use 'snappyjs' - a pure JavaScript Snappy implementation
 * 2. Import individual decompressors from hyparquet-compressors/src/* directly
 * 3. Use our patched LZ4 that fixes match length extension bug
 *
 * This makes the compressors work in all environments:
 * - Node.js
 * - Browsers
 * - Cloudflare Workers
 */

// Import individual decompressors (avoiding the problematic index.js that imports hysnappy)
import { decompress as decompressZstd } from 'fzstd'
// @ts-expect-error - hyparquet-compressors does not have type declarations
import { decompressBrotli } from 'hyparquet-compressors/src/brotli.js'
// @ts-expect-error - hyparquet-compressors does not have type declarations
import { gunzip } from 'hyparquet-compressors/src/gzip.js'

// Pure JavaScript Snappy (no WebAssembly - works everywhere)
// @ts-expect-error - snappyjs does not have type declarations
import { uncompress as snappyUncompress } from 'snappyjs'

// Patched LZ4 that fixes match length extension bug
import { decompressLz4, decompressLz4Raw } from './lz4'

/**
 * Decompress Snappy data using pure JavaScript implementation.
 * Works in Node.js, browsers, and Cloudflare Workers.
 *
 * @param input - Compressed data
 * @param outputLength - Expected output length
 * @returns Decompressed data
 */
function decompressSnappy(input: Uint8Array, outputLength: number): Uint8Array {
  // snappyjs.uncompress returns Uint8Array when given Uint8Array input
  return snappyUncompress(input, outputLength) as Uint8Array
}

/**
 * Decompress GZIP data.
 *
 * @param input - Compressed data
 * @param outputLength - Expected output length
 * @returns Decompressed data
 */
function decompressGzip(input: Uint8Array, outputLength: number): Uint8Array {
  return gunzip(input, new Uint8Array(outputLength))
}

/**
 * Compressors for parquet decompression.
 *
 * Worker-compatible: Uses pure JavaScript Snappy instead of WebAssembly.
 * Also uses patched LZ4 that fixes match length extension bug.
 */
export const compressors = {
  SNAPPY: decompressSnappy,
  GZIP: decompressGzip,
  BROTLI: decompressBrotli,
  ZSTD: (input: Uint8Array, _outputLength: number) => decompressZstd(input),
  LZ4: decompressLz4,
  LZ4_RAW: decompressLz4Raw,
}
