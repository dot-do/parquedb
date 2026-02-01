/**
 * Type declarations for external modules without TypeScript definitions.
 */

/**
 * snappyjs - Pure JavaScript Snappy compression library
 * @see https://github.com/zhipeng-jia/snappyjs
 */
declare module 'snappyjs' {
  /**
   * Decompress Snappy-compressed data
   * @param compressed - Compressed data as ArrayBuffer, Uint8Array, or Buffer
   * @param maxLength - Maximum expected uncompressed length
   * @returns Decompressed data in the same type as input
   */
  export function uncompress(
    compressed: Uint8Array,
    maxLength: number
  ): Uint8Array
  export function uncompress(
    compressed: ArrayBuffer,
    maxLength: number
  ): ArrayBuffer

  /**
   * Compress data using Snappy algorithm
   * @param uncompressed - Data to compress as ArrayBuffer, Uint8Array, or Buffer
   * @returns Compressed data in the same type as input
   */
  export function compress(uncompressed: Uint8Array): Uint8Array
  export function compress(uncompressed: ArrayBuffer): ArrayBuffer
}

/**
 * hyparquet-compressors internal modules
 * These are individual decompressors from the hyparquet-compressors package.
 * We import them directly to avoid the main index.js which imports hysnappy
 * (WebAssembly-based) which fails in Cloudflare Workers.
 */
declare module 'hyparquet-compressors/src/brotli.js' {
  /**
   * Decompress Brotli-compressed data
   * @param input - Compressed data
   * @param outputLength - Expected output length
   * @returns Decompressed data
   */
  export function decompressBrotli(
    input: Uint8Array,
    outputLength: number
  ): Uint8Array
}

declare module 'hyparquet-compressors/src/gzip.js' {
  /**
   * Decompress GZIP-compressed data
   * @param input - Compressed data
   * @param output - Optional output buffer to write to
   * @returns Decompressed data
   */
  export function gunzip(input: Uint8Array, output?: Uint8Array): Uint8Array
}
