/**
 * Parquet compressors with patched LZ4.
 *
 * Re-exports all compressors from hyparquet-compressors but replaces
 * the LZ4 decompressor with our patched version that fixes the match
 * length extension bug.
 */

import { compressors as baseCompressors } from 'hyparquet-compressors'
import { decompressLz4, decompressLz4Raw } from './lz4'

/**
 * Compressors for parquet decompression.
 * Uses patched LZ4 that fixes match length extension bug.
 */
export const compressors = {
  ...baseCompressors,
  LZ4: decompressLz4,
  LZ4_RAW: decompressLz4Raw,
}
