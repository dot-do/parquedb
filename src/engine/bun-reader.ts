/**
 * Bun-optimized Parquet File Reader
 *
 * Provides optimized file reading for Parquet files when running on Bun:
 * - Uses Bun.file().arrayBuffer() for fast file reads
 * - Falls back to Node.js fs.readFile on other runtimes
 *
 * The returned AsyncBuffer is compatible with hyparquet's file interface.
 */

import { readFile } from 'node:fs/promises'

/** Whether we're running on Bun runtime */
const isBun = typeof globalThis.Bun !== 'undefined'

/** AsyncBuffer interface compatible with hyparquet */
export interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Promise<ArrayBuffer>
}

/**
 * Read a file and return an AsyncBuffer for hyparquet.
 * Uses Bun.file() on Bun runtime, fs.readFile on Node.js.
 */
export async function readParquetFile(path: string): Promise<AsyncBuffer | null> {
  try {
    if (isBun) {
      return await readWithBun(path)
    }
    return await readWithNode(path)
  } catch {
    return null
  }
}

async function readWithBun(path: string): Promise<AsyncBuffer> {
  // @ts-expect-error - Bun global
  const file = Bun.file(path)
  const buffer = await file.arrayBuffer()
  return {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) => {
      return buffer.slice(start, end ?? buffer.byteLength)
    },
  }
}

async function readWithNode(path: string): Promise<AsyncBuffer> {
  const buffer = await readFile(path)
  return {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) => {
      const sliced = buffer.slice(start, end ?? buffer.byteLength)
      return sliced.buffer.slice(sliced.byteOffset, sliced.byteOffset + sliced.byteLength)
    },
  }
}

/** Check if the current runtime is Bun */
export function isBunRuntime(): boolean {
  return isBun
}
