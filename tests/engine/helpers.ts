/**
 * Shared Test Helpers for Engine Tests
 *
 * Centralizes commonly duplicated helper functions used across engine test files.
 * This file should NOT contain any test cases itself.
 */

import type { DataLine, RelLine } from '@/engine/types'

// =============================================================================
// Entity Helpers
// =============================================================================

/** Create a DataLine with sensible defaults */
export function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

// =============================================================================
// Relationship Helpers
// =============================================================================

/** Create a link RelLine (positional args) */
export function makeLink(f: string, p: string, r: string, t: string, ts = 1000): RelLine {
  return { $op: 'l', $ts: ts, f, p, r, t }
}

/** Create an unlink RelLine (positional args) */
export function makeUnlink(f: string, p: string, r: string, t: string, ts = 2000): RelLine {
  return { $op: 'u', $ts: ts, f, p, r, t }
}

/** Create a RelLine (positional args, alias for makeLink) */
export function makeRel(f: string, p: string, r: string, t: string, ts = 1000): RelLine {
  return { $op: 'l', $ts: ts, f, p, r, t }
}

// =============================================================================
// Parquet Helpers
// =============================================================================

/** Decode a Parquet ArrayBuffer into rows using hyparquet */
export async function decodeParquet(buffer: ArrayBuffer): Promise<Array<Record<string, unknown>>> {
  const { parquetReadObjects } = await import('hyparquet')
  const asyncBuffer = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) => buffer.slice(start, end ?? buffer.byteLength),
  }
  return parquetReadObjects({ file: asyncBuffer }) as Promise<Array<Record<string, unknown>>>
}

// =============================================================================
// Conversion Helpers
// =============================================================================

/** Convert BigInt values to Number for comparison */
export function toNumber(value: unknown): number {
  if (typeof value === 'bigint') return Number(value)
  if (typeof value === 'number') return value
  return 0
}
