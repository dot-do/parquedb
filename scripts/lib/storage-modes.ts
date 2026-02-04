/**
 * Storage Mode Utilities for Benchmark Data Generation
 *
 * Supports 4 storage modes:
 * - columnar-only: Native typed columns, no row store
 * - columnar-row:  Native typed columns + $data JSON blob
 * - row-only:      Just $id, $type, $data (minimal)
 * - row-index:     $data + $index_* shredded columns
 */

import { parquetWriteBuffer } from 'hyparquet-writer'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'

export type StorageMode = 'columnar-only' | 'columnar-row' | 'row-only' | 'row-index'

export interface ColumnDef {
  name: string
  type: 'STRING' | 'INT32' | 'INT64' | 'DOUBLE' | 'BOOLEAN' | 'FLOAT'
  data: unknown[]
}

export interface EntitySchema {
  /** Fields that should be native columns in typed mode */
  columns: Array<{
    name: string
    type: ColumnDef['type']
    indexed?: boolean  // If true, becomes $index_{name} in row-index mode
  }>
}

export interface GenerateOptions {
  mode: StorageMode
  outputDir: string
  rowGroupSize?: number
}

/**
 * Generate column data for a specific storage mode
 */
export function generateColumns<T extends Record<string, unknown>>(
  entities: T[],
  schema: EntitySchema,
  mode: StorageMode,
  getEntityId: (e: T) => string,
  getEntityType: (e: T) => string,
  getEntityName: (e: T) => string
): ColumnDef[] {
  const columns: ColumnDef[] = []

  // $id is always present
  columns.push({
    name: '$id',
    type: 'STRING',
    data: entities.map(getEntityId),
  })

  // $type is always present
  columns.push({
    name: '$type',
    type: 'STRING',
    data: entities.map(getEntityType),
  })

  switch (mode) {
    case 'columnar-only':
      // Native columns only, no $data
      columns.push({
        name: 'name',
        type: 'STRING',
        data: entities.map(getEntityName),
      })
      for (const col of schema.columns) {
        if (col.name === 'name') continue // Already added
        columns.push({
          name: col.name,
          type: col.type,
          data: entities.map(e => e[col.name] ?? null),
        })
      }
      break

    case 'columnar-row':
      // Native columns + $data JSON blob
      columns.push({
        name: 'name',
        type: 'STRING',
        data: entities.map(getEntityName),
      })
      for (const col of schema.columns) {
        if (col.name === 'name') continue
        columns.push({
          name: col.name,
          type: col.type,
          data: entities.map(e => e[col.name] ?? null),
        })
      }
      // Add $data row store
      columns.push({
        name: 'data',
        type: 'STRING',
        data: entities.map(e => JSON.stringify(e)),
      })
      break

    case 'row-only':
      // Minimal: just $id, $type, name, $data
      columns.push({
        name: 'name',
        type: 'STRING',
        data: entities.map(getEntityName),
      })
      columns.push({
        name: 'data',
        type: 'STRING',
        data: entities.map(e => JSON.stringify(e)),
      })
      break

    case 'row-index':
      // $data + $index_* for indexed fields
      columns.push({
        name: 'name',
        type: 'STRING',
        data: entities.map(getEntityName),
      })
      // Add $index_* columns for indexed fields
      for (const col of schema.columns) {
        if (col.indexed) {
          columns.push({
            name: `$index_${col.name}`,
            type: col.type,
            data: entities.map(e => e[col.name] ?? null),
          })
        }
      }
      // Add $data row store
      columns.push({
        name: 'data',
        type: 'STRING',
        data: entities.map(e => JSON.stringify(e)),
      })
      break
  }

  return columns
}

/**
 * Write parquet file with given columns
 */
export async function writeParquetFile(
  outputPath: string,
  columns: ColumnDef[],
  rowGroupSize: number = 5000
): Promise<{ path: string; size: number; rows: number }> {
  const buffer = parquetWriteBuffer({
    columnData: columns.map(col => ({
      name: col.name,
      type: col.type,
      data: col.data,
    })),
    rowGroupSize,
  })

  await fs.mkdir(join(outputPath, '..'), { recursive: true })
  await fs.writeFile(outputPath, Buffer.from(buffer))

  return {
    path: outputPath,
    size: buffer.byteLength,
    rows: columns[0]?.data.length ?? 0,
  }
}

/**
 * Get output directory for a dataset and mode
 */
export function getOutputDir(baseDir: string, dataset: string, mode: StorageMode): string {
  return join(baseDir, `${dataset}-${mode}`)
}

/**
 * Format bytes as human-readable string
 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`
}

/**
 * Format number with commas
 */
export function formatNumber(n: number): string {
  return n.toLocaleString()
}
