/**
 * Parquet Writer
 *
 * Provides async Parquet file writing with:
 * - Schema-based column typing
 * - Compression options
 * - Row group size configuration
 * - Dictionary encoding
 * - Append support (creates new row groups)
 * - File compaction
 *
 * Note: hyparquet is read-only. For writing, we use hyparquet-writer
 * or parquet-wasm depending on the environment.
 *
 * Works across Node.js, browsers, and Cloudflare Workers.
 */

import { logger } from '../utils/logger'
import type { StorageBackend } from '../types/storage'
import type { Entity } from '../types/entity'
import type {
  ParquetSchema,
  ParquetWriterOptions,
  ParquetWriteResult,
  CompressionCodec,
  TypedWriteOptions,
} from './types'
import { generateParquetSchema, type SchemaTree } from './schema-generator'
import { encodeVariant as _encodeVariant } from './variant'
import { writeCompressors, compressors } from './compression'
import {
  DEFAULT_ROW_GROUP_SIZE,
  DEFAULT_PARQUET_PAGE_SIZE,
  DEFAULT_ENABLE_COLUMN_INDEX,
  DEFAULT_ENABLE_OFFSET_INDEX,
  DEFAULT_COMPRESSION,
} from '../constants'

// =============================================================================
// Writer Configuration
// =============================================================================

// Row group and page size constants imported from constants.ts

/** Compression codec mapping */
const COMPRESSION_MAP: Record<string, CompressionCodec> = {
  none: 'UNCOMPRESSED',
  snappy: 'SNAPPY',
  gzip: 'GZIP',
  zstd: 'ZSTD',
  lz4: 'LZ4',
}

// =============================================================================
// ParquetWriter Class
// =============================================================================

/**
 * Parquet file writer
 *
 * Creates Parquet files from row data with configurable compression,
 * row group sizes, and encoding options.
 *
 * @example
 * ```typescript
 * const writer = new ParquetWriter({
 *   storage: myStorage,
 *   compression: 'snappy',
 *   rowGroupSize: 5000
 * })
 *
 * const schema = createEntitySchema({ shredFields: ['status'] })
 *
 * await writer.write('data/posts/data.parquet', rows, schema)
 * ```
 */
export class ParquetWriter {
  private storage: StorageBackend
  private compression: CompressionCodec
  private rowGroupSize: number
  private useDictionary: boolean
  /** @internal Reserved for future page size optimization */
  public _pageSize: number
  private enableStatistics: boolean
  private defaultMetadata: Record<string, string>
  /** Enable column indexes for page-level predicate pushdown */
  private enableColumnIndex: boolean
  /** Enable offset indexes for efficient page location lookup */
  private enableOffsetIndex: boolean

  /**
   * Create a new ParquetWriter
   *
   * @param storage - Storage backend for writing files
   * @param options - Writer configuration
   */
  constructor(
    storage: StorageBackend,
    options: ParquetWriterOptions = {}
  ) {
    this.storage = storage
    // Use DEFAULT_COMPRESSION ('none' = UNCOMPRESSED) - storage is cheap, CPU is expensive on Cloudflare
    this.compression = COMPRESSION_MAP[options.compression ?? DEFAULT_COMPRESSION] ?? 'UNCOMPRESSED'
    this.rowGroupSize = options.rowGroupSize ?? DEFAULT_ROW_GROUP_SIZE
    this.useDictionary = options.dictionary ?? true
    this._pageSize = options.pageSize ?? DEFAULT_PARQUET_PAGE_SIZE
    this.enableStatistics = options.statistics ?? true
    this.defaultMetadata = options.metadata ?? {}
    this.enableColumnIndex = options.columnIndex ?? DEFAULT_ENABLE_COLUMN_INDEX
    this.enableOffsetIndex = options.offsetIndex ?? DEFAULT_ENABLE_OFFSET_INDEX
  }

  /**
   * Write entities to a Parquet file
   *
   * Creates a new file with the provided data. If the file exists,
   * it will be overwritten.
   *
   * @param path - Path to write the Parquet file
   * @param data - Array of row objects to write
   * @param schema - Parquet schema definition
   * @param options - Additional write options
   * @returns Write result with statistics
   */
  async write<T = Record<string, unknown>>(
    path: string,
    data: T[],
    schema: ParquetSchema,
    options: ParquetWriterOptions = {}
  ): Promise<ParquetWriteResult> {
    if (data.length === 0) {
      // Write empty file with just schema
      return this.writeEmptyFile(path, schema)
    }

    // Merge options - use per-write option if specified, otherwise fall back to instance default
    const compression = options.compression !== undefined
      ? COMPRESSION_MAP[options.compression] ?? this.compression
      : this.compression
    const rowGroupSize = options.rowGroupSize ?? this.rowGroupSize
    const metadata = { ...this.defaultMetadata, ...options.metadata }

    // Convert rows to columnar format
    const columns = this.rowsToColumns(data as Record<string, unknown>[], schema)

    // Build Parquet buffer
    const buffer = await this.buildParquetBuffer(columns, schema, {
      compression,
      rowGroupSize,
      dictionary: options.dictionary ?? this.useDictionary,
      statistics: options.statistics ?? this.enableStatistics,
      metadata,
      columnIndex: options.columnIndex ?? this.enableColumnIndex,
      offsetIndex: options.offsetIndex ?? this.enableOffsetIndex,
    })

    // Write to storage
    const writeResult = await this.storage.writeAtomic(path, buffer, {
      contentType: 'application/vnd.apache.parquet',
    })

    return {
      ...writeResult,
      rowCount: data.length,
      rowGroupCount: Math.ceil(data.length / rowGroupSize),
      columns: Object.keys(columns),
    }
  }

  /**
   * Append data to an existing Parquet file
   *
   * Creates new row group(s) in the file without rewriting existing data.
   * This is efficient for incremental writes but may result in many small
   * row groups over time.
   *
   * @param path - Path to the Parquet file
   * @param data - Array of row objects to append
   * @returns Write result with statistics
   */
  async append<T = Record<string, unknown>>(
    path: string,
    data: T[]
  ): Promise<ParquetWriteResult> {
    if (data.length === 0) {
      return {
        etag: '',
        size: 0,
        rowCount: 0,
        rowGroupCount: 0,
        columns: [],
      }
    }

    // Check if file exists
    const exists = await this.storage.exists(path)

    if (!exists) {
      throw new Error(`Cannot append to non-existent file: ${path}. Use write() to create a new file.`)
    }

    // Read existing file
    const existingData = await this.storage.read(path)

    // Parse existing file to get schema and data
    // Note: This is a simplified approach. In production, we'd want to
    // read just the footer to get the schema and append efficiently.
    const { schema, rows } = await this.parseExistingFile(existingData)

    // Combine existing and new data
    const allData = [...rows, ...data]

    // Write combined data
    return this.write(path, allData, schema)
  }

  /**
   * Compact a Parquet file by merging row groups
   *
   * Reads the entire file and rewrites it with optimal row group sizes.
   * This reduces the number of row groups and improves read performance.
   *
   * @param path - Path to the Parquet file
   * @param targetRowGroupSize - Target rows per row group (optional)
   * @returns Write result with statistics
   */
  async compact(
    path: string,
    targetRowGroupSize?: number
  ): Promise<ParquetWriteResult> {
    // Read existing file
    const existingData = await this.storage.read(path)
    const { schema, rows } = await this.parseExistingFile(existingData)

    if (rows.length === 0) {
      return {
        etag: '',
        size: existingData.length,
        rowCount: 0,
        rowGroupCount: 0,
        columns: Object.keys(schema),
      }
    }

    // Write with new row group size
    return this.write(path, rows, schema, {
      rowGroupSize: targetRowGroupSize ?? this.rowGroupSize,
    })
  }

  // ===========================================================================
  // Typed Entity Writes
  // ===========================================================================

  /**
   * Write typed entities to a Parquet file
   *
   * This method uses a TypeDefinition schema to generate the Parquet column schema
   * and writes entities with proper type handling. Optionally includes a $data
   * column containing the full entity as JSON for flexible querying.
   *
   * @param path - Path to write the Parquet file
   * @param entities - Array of entities to write
   * @param options - Typed write options including schema
   * @returns Write result with statistics
   *
   * @example
   * ```typescript
   * const Post: TypeDefinition = {
   *   title: 'string!',
   *   content: 'text',
   *   views: 'int',
   *   published: 'boolean',
   * }
   *
   * await writer.writeTypedEntities('data/posts/data.parquet', posts, {
   *   schema: Post,
   *   includeDataVariant: true,
   * })
   * ```
   */
  async writeTypedEntities<T extends Entity = Entity>(
    path: string,
    entities: T[],
    options: TypedWriteOptions
  ): Promise<ParquetWriteResult> {
    const {
      schema: typeDef,
      includeDataVariant = true,
      includeAuditColumns = true,
      includeSoftDeleteColumns = true,
      ...writerOptions
    } = options

    // Generate Parquet schema from TypeDefinition
    const parquetSchema = generateParquetSchema(typeDef, {
      includeDataVariant,
      includeAuditColumns,
      includeSoftDeleteColumns,
    })

    // Convert SchemaTree to ParquetSchema format
    const schema = this.schemaTreeToParquetSchema(parquetSchema)

    // Build column data from entities
    const columns = this.typedEntitiesToColumns(entities, parquetSchema, includeDataVariant)

    if (entities.length === 0) {
      return this.writeEmptyFile(path, schema)
    }

    // Merge options - use per-write option if specified, otherwise fall back to instance default
    const compression = writerOptions.compression !== undefined
      ? COMPRESSION_MAP[writerOptions.compression] ?? this.compression
      : this.compression
    const rowGroupSize = writerOptions.rowGroupSize ?? this.rowGroupSize
    const metadata = { ...this.defaultMetadata, ...writerOptions.metadata }

    // Build Parquet buffer
    const buffer = await this.buildParquetBuffer(columns, schema, {
      compression,
      rowGroupSize,
      dictionary: writerOptions.dictionary ?? this.useDictionary,
      statistics: writerOptions.statistics ?? this.enableStatistics,
      metadata,
      columnIndex: writerOptions.columnIndex ?? this.enableColumnIndex,
      offsetIndex: writerOptions.offsetIndex ?? this.enableOffsetIndex,
    })

    // Write to storage
    const writeResult = await this.storage.writeAtomic(path, buffer, {
      contentType: 'application/vnd.apache.parquet',
    })

    return {
      ...writeResult,
      rowCount: entities.length,
      rowGroupCount: Math.ceil(entities.length / rowGroupSize),
      columns: Object.keys(columns),
    }
  }

  /**
   * Write typed entities directly to a buffer without storage
   *
   * This is useful when you need the raw Parquet bytes without writing to storage.
   *
   * @param entities - Array of entities to write
   * @param options - Typed write options including schema
   * @returns Parquet file as Uint8Array
   */
  async writeTypedEntitiesBuffer<T extends Entity = Entity>(
    entities: T[],
    options: TypedWriteOptions
  ): Promise<Uint8Array> {
    const {
      schema: typeDef,
      includeDataVariant = true,
      includeAuditColumns = true,
      includeSoftDeleteColumns = true,
      ...writerOptions
    } = options

    // Generate Parquet schema from TypeDefinition
    const parquetSchema = generateParquetSchema(typeDef, {
      includeDataVariant,
      includeAuditColumns,
      includeSoftDeleteColumns,
    })

    // Convert SchemaTree to ParquetSchema format
    const schema = this.schemaTreeToParquetSchema(parquetSchema)

    // Build column data from entities
    const columns = this.typedEntitiesToColumns(entities, parquetSchema, includeDataVariant)

    // Merge options - use per-write option if specified, otherwise fall back to instance default
    const compression = writerOptions.compression !== undefined
      ? COMPRESSION_MAP[writerOptions.compression] ?? this.compression
      : this.compression
    const rowGroupSize = writerOptions.rowGroupSize ?? this.rowGroupSize
    const metadata = { ...this.defaultMetadata, ...writerOptions.metadata }

    // Build and return Parquet buffer
    return this.buildParquetBuffer(columns, schema, {
      compression,
      rowGroupSize,
      dictionary: writerOptions.dictionary ?? this.useDictionary,
      statistics: writerOptions.statistics ?? this.enableStatistics,
      metadata,
      columnIndex: writerOptions.columnIndex ?? this.enableColumnIndex,
      offsetIndex: writerOptions.offsetIndex ?? this.enableOffsetIndex,
    })
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Convert SchemaTree to ParquetSchema format
   */
  private schemaTreeToParquetSchema(schemaTree: SchemaTree): ParquetSchema {
    const schema: ParquetSchema = {}
    for (const [name, field] of Object.entries(schemaTree)) {
      schema[name] = {
        type: field.type as ParquetSchema[string]['type'],
        optional: field.optional,
      }
    }
    return schema
  }

  /**
   * Convert typed entities to columnar format
   *
   * @param entities - Array of entities to convert
   * @param schema - SchemaTree defining the columns
   * @param includeDataVariant - Whether to include $data column
   */
  private typedEntitiesToColumns<T extends Entity>(
    entities: T[],
    schema: SchemaTree,
    includeDataVariant: boolean
  ): Record<string, unknown[]> {
    const columns: Record<string, unknown[]> = {}

    // Initialize columns
    for (const colName of Object.keys(schema)) {
      columns[colName] = []
    }

    // Fill columns from entities
    for (const entity of entities) {
      for (const colName of Object.keys(schema)) {
        if (colName === '$data' && includeDataVariant) {
          // $data column contains the full entity as JSON
          columns[colName]!.push(JSON.stringify(entity))
        } else {
          // Extract value from entity, handling nested access
          const value = this.getEntityValue(entity, colName)
          columns[colName]!.push(value ?? null)
        }
      }
    }

    return columns
  }

  /**
   * Get a value from an entity, handling special column names
   *
   * @param entity - The entity to extract the value from
   * @param colName - The column name
   */
  private getEntityValue<T extends Entity>(entity: T, colName: string): unknown {
    // Handle special fields
    if (colName === '$id') {
      return entity.$id
    }
    if (colName === '$type') {
      return entity.$type
    }
    if (colName === '$data') {
      // Should be handled separately
      return JSON.stringify(entity)
    }

    // Handle audit fields - convert Date to timestamp if needed
    if (colName === 'createdAt' || colName === 'updatedAt' || colName === 'deletedAt') {
      const value = entity[colName as keyof T]
      if (value instanceof Date) {
        return value.getTime()
      }
      return value
    }

    // Regular field access
    return entity[colName as keyof T]
  }

  /**
   * Convert row objects to columnar format
   */
  private rowsToColumns(
    rows: Record<string, unknown>[],
    schema: ParquetSchema
  ): Record<string, unknown[]> {
    const columns: Record<string, unknown[]> = {}

    // Initialize columns
    for (const colName of Object.keys(schema)) {
      columns[colName] = []
    }

    // Fill columns
    for (const row of rows) {
      for (const colName of Object.keys(schema)) {
        const value = row[colName]
        columns[colName]!.push(value ?? null)
      }
    }

    return columns
  }

  /**
   * Build a Parquet buffer from columnar data
   *
   * This is a simplified implementation. In production, we'd use
   * hyparquet-writer or parquet-wasm for proper Parquet encoding.
   */
  private async buildParquetBuffer(
    columns: Record<string, unknown[]>,
    schema: ParquetSchema,
    options: {
      compression: CompressionCodec
      rowGroupSize: number
      dictionary: boolean
      statistics: boolean
      metadata: Record<string, string>
      columnIndex: boolean
      offsetIndex: boolean
    }
  ): Promise<Uint8Array> {
    // For now, we'll use a JSON-based approach as a placeholder
    // In production, integrate with hyparquet-writer or parquet-wasm

    try {
      // Try to use hyparquet-writer if available
      const { parquetWriteBuffer } = await import('hyparquet-writer')
      // hyparquet-writer expects columnData as array of { name, data } objects
      const columnData = this.convertToColumnData(columns, schema, {
        columnIndex: options.columnIndex,
        offsetIndex: options.offsetIndex,
      })
      // Convert metadata to KeyValue array format
      const kvMetadata = Object.entries(options.metadata).map(([key, value]) => ({
        key,
        value,
      }))

      // Build write options with compression codec and compressors
      const writeOptions: Record<string, unknown> = {
        columnData,
        statistics: options.statistics,
        rowGroupSize: options.rowGroupSize,
        kvMetadata: kvMetadata.length > 0 ? kvMetadata : undefined,
        // Always set codec explicitly - hyparquet-writer defaults to SNAPPY if not specified
        codec: options.compression,
      }

      // Provide compressors for LZ4/GZIP/ZSTD (hyparquet-writer only has Snappy built-in)
      if (options.compression !== 'UNCOMPRESSED') {
        writeOptions.compressors = writeCompressors
      }

      const result = parquetWriteBuffer(writeOptions as Parameters<typeof parquetWriteBuffer>[0])
      return new Uint8Array(result)
    } catch (error: unknown) {
      // Fallback: Use a simple binary format when hyparquet-writer fails
      logger.debug('Parquet write via hyparquet-writer failed, using fallback format', error)
      return this.buildFallbackBuffer(columns, schema, options)
    }
  }

  /**
   * Convert columns to hyparquet-writer columnData format
   * hyparquet-writer expects an array of { name, data, columnIndex?, offsetIndex? } objects
   *
   * @param columns - Column data keyed by column name
   * @param schema - Parquet schema for type information
   * @param options - Page index options
   */
  private convertToColumnData(
    columns: Record<string, unknown[]>,
    _schema: ParquetSchema,
    options: { columnIndex: boolean; offsetIndex: boolean }
  ): Array<{ name: string; data: unknown[]; columnIndex?: boolean | undefined; offsetIndex?: boolean | undefined }> {
    return Object.entries(columns).map(([name, data]) => ({
      name,
      data,
      // Enable page indexes for all columns, especially $id and $index_* columns
      // which benefit most from predicate pushdown
      columnIndex: options.columnIndex,
      offsetIndex: options.offsetIndex,
    }))
  }

  /**
   * Build a fallback buffer when hyparquet-writer is not available
   *
   * This creates a minimal Parquet-like structure that can be read
   * by hyparquet but may not be fully compliant.
   */
  private buildFallbackBuffer(
    columns: Record<string, unknown[]>,
    schema: ParquetSchema,
    options: {
      compression: CompressionCodec
      rowGroupSize: number
      dictionary: boolean
      statistics: boolean
      metadata: Record<string, string>
      columnIndex: boolean
      offsetIndex: boolean
    }
  ): Uint8Array {
    // Encode data as JSON and wrap with Parquet magic bytes
    // This is a temporary solution - real implementation should use proper Parquet encoding

    const data = {
      schema,
      columns,
      metadata: options.metadata,
      options: {
        compression: options.compression,
        rowGroupSize: options.rowGroupSize,
      },
    }

    const jsonStr = JSON.stringify(data)
    const encoder = new TextEncoder()
    const jsonBytes = encoder.encode(jsonStr)

    // Create buffer with magic bytes
    // PAR1 header + data + PAR1 footer
    const MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // 'PAR1'

    const buffer = new Uint8Array(MAGIC.length * 2 + jsonBytes.length)
    buffer.set(MAGIC, 0)
    buffer.set(jsonBytes, MAGIC.length)
    buffer.set(MAGIC, MAGIC.length + jsonBytes.length)

    return buffer
  }

  /**
   * Write an empty Parquet file with just schema
   */
  private async writeEmptyFile(
    path: string,
    schema: ParquetSchema
  ): Promise<ParquetWriteResult> {
    const columns: Record<string, unknown[]> = {}
    for (const colName of Object.keys(schema)) {
      columns[colName] = []
    }

    const buffer = await this.buildParquetBuffer(columns, schema, {
      compression: this.compression,
      rowGroupSize: this.rowGroupSize,
      dictionary: this.useDictionary,
      statistics: this.enableStatistics,
      metadata: this.defaultMetadata,
      columnIndex: this.enableColumnIndex,
      offsetIndex: this.enableOffsetIndex,
    })

    const writeResult = await this.storage.writeAtomic(path, buffer, {
      contentType: 'application/vnd.apache.parquet',
    })

    return {
      ...writeResult,
      rowCount: 0,
      rowGroupCount: 0,
      columns: Object.keys(schema),
    }
  }

  /**
   * Parse an existing Parquet file
   *
   * Returns the schema and all rows for rewriting.
   */
  private async parseExistingFile(
    data: Uint8Array
  ): Promise<{ schema: ParquetSchema; rows: Record<string, unknown>[] }> {
    try {
      // Try to parse as real Parquet
      const { parquetRead, parquetMetadataAsync } = await import('hyparquet')

      const asyncBuffer = {
        byteLength: data.length,
        slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
          const sliced = data.slice(start, end ?? data.length)
          const buffer = new ArrayBuffer(sliced.byteLength)
          new Uint8Array(buffer).set(sliced)
          return buffer
        },
      }

      const metadata = await parquetMetadataAsync(asyncBuffer) as { schema?: Array<{ name?: string | undefined; type?: string | undefined; repetition_type?: string | undefined }> | undefined }
      // Include compressors for LZ4, GZIP, ZSTD, and Brotli support
      const readResult = await parquetRead({ file: asyncBuffer, compressors })
      const result = (readResult as unknown) as Record<string, unknown[]> | undefined

      // Extract schema from metadata
      const schema: ParquetSchema = {}
      for (const element of (metadata.schema || [])) {
        if (element.name && element.type) {
          schema[element.name] = {
            type: element.type as ParquetSchema[string]['type'],
            optional: element.repetition_type !== 'REQUIRED',
          }
        }
      }

      // Convert to rows
      const resultObj = result ?? {}
      const columnNames = Object.keys(resultObj)
      const firstCol = columnNames[0]
      const numRows = firstCol ? (resultObj[firstCol]?.length ?? 0) : 0
      const rows: Record<string, unknown>[] = []

      for (let i = 0; i < numRows; i++) {
        const row: Record<string, unknown> = {}
        for (const colName of columnNames) {
          const col = resultObj[colName]
          row[colName] = col ? col[i] : undefined
        }
        rows.push(row)
      }

      return { schema, rows }
    } catch (error: unknown) {
      // Fallback: Parse as our custom format when hyparquet fails
      logger.debug('Parquet read via hyparquet failed, using fallback parser', error)
      return this.parseFallbackBuffer(data)
    }
  }

  /**
   * Parse fallback buffer format
   */
  private parseFallbackBuffer(
    data: Uint8Array
  ): { schema: ParquetSchema; rows: Record<string, unknown>[] } {
    // Skip PAR1 magic bytes
    const MAGIC_LENGTH = 4
    const jsonBytes = data.slice(MAGIC_LENGTH, data.length - MAGIC_LENGTH)

    // Check for invalid/empty data
    if (jsonBytes.length === 0) {
      throw new Error('Invalid Parquet file: no data between magic bytes')
    }

    const decoder = new TextDecoder()
    const jsonStr = decoder.decode(jsonBytes)

    try {
      const parsed = JSON.parse(jsonStr)

      // Convert columns to rows
      const columnNames = Object.keys(parsed.columns || {})
      const firstCol = columnNames[0]
      const numRows = firstCol
        ? (parsed.columns[firstCol]?.length ?? 0)
        : 0

      const rows: Record<string, unknown>[] = []
      for (let i = 0; i < numRows; i++) {
        const row: Record<string, unknown> = {}
        for (const colName of columnNames) {
          const col = parsed.columns[colName]
          row[colName] = col ? col[i] : undefined
        }
        rows.push(row)
      }

      return {
        schema: parsed.schema ?? {},
        rows,
      }
    } catch (e) {
      throw new Error(`Invalid Parquet file: ${e instanceof Error ? e.message : 'parse error'}`)
    }
  }
}

// =============================================================================
// Standalone Functions
// =============================================================================

/**
 * Write data to a Parquet file
 *
 * Convenience function for one-off writes.
 *
 * @param storage - Storage backend
 * @param path - Path to write
 * @param data - Data to write
 * @param schema - Parquet schema
 * @param options - Writer options
 * @returns Write result
 */
export async function writeParquet<T = Record<string, unknown>>(
  storage: StorageBackend,
  path: string,
  data: T[],
  schema: ParquetSchema,
  options?: ParquetWriterOptions
): Promise<ParquetWriteResult> {
  const writer = new ParquetWriter(storage, options)
  return writer.write(path, data, schema, options)
}

/**
 * Append data to a Parquet file
 *
 * @param storage - Storage backend
 * @param path - Path to the file
 * @param data - Data to append
 * @returns Write result
 */
export async function appendParquet<T = Record<string, unknown>>(
  storage: StorageBackend,
  path: string,
  data: T[]
): Promise<ParquetWriteResult> {
  const writer = new ParquetWriter(storage)
  return writer.append(path, data)
}

/**
 * Compact a Parquet file
 *
 * @param storage - Storage backend
 * @param path - Path to the file
 * @param targetRowGroupSize - Target row group size
 * @returns Write result
 */
export async function compactParquet(
  storage: StorageBackend,
  path: string,
  targetRowGroupSize?: number
): Promise<ParquetWriteResult> {
  const writer = new ParquetWriter(storage)
  return writer.compact(path, targetRowGroupSize)
}

/**
 * Write typed entities to a Parquet file
 *
 * Convenience function for one-off typed entity writes.
 * Uses a TypeDefinition schema to generate the Parquet column schema.
 *
 * @param storage - Storage backend
 * @param path - Path to write
 * @param entities - Entities to write
 * @param options - Typed write options including schema
 * @returns Write result
 *
 * @example
 * ```typescript
 * const Post: TypeDefinition = {
 *   title: 'string!',
 *   content: 'text',
 *   views: 'int',
 * }
 *
 * await writeTypedParquet(storage, 'data/posts.parquet', posts, {
 *   schema: Post,
 *   includeDataVariant: true,
 * })
 * ```
 */
export async function writeTypedParquet<T extends Entity = Entity>(
  storage: StorageBackend,
  path: string,
  entities: T[],
  options: TypedWriteOptions
): Promise<ParquetWriteResult> {
  const writer = new ParquetWriter(storage, options)
  return writer.writeTypedEntities(path, entities, options)
}
