/**
 * Minimal Parquet Reader for Cloudflare Snippets
 *
 * This is a stripped-down Parquet reader optimized for:
 * - Small bundle size (<5KB minified)
 * - Fast cold starts
 * - Reading from static assets or R2
 *
 * Supports:
 * - Footer parsing
 * - Uncompressed data
 * - Basic column types (INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY for strings)
 *
 * Does NOT support:
 * - Compression (GZIP, SNAPPY, LZ4, ZSTD)
 * - Nested schemas
 * - Repetition levels
 * - Page-level encoding
 *
 * For full Parquet support, use hyparquet directly.
 */

import type { AsyncBuffer, ParquetFooter, Row, SchemaElement, RowGroupMetadata, ColumnChunkMetadata } from './types'

// =============================================================================
// Constants
// =============================================================================

const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // PAR1
const FOOTER_SIZE_BYTES = 8 // 4 bytes footer length + 4 bytes magic

// Thrift Compact Protocol type IDs
// https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
const THRIFT_STOP = 0
const THRIFT_BOOL_TRUE = 1
const THRIFT_BOOL_FALSE = 2
const THRIFT_BYTE = 3
const THRIFT_I16 = 4
const THRIFT_I32 = 5
const THRIFT_I64 = 6
const THRIFT_DOUBLE = 7
const THRIFT_BINARY = 8  // Binary/String
const THRIFT_LIST = 9
const THRIFT_SET = 10
const THRIFT_MAP = 11
const THRIFT_STRUCT = 12

// =============================================================================
// Thrift Decoder
// =============================================================================

/**
 * Minimal Thrift compact protocol decoder
 */
class ThriftDecoder {
  private view: DataView
  private pos = 0
  private lastFieldId = 0

  constructor(buffer: ArrayBuffer) {
    this.view = new DataView(buffer)
  }

  /** Read a varint */
  readVarint(): number {
    let result = 0
    let shift = 0
    while (true) {
      const byte = this.view.getUint8(this.pos++)
      result |= (byte & 0x7f) << shift
      if ((byte & 0x80) === 0) break
      shift += 7
    }
    return result
  }

  /** Read a zigzag-encoded varint */
  readZigzag(): number {
    const n = this.readVarint()
    return (n >>> 1) ^ -(n & 1)
  }

  /** Read a field header, returns [fieldId, type] */
  readFieldHeader(): [number, number] {
    const byte = this.view.getUint8(this.pos++)
    if (byte === THRIFT_STOP) return [0, THRIFT_STOP]

    const type = byte & 0x0f
    let fieldIdDelta = (byte >> 4) & 0x0f

    let fieldId: number
    if (fieldIdDelta === 0) {
      fieldId = this.readZigzag()
    } else {
      fieldId = this.lastFieldId + fieldIdDelta
    }

    this.lastFieldId = fieldId
    return [fieldId, type]
  }

  /** Read a string (length-prefixed) */
  readString(): string {
    const length = this.readVarint()
    const bytes = new Uint8Array(this.view.buffer, this.pos, length)
    this.pos += length
    return new TextDecoder().decode(bytes)
  }

  /** Read bytes (length-prefixed) */
  readBytes(): Uint8Array {
    const length = this.readVarint()
    const bytes = new Uint8Array(this.view.buffer, this.pos, length)
    this.pos += length
    return bytes
  }

  /** Read a single byte */
  readByte(): number {
    return this.view.getUint8(this.pos++)
  }

  /** Read i32 */
  readI32(): number {
    return this.readZigzag()
  }

  /** Read a varint as bigint */
  readVarBigInt(): bigint {
    let result = 0n
    let shift = 0n
    while (true) {
      const byte = this.view.getUint8(this.pos++)
      result |= BigInt(byte & 0x7f) << shift
      if ((byte & 0x80) === 0) break
      shift += 7n
    }
    return result
  }

  /** Read i64 as BigInt (zigzag encoded) */
  readI64(): bigint {
    const zigzag = this.readVarBigInt()
    return (zigzag >> 1n) ^ -(zigzag & 1n)
  }

  /** Read i64 as number (lossy for large values) */
  readI64AsNumber(): number {
    return Number(this.readI64())
  }

  /** Skip a field value */
  skip(type: number): void {
    switch (type) {
      case THRIFT_STOP:
        // Nothing to skip
        break
      case THRIFT_BOOL_TRUE:
      case THRIFT_BOOL_FALSE:
        // Boolean values are encoded in the type, no extra bytes
        break
      case THRIFT_BYTE:
        this.pos += 1
        break
      case THRIFT_I16:
      case THRIFT_I32:
        this.readVarint()
        break
      case THRIFT_I64:
        this.readVarBigInt()  // i64 is a single zigzag varint
        break
      case THRIFT_DOUBLE:
        // Double is fixed 8 bytes (not varint)
        this.pos += 8
        break
      case THRIFT_BINARY:
        const len = this.readVarint()
        this.pos += len
        break
      case THRIFT_STRUCT:
        this.skipStruct()
        break
      case THRIFT_MAP:
        // Map: size varint, then key-value types byte, then entries
        const mapSize = this.readVarint()
        if (mapSize > 0) {
          const kvTypes = this.view.getUint8(this.pos++)
          const keyType = (kvTypes >> 4) & 0x0f
          const valType = kvTypes & 0x0f
          for (let i = 0; i < mapSize; i++) {
            this.skip(keyType)
            this.skip(valType)
          }
        }
        break
      case THRIFT_SET:
      case THRIFT_LIST:
        const header = this.view.getUint8(this.pos++)
        let size = (header >> 4) & 0x0f
        if (size === 15) size = this.readVarint()
        const elemType = header & 0x0f
        for (let i = 0; i < size; i++) {
          this.skip(elemType)
        }
        break
      default:
        throw new Error(`Unknown Thrift type: ${type}`)
    }
  }

  /** Skip an entire struct */
  skipStruct(): void {
    const savedLastFieldId = this.lastFieldId
    this.lastFieldId = 0
    while (true) {
      const [, type] = this.readFieldHeader()
      if (type === THRIFT_STOP) break
      this.skip(type)
    }
    this.lastFieldId = savedLastFieldId
  }

  /** Reset field tracking (for nested structs), returns previous value */
  resetFieldId(): number {
    const saved = this.lastFieldId
    this.lastFieldId = 0
    return saved
  }

  /** Restore field tracking */
  restoreFieldId(saved: number): void {
    this.lastFieldId = saved
  }

  /** Get current position */
  get position(): number {
    return this.pos
  }
}

// =============================================================================
// Footer Parser
// =============================================================================

/**
 * Parse Parquet file footer
 */
export async function parseFooter(buffer: AsyncBuffer): Promise<ParquetFooter> {
  const fileSize = buffer.byteLength

  // Read footer size and magic
  const tailBytes = await buffer.slice(fileSize - FOOTER_SIZE_BYTES, fileSize)
  const tailView = new DataView(tailBytes)

  // Verify magic
  const magic = new Uint8Array(tailBytes, 4, 4)
  for (let i = 0; i < 4; i++) {
    if (magic[i] !== PARQUET_MAGIC[i]) {
      throw new Error('Invalid Parquet file: bad magic')
    }
  }

  // Read footer length
  const footerLength = tailView.getUint32(0, true) // little-endian

  // Read footer data
  const footerStart = fileSize - FOOTER_SIZE_BYTES - footerLength
  const footerBytes = await buffer.slice(footerStart, footerStart + footerLength)

  // Parse Thrift-encoded FileMetaData
  const decoder = new ThriftDecoder(footerBytes)
  return parseFileMetaData(decoder)
}

/**
 * Parse FileMetaData Thrift struct
 */
function parseFileMetaData(decoder: ThriftDecoder): ParquetFooter {
  const result: ParquetFooter = {
    version: 1,
    schema: [],
    numRows: 0,
    rowGroups: [],
  }

  decoder.resetFieldId()
  while (true) {
    const [fieldId, type] = decoder.readFieldHeader()
    if (type === THRIFT_STOP) break

    switch (fieldId) {
      case 1: // version
        result.version = decoder.readI32()
        break
      case 2: // schema
        result.schema = parseSchemaList(decoder)
        break
      case 3: // num_rows
        result.numRows = decoder.readI64AsNumber()
        break
      case 4: // row_groups
        result.rowGroups = parseRowGroupList(decoder)
        break
      case 5: // key_value_metadata
        result.keyValueMetadata = parseKeyValueList(decoder)
        break
      default:
        decoder.skip(type)
    }
  }

  return result
}

/**
 * Read a list header and return [size, elementType]
 */
function readListHeader(decoder: ThriftDecoder): [number, number] {
  const header = decoder.readByte()
  let size = (header >> 4) & 0x0f
  const elemType = header & 0x0f
  if (size === 15) size = decoder.readVarint()
  return [size, elemType]
}

/**
 * Parse schema element list
 */
function parseSchemaList(decoder: ThriftDecoder): SchemaElement[] {
  const [size] = readListHeader(decoder)

  const elements: SchemaElement[] = []
  for (let i = 0; i < size; i++) {
    elements.push(parseSchemaElement(decoder))
  }
  return elements
}

/**
 * Parse a single schema element
 */
function parseSchemaElement(decoder: ThriftDecoder): SchemaElement {
  const elem: SchemaElement = { name: '' }

  const saved = decoder.resetFieldId()
  while (true) {
    const [fieldId, type] = decoder.readFieldHeader()
    if (type === THRIFT_STOP) break

    switch (fieldId) {
      case 1: // type (PhysicalType)
        elem.type = parsePhysicalType(decoder.readI32())
        break
      case 4: // name
        elem.name = decoder.readString()
        break
      case 5: // num_children
        elem.numChildren = decoder.readI32()
        break
      default:
        decoder.skip(type)
    }
  }
  decoder.restoreFieldId(saved)

  return elem
}

/**
 * Convert physical type enum to string
 */
function parsePhysicalType(value: number): SchemaElement['type'] {
  const types: SchemaElement['type'][] = [
    'BOOLEAN', 'INT32', 'INT64', 'INT96', 'FLOAT', 'DOUBLE', 'BYTE_ARRAY', 'FIXED_LEN_BYTE_ARRAY'
  ]
  return types[value]
}

/**
 * Parse row group list
 */
function parseRowGroupList(decoder: ThriftDecoder): RowGroupMetadata[] {
  const [size] = readListHeader(decoder)

  const groups: RowGroupMetadata[] = []
  for (let i = 0; i < size; i++) {
    groups.push(parseRowGroup(decoder))
  }
  return groups
}

/**
 * Parse a single row group
 */
function parseRowGroup(decoder: ThriftDecoder): RowGroupMetadata {
  const group: RowGroupMetadata = {
    numRows: 0,
    totalByteSize: 0,
    columns: [],
  }

  const saved = decoder.resetFieldId()
  while (true) {
    const [fieldId, type] = decoder.readFieldHeader()
    if (type === THRIFT_STOP) break

    switch (fieldId) {
      case 1: // columns
        group.columns = parseColumnChunkList(decoder)
        break
      case 2: // total_byte_size
        group.totalByteSize = decoder.readI64AsNumber()
        break
      case 3: // num_rows
        group.numRows = decoder.readI64AsNumber()
        break
      default:
        decoder.skip(type)
    }
  }
  decoder.restoreFieldId(saved)

  return group
}

/**
 * Parse column chunk list
 */
function parseColumnChunkList(decoder: ThriftDecoder): ColumnChunkMetadata[] {
  const [size] = readListHeader(decoder)

  const columns: ColumnChunkMetadata[] = []
  for (let i = 0; i < size; i++) {
    columns.push(parseColumnChunk(decoder))
  }
  return columns
}

/**
 * Parse a single column chunk
 */
function parseColumnChunk(decoder: ThriftDecoder): ColumnChunkMetadata {
  const chunk: ColumnChunkMetadata = {
    path: [],
    fileOffset: 0,
    numValues: 0,
    totalCompressedSize: 0,
    totalUncompressedSize: 0,
  }

  const saved = decoder.resetFieldId()
  while (true) {
    const [fieldId, type] = decoder.readFieldHeader()
    if (type === THRIFT_STOP) break

    switch (fieldId) {
      case 2: // file_offset (i64)
        chunk.fileOffset = decoder.readI64AsNumber()
        break
      case 3: // meta_data (ColumnMetaData struct)
        parseColumnMetaData(decoder, chunk)
        break
      default:
        decoder.skip(type)
    }
  }
  decoder.restoreFieldId(saved)

  return chunk
}

/**
 * Parse column metadata into chunk
 */
function parseColumnMetaData(decoder: ThriftDecoder, chunk: ColumnChunkMetadata): void {
  const saved = decoder.resetFieldId()
  while (true) {
    const [fieldId, type] = decoder.readFieldHeader()
    if (type === THRIFT_STOP) break

    switch (fieldId) {
      case 3: // path_in_schema
        chunk.path = parseStringList(decoder)
        break
      case 5: // num_values
        chunk.numValues = decoder.readI64AsNumber()
        break
      case 6: // total_uncompressed_size
        chunk.totalUncompressedSize = decoder.readI64AsNumber()
        break
      case 7: // total_compressed_size
        chunk.totalCompressedSize = decoder.readI64AsNumber()
        break
      case 9: // data_page_offset
        chunk.fileOffset = decoder.readI64AsNumber()
        break
      default:
        decoder.skip(type)
    }
  }
  decoder.restoreFieldId(saved)
}

/**
 * Parse string list
 */
function parseStringList(decoder: ThriftDecoder): string[] {
  const [size] = readListHeader(decoder)

  const strings: string[] = []
  for (let i = 0; i < size; i++) {
    strings.push(decoder.readString())
  }
  return strings
}

/**
 * Parse key-value metadata list
 */
function parseKeyValueList(decoder: ThriftDecoder): { key: string; value?: string }[] {
  const [size] = readListHeader(decoder)

  const kvs: { key: string; value?: string }[] = []
  for (let i = 0; i < size; i++) {
    const kv: { key: string; value?: string } = { key: '' }
    const saved = decoder.resetFieldId()
    while (true) {
      const [fieldId, type] = decoder.readFieldHeader()
      if (type === THRIFT_STOP) break
      switch (fieldId) {
        case 1:
          kv.key = decoder.readString()
          break
        case 2:
          kv.value = decoder.readString()
          break
        default:
          decoder.skip(type)
      }
    }
    decoder.restoreFieldId(saved)
    kvs.push(kv)
  }
  return kvs
}

// =============================================================================
// Data Page Reader
// =============================================================================

/**
 * Read all rows from a Parquet file (uncompressed only)
 *
 * WARNING: This is a minimal implementation for Snippets.
 * It only supports uncompressed PLAIN encoding.
 * For compressed files or complex encodings, use hyparquet.
 */
export async function readRows(
  buffer: AsyncBuffer,
  footer: ParquetFooter,
  columns?: string[]
): Promise<Row[]> {
  const rows: Row[] = []

  // Get column names from schema (skip root element)
  const columnNames = footer.schema.slice(1).map(s => s.name)
  const selectedColumns = columns ?? columnNames

  // Build column index map
  const columnIndices = new Map<string, number>()
  columnNames.forEach((name, idx) => columnIndices.set(name, idx))

  // Read each row group
  for (const rowGroup of footer.rowGroups) {
    const columnData = new Map<string, unknown[]>()

    // Read selected columns
    for (const colName of selectedColumns) {
      const colIdx = columnIndices.get(colName)
      if (colIdx === undefined) continue

      const chunkMeta = rowGroup.columns[colIdx]
      if (!chunkMeta) continue

      // Find schema element for type info
      const schemaElem = footer.schema[colIdx + 1] // +1 to skip root

      // Read column data
      const colValues = await readColumnChunk(buffer, chunkMeta, schemaElem, rowGroup.numRows)
      columnData.set(colName, colValues)
    }

    // Convert column data to rows
    for (let i = 0; i < rowGroup.numRows; i++) {
      const row: Row = {}
      for (const [colName, values] of columnData) {
        row[colName] = values[i]
      }
      rows.push(row)
    }
  }

  return rows
}

/**
 * Read a single column chunk
 */
async function readColumnChunk(
  buffer: AsyncBuffer,
  meta: ColumnChunkMetadata,
  schema: SchemaElement | undefined,
  numRows: number
): Promise<unknown[]> {
  // Read raw page data
  const pageData = await buffer.slice(
    meta.fileOffset,
    meta.fileOffset + meta.totalCompressedSize
  )

  // For uncompressed PLAIN encoding, data follows the page header
  // This is a simplified reader - skip page headers and read values directly
  const view = new DataView(pageData)

  // Skip Thrift-encoded page header (simplified: scan for data start)
  let offset = 0

  // Read page header to find data start
  // PageHeader has: type(i32), uncompressed_size(i32), compressed_size(i32)
  // For PLAIN uncompressed, data follows immediately after header

  // Simple heuristic: scan for start of data
  // In reality, we'd properly parse the PageHeader
  // For this minimal implementation, we try to detect data start

  // Parse data based on physical type
  const values: unknown[] = []
  const physicalType = schema?.type

  try {
    // Attempt to skip page header (Thrift compact)
    // This is fragile but works for simple cases
    const headerDecoder = new ThriftDecoder(pageData)

    // Read page type
    const [, typeField] = headerDecoder.readFieldHeader()
    if (typeField !== THRIFT_STOP) headerDecoder.skip(typeField)

    // Read uncompressed size
    const [, uncompField] = headerDecoder.readFieldHeader()
    if (uncompField !== THRIFT_STOP) headerDecoder.skip(uncompField)

    // Read compressed size
    const [, compField] = headerDecoder.readFieldHeader()
    if (compField !== THRIFT_STOP) headerDecoder.skip(compField)

    // Skip remaining header fields
    headerDecoder.skipStruct()

    offset = headerDecoder.position

    // Now read values
    for (let i = 0; i < numRows && offset < pageData.byteLength; i++) {
      switch (physicalType) {
        case 'BOOLEAN':
          // Bit-packed booleans
          const byteIdx = Math.floor(i / 8)
          const bitIdx = i % 8
          if (offset + byteIdx < pageData.byteLength) {
            const byte = view.getUint8(offset + byteIdx)
            values.push((byte & (1 << bitIdx)) !== 0)
          }
          if (i === numRows - 1) offset += Math.ceil(numRows / 8)
          break

        case 'INT32':
          values.push(view.getInt32(offset, true))
          offset += 4
          break

        case 'INT64':
          // Read as BigInt then convert to Number
          const low = view.getUint32(offset, true)
          const high = view.getInt32(offset + 4, true)
          values.push(Number(BigInt(low) | (BigInt(high) << 32n)))
          offset += 8
          break

        case 'FLOAT':
          values.push(view.getFloat32(offset, true))
          offset += 4
          break

        case 'DOUBLE':
          values.push(view.getFloat64(offset, true))
          offset += 8
          break

        case 'BYTE_ARRAY':
          // Length-prefixed string
          const len = view.getUint32(offset, true)
          offset += 4
          const bytes = new Uint8Array(pageData, offset, len)
          values.push(new TextDecoder().decode(bytes))
          offset += len
          break

        default:
          // Unknown type, skip
          values.push(null)
          break
      }
    }
  } catch {
    // If parsing fails, return empty values
    for (let i = 0; i < numRows; i++) {
      values.push(null)
    }
  }

  return values
}

// =============================================================================
// Index Support
// =============================================================================

/**
 * Pre-built index for fast lookups
 *
 * Store alongside Parquet file as index.json:
 * { "byId": { "abc123": 0, "def456": 1, ... } }
 */
export interface ParquetIndex {
  /** Map from ID value to row number */
  byId?: Record<string, number>
  /** Map from other fields to row numbers */
  [field: string]: Record<string, number | number[]> | undefined
}

/**
 * Look up a row by index
 *
 * Much faster than scanning for single-row lookups.
 */
export async function lookupByIndex(
  buffer: AsyncBuffer,
  footer: ParquetFooter,
  index: ParquetIndex,
  field: string,
  value: string
): Promise<Row | null> {
  const fieldIndex = index[field]
  if (!fieldIndex) return null

  const rowNumber = fieldIndex[value]
  if (rowNumber === undefined) return null

  // Find which row group contains this row
  let currentRow = 0
  for (const rowGroup of footer.rowGroups) {
    if (typeof rowNumber === 'number' && rowNumber < currentRow + rowGroup.numRows) {
      // Row is in this row group
      const rows = await readRows(buffer, {
        ...footer,
        rowGroups: [rowGroup],
      })
      const localIdx = rowNumber - currentRow
      return rows[localIdx] ?? null
    }
    currentRow += rowGroup.numRows
  }

  return null
}

// =============================================================================
// Utility
// =============================================================================

/**
 * Create an AsyncBuffer from a Response
 */
export function responseToAsyncBuffer(response: Response, size: number): AsyncBuffer {
  return {
    byteLength: size,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const effectiveEnd = end ?? size

      // Clone the response for range request
      const rangeResponse = await fetch(response.url, {
        headers: {
          Range: `bytes=${start}-${effectiveEnd - 1}`,
        },
      })

      return rangeResponse.arrayBuffer()
    },
  }
}

/**
 * Create an AsyncBuffer from an ArrayBuffer (for small files)
 */
export function arrayBufferToAsyncBuffer(buffer: ArrayBuffer): AsyncBuffer {
  return {
    byteLength: buffer.byteLength,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      return buffer.slice(start, end)
    },
  }
}
