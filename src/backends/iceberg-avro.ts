/**
 * Avro Encoding/Decoding for Iceberg Manifest Files
 *
 * This module provides Avro binary encoding and decoding for Iceberg manifest files
 * and manifest lists. This is required for interoperability with DuckDB, Spark,
 * Snowflake, and other tools that expect Avro-encoded manifests.
 *
 * @see https://iceberg.apache.org/spec/
 * @see https://avro.apache.org/docs/current/specification/
 */

import {
  AvroEncoder,
  AvroFileWriter,
  createManifestEntrySchema,
  createManifestListSchema,
  type ManifestEntry,
  type ManifestFile,
} from '@dotdo/iceberg'

// Avro magic bytes for detecting Avro container files
const AVRO_MAGIC = new Uint8Array([0x4f, 0x62, 0x6a, 0x01]) // 'Obj' + version 1

/**
 * Check if data starts with Avro magic bytes
 */
export function isAvroFormat(data: Uint8Array): boolean {
  if (data.length < 4) return false
  return (
    data[0] === AVRO_MAGIC[0] &&
    data[1] === AVRO_MAGIC[1] &&
    data[2] === AVRO_MAGIC[2] &&
    data[3] === AVRO_MAGIC[3]
  )
}

// ===========================================================================
// Helper functions for encoding optional values
// ===========================================================================

function writeOptionalLong(encoder: AvroEncoder, value: number | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeLong(value)
  }
}

function writeOptionalInt(encoder: AvroEncoder, value: number | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeInt(value)
  }
}

function writeOptionalKeyValueArray<T>(
  encoder: AvroEncoder,
  value: T[] | null | undefined,
  writeElement: (item: T) => void
): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeArray(value, writeElement)
  }
}

function writeOptionalBytes(encoder: AvroEncoder, value: Uint8Array | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeBytes(value)
  }
}

function writeOptionalLongArray(encoder: AvroEncoder, value: number[] | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeArray(value, (v) => encoder.writeLong(v))
  }
}

function writeOptionalIntArray(encoder: AvroEncoder, value: number[] | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeArray(value, (v) => encoder.writeInt(v))
  }
}

function writeOptionalString(encoder: AvroEncoder, value: string | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeString(value)
  }
}

function writeOptionalBoolean(encoder: AvroEncoder, value: boolean | null | undefined): void {
  if (value === null || value === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeBoolean(value)
  }
}

// ===========================================================================
// Encoding functions (inline implementations since not exported from @dotdo/iceberg)
// ===========================================================================

interface EncodableDataFile {
  content: number
  file_path: string
  file_format: string
  partition: Record<string, unknown>
  record_count: number
  file_size_in_bytes: number
  column_sizes?: Array<{ key: number; value: number }> | null
  value_counts?: Array<{ key: number; value: number }> | null
  null_value_counts?: Array<{ key: number; value: number }> | null
  nan_value_counts?: Array<{ key: number; value: number }> | null
  lower_bounds?: Array<{ key: number; value: Uint8Array }> | null
  upper_bounds?: Array<{ key: number; value: Uint8Array }> | null
  key_metadata?: Uint8Array | null
  split_offsets?: number[] | null
  equality_ids?: number[] | null
  sort_order_id?: number | null
  first_row_id?: number | null
  referenced_data_file?: string | null
  content_offset?: number | null
  content_size_in_bytes?: number | null
}

interface EncodableManifestEntry {
  status: number
  snapshot_id: number | null
  sequence_number: number | null
  file_sequence_number: number | null
  data_file: EncodableDataFile
}

interface EncodableManifestListEntry {
  manifest_path: string
  manifest_length: number
  partition_spec_id: number
  content: number
  sequence_number: number
  min_sequence_number: number
  added_snapshot_id: number
  added_files_count: number
  existing_files_count: number
  deleted_files_count: number
  added_rows_count: number
  existing_rows_count: number
  deleted_rows_count: number
  partitions?: Array<{
    contains_null: boolean
    contains_nan?: boolean | null
    lower_bound?: Uint8Array | null
    upper_bound?: Uint8Array | null
  }> | null
  first_row_id?: number | null
}

interface PartitionFieldDef {
  name: string
  type: string
}

function writePartitionValue(encoder: AvroEncoder, value: unknown, type: string): void {
  switch (type) {
    case 'int':
      encoder.writeInt(value as number)
      break
    case 'long':
      encoder.writeLong(value as number)
      break
    case 'string':
      encoder.writeString(value as string)
      break
    case 'boolean':
      encoder.writeBoolean(value as boolean)
      break
    case 'float':
      encoder.writeFloat(value as number)
      break
    case 'double':
      encoder.writeDouble(value as number)
      break
    case 'bytes':
      encoder.writeBytes(value as Uint8Array)
      break
    default:
      encoder.writeString(String(value))
  }
}

function encodeDataFile(
  encoder: AvroEncoder,
  dataFile: EncodableDataFile,
  partitionFields: PartitionFieldDef[]
): void {
  // content (int)
  encoder.writeInt(dataFile.content)
  // file_path (string)
  encoder.writeString(dataFile.file_path)
  // file_format (string)
  encoder.writeString(dataFile.file_format)
  // partition (record)
  for (const field of partitionFields) {
    const value = dataFile.partition[field.name]
    if (value === null || value === undefined) {
      encoder.writeUnionIndex(0) // null
    } else {
      encoder.writeUnionIndex(1) // non-null
      writePartitionValue(encoder, value, field.type)
    }
  }
  // record_count (long)
  encoder.writeLong(dataFile.record_count)
  // file_size_in_bytes (long)
  encoder.writeLong(dataFile.file_size_in_bytes)
  // column_sizes (optional map as array)
  writeOptionalKeyValueArray(encoder, dataFile.column_sizes, (kv) => {
    encoder.writeInt(kv.key)
    encoder.writeLong(kv.value)
  })
  // value_counts
  writeOptionalKeyValueArray(encoder, dataFile.value_counts, (kv) => {
    encoder.writeInt(kv.key)
    encoder.writeLong(kv.value)
  })
  // null_value_counts
  writeOptionalKeyValueArray(encoder, dataFile.null_value_counts, (kv) => {
    encoder.writeInt(kv.key)
    encoder.writeLong(kv.value)
  })
  // nan_value_counts
  writeOptionalKeyValueArray(encoder, dataFile.nan_value_counts, (kv) => {
    encoder.writeInt(kv.key)
    encoder.writeLong(kv.value)
  })
  // lower_bounds
  writeOptionalKeyValueArray(encoder, dataFile.lower_bounds, (kv) => {
    encoder.writeInt(kv.key)
    encoder.writeBytes(kv.value)
  })
  // upper_bounds
  writeOptionalKeyValueArray(encoder, dataFile.upper_bounds, (kv) => {
    encoder.writeInt(kv.key)
    encoder.writeBytes(kv.value)
  })
  // key_metadata
  writeOptionalBytes(encoder, dataFile.key_metadata)
  // split_offsets
  writeOptionalLongArray(encoder, dataFile.split_offsets)
  // equality_ids
  writeOptionalIntArray(encoder, dataFile.equality_ids)
  // sort_order_id
  writeOptionalInt(encoder, dataFile.sort_order_id)
  // v3 fields
  // first_row_id
  writeOptionalLong(encoder, dataFile.first_row_id)
  // referenced_data_file
  writeOptionalString(encoder, dataFile.referenced_data_file)
  // content_offset
  writeOptionalLong(encoder, dataFile.content_offset)
  // content_size_in_bytes
  writeOptionalLong(encoder, dataFile.content_size_in_bytes)
}

function encodeManifestEntry(
  encoder: AvroEncoder,
  entry: EncodableManifestEntry,
  partitionFields: PartitionFieldDef[]
): void {
  // status (int)
  encoder.writeInt(entry.status)
  // snapshot_id (optional long)
  writeOptionalLong(encoder, entry.snapshot_id)
  // sequence_number (optional long)
  writeOptionalLong(encoder, entry.sequence_number)
  // file_sequence_number (optional long)
  writeOptionalLong(encoder, entry.file_sequence_number)
  // data_file (record)
  encodeDataFile(encoder, entry.data_file, partitionFields)
}

function encodeManifestListEntry(encoder: AvroEncoder, entry: EncodableManifestListEntry): void {
  // manifest_path (string)
  encoder.writeString(entry.manifest_path)
  // manifest_length (long)
  encoder.writeLong(entry.manifest_length)
  // partition_spec_id (int)
  encoder.writeInt(entry.partition_spec_id)
  // content (int)
  encoder.writeInt(entry.content)
  // sequence_number (long)
  encoder.writeLong(entry.sequence_number)
  // min_sequence_number (long)
  encoder.writeLong(entry.min_sequence_number)
  // added_snapshot_id (long)
  encoder.writeLong(entry.added_snapshot_id)
  // added_files_count (int)
  encoder.writeInt(entry.added_files_count)
  // existing_files_count (int)
  encoder.writeInt(entry.existing_files_count)
  // deleted_files_count (int)
  encoder.writeInt(entry.deleted_files_count)
  // added_rows_count (long)
  encoder.writeLong(entry.added_rows_count)
  // existing_rows_count (long)
  encoder.writeLong(entry.existing_rows_count)
  // deleted_rows_count (long)
  encoder.writeLong(entry.deleted_rows_count)
  // partitions (optional array)
  if (entry.partitions === null || entry.partitions === undefined) {
    encoder.writeUnionIndex(0) // null
  } else {
    encoder.writeUnionIndex(1) // non-null
    encoder.writeArray(entry.partitions, (p) => {
      encoder.writeBoolean(p.contains_null)
      writeOptionalBoolean(encoder, p.contains_nan)
      writeOptionalBytes(encoder, p.lower_bound)
      writeOptionalBytes(encoder, p.upper_bound)
    })
  }
  // first_row_id (v3)
  writeOptionalLong(encoder, entry.first_row_id)
}

/**
 * Encode manifest entries to Avro binary format.
 * Required for DuckDB, Spark, Snowflake interoperability.
 */
export function encodeManifestToAvro(entries: ManifestEntry[], sequenceNumber: number): Uint8Array {
  // Empty partition fields for unpartitioned tables
  const partitionFields: Array<{ name: string; type: 'string' | 'int' | 'long'; 'field-id': number }> = []
  const schema = createManifestEntrySchema(partitionFields as never, 0)
  const metadata: Record<string, string> = {
    'format-version': '2',
    'content': 'data',
  }
  const fileWriter = new AvroFileWriter(schema, metadata)
  const encoder = new AvroEncoder()

  for (const entry of entries) {
    const dataFile = entry['data-file']
    const encodableEntry: EncodableManifestEntry = {
      status: entry.status,
      snapshot_id: entry['snapshot-id'],
      sequence_number: entry['sequence-number'] ?? sequenceNumber,
      file_sequence_number: entry['file-sequence-number'] ?? sequenceNumber,
      data_file: {
        content: dataFile.content ?? 0,
        file_path: dataFile['file-path'],
        file_format: dataFile['file-format'].toLowerCase(),
        partition: dataFile.partition ?? {},
        record_count: dataFile['record-count'],
        file_size_in_bytes: dataFile['file-size-in-bytes'],
        column_sizes: null,
        value_counts: null,
        null_value_counts: null,
        nan_value_counts: null,
        lower_bounds: null,
        upper_bounds: null,
        key_metadata: null,
        split_offsets: null,
        equality_ids: null,
        sort_order_id: null,
      },
    }
    const partitionFieldDefs: PartitionFieldDef[] = []
    encodeManifestEntry(encoder, encodableEntry, partitionFieldDefs)
  }

  const entriesData = encoder.toBuffer()
  fileWriter.addBlock(entries.length, entriesData)
  return fileWriter.toBuffer()
}

/**
 * Encode manifest list entries to Avro binary format.
 */
export function encodeManifestListToAvro(manifests: ManifestFile[]): Uint8Array {
  const schema = createManifestListSchema()
  const metadata: Record<string, string> = {
    'format-version': '2',
  }
  const fileWriter = new AvroFileWriter(schema, metadata)
  const encoder = new AvroEncoder()

  for (const manifest of manifests) {
    const encodableEntry: EncodableManifestListEntry = {
      manifest_path: manifest['manifest-path'],
      manifest_length: manifest['manifest-length'],
      partition_spec_id: manifest['partition-spec-id'] ?? 0,
      content: manifest.content ?? 0,
      sequence_number: manifest['sequence-number'] ?? 0,
      min_sequence_number: manifest['min-sequence-number'] ?? 0,
      added_snapshot_id: manifest['added-snapshot-id'] ?? 0,
      added_files_count: manifest['added-files-count'] ?? 0,
      existing_files_count: manifest['existing-files-count'] ?? 0,
      deleted_files_count: manifest['deleted-files-count'] ?? 0,
      added_rows_count: manifest['added-rows-count'] ?? 0,
      existing_rows_count: manifest['existing-rows-count'] ?? 0,
      deleted_rows_count: manifest['deleted-rows-count'] ?? 0,
      partitions: null,
    }
    encodeManifestListEntry(encoder, encodableEntry)
  }

  const entriesData = encoder.toBuffer()
  fileWriter.addBlock(manifests.length, entriesData)
  return fileWriter.toBuffer()
}

// ===========================================================================
// Avro Decoding Helpers
// ===========================================================================

function readVarLong(data: Uint8Array, offset: number): { value: number; newOffset: number } {
  let value = 0
  let shift = 0
  while (offset < data.length) {
    const b = data[offset++]!
    value |= (b & 0x7f) << shift
    if ((b & 0x80) === 0) break
    shift += 7
  }
  value = (value >>> 1) ^ -(value & 1)
  return { value, newOffset: offset }
}

function readAvroString(data: Uint8Array, offset: number): { value: string; newOffset: number } {
  const lenResult = readVarLong(data, offset)
  const strData = data.slice(lenResult.newOffset, lenResult.newOffset + lenResult.value)
  return { value: new TextDecoder().decode(strData), newOffset: lenResult.newOffset + lenResult.value }
}

function skipAvroMap(data: Uint8Array, offset: number): number {
  // Avro maps are written as: count, entries..., 0
  // The count can be negative (means block has size prefix)
  while (true) {
    let mapEntries = readVarLong(data, offset)
    offset = mapEntries.newOffset

    // Zero terminates the map
    if (mapEntries.value === 0) break

    // Negative count means block has size prefix
    if (mapEntries.value < 0) {
      mapEntries.value = -mapEntries.value
      offset = readVarLong(data, offset).newOffset // Skip block size
    }

    // Read each key-value pair
    for (let i = 0; i < mapEntries.value; i++) {
      const keyLen = readVarLong(data, offset)
      offset = keyLen.newOffset + keyLen.value
      const valLen = readVarLong(data, offset)
      offset = valLen.newOffset + valLen.value
    }
  }
  return offset
}

function readAvroBlock(data: Uint8Array, offset: number): { count: number; data: Uint8Array; newOffset: number } {
  const countResult = readVarLong(data, offset)
  if (countResult.value === 0) return { count: 0, data: new Uint8Array(0), newOffset: countResult.newOffset }
  const sizeResult = readVarLong(data, countResult.newOffset)
  const blockData = data.slice(sizeResult.newOffset, sizeResult.newOffset + sizeResult.value)
  return { count: countResult.value, data: blockData, newOffset: sizeResult.newOffset + sizeResult.value }
}

/**
 * Decode manifest list from either Avro or JSON format.
 * Supports both new Avro format and legacy JSON format.
 */
export function decodeManifestListFromAvroOrJson(data: Uint8Array): ManifestFile[] {
  if (isAvroFormat(data)) return decodeManifestListFromAvro(data)
  return JSON.parse(new TextDecoder().decode(data)) as ManifestFile[]
}

function decodeManifestListFromAvro(data: Uint8Array): ManifestFile[] {
  const result: ManifestFile[] = []
  let offset = 4 // Skip magic bytes
  offset = skipAvroMap(data, offset) // Skip header metadata map (includes terminating zero)
  offset += 16 // Skip sync marker
  while (offset < data.length) {
    const blockResult = readAvroBlock(data, offset)
    if (blockResult.count === 0) break
    let blockOffset = 0
    for (let i = 0; i < blockResult.count && blockOffset < blockResult.data.length; i++) {
      const entryResult = decodeManifestListEntryFromBlock(blockResult.data, blockOffset)
      result.push(entryResult.manifest)
      blockOffset = entryResult.newOffset
    }
    offset = blockResult.newOffset + 16
  }
  return result
}

function decodeManifestListEntryFromBlock(
  data: Uint8Array,
  offset: number
): { manifest: ManifestFile; newOffset: number } {
  const manifestPath = readAvroString(data, offset)
  offset = manifestPath.newOffset
  const manifestLength = readVarLong(data, offset)
  offset = manifestLength.newOffset
  const partitionSpecId = readVarLong(data, offset)
  offset = partitionSpecId.newOffset
  const content = readVarLong(data, offset)
  offset = content.newOffset
  const sequenceNumber = readVarLong(data, offset)
  offset = sequenceNumber.newOffset
  const minSequenceNumber = readVarLong(data, offset)
  offset = minSequenceNumber.newOffset
  const addedSnapshotId = readVarLong(data, offset)
  offset = addedSnapshotId.newOffset
  const addedFilesCount = readVarLong(data, offset)
  offset = addedFilesCount.newOffset
  const existingFilesCount = readVarLong(data, offset)
  offset = existingFilesCount.newOffset
  const deletedFilesCount = readVarLong(data, offset)
  offset = deletedFilesCount.newOffset
  const addedRowsCount = readVarLong(data, offset)
  offset = addedRowsCount.newOffset
  const existingRowsCount = readVarLong(data, offset)
  offset = existingRowsCount.newOffset
  const deletedRowsCount = readVarLong(data, offset)
  offset = deletedRowsCount.newOffset

  // Skip optional partitions array (union: null or array)
  const unionIndex = readVarLong(data, offset)
  offset = unionIndex.newOffset
  if (unionIndex.value === 1) {
    const arrayCount = readVarLong(data, offset)
    offset = arrayCount.newOffset
    for (let i = 0; i < Math.abs(arrayCount.value); i++) {
      offset = readVarLong(data, offset).newOffset // contains_null
      const nanUnion = readVarLong(data, offset)
      offset = nanUnion.newOffset
      if (nanUnion.value === 1) offset = readVarLong(data, offset).newOffset
      const lowerUnion = readVarLong(data, offset)
      offset = lowerUnion.newOffset
      if (lowerUnion.value === 1) {
        const lowerLen = readVarLong(data, offset)
        offset = lowerLen.newOffset + lowerLen.value
      }
      const upperUnion = readVarLong(data, offset)
      offset = upperUnion.newOffset
      if (upperUnion.value === 1) {
        const upperLen = readVarLong(data, offset)
        offset = upperLen.newOffset + upperLen.value
      }
    }
    if (arrayCount.value > 0) offset = readVarLong(data, offset).newOffset
  }

  return {
    manifest: {
      'manifest-path': manifestPath.value,
      'manifest-length': manifestLength.value,
      'partition-spec-id': partitionSpecId.value,
      content: content.value,
      'sequence-number': sequenceNumber.value,
      'min-sequence-number': minSequenceNumber.value,
      'added-snapshot-id': addedSnapshotId.value,
      'added-files-count': addedFilesCount.value,
      'existing-files-count': existingFilesCount.value,
      'deleted-files-count': deletedFilesCount.value,
      'added-rows-count': addedRowsCount.value,
      'existing-rows-count': existingRowsCount.value,
      'deleted-rows-count': deletedRowsCount.value,
    },
    newOffset: offset,
  }
}

/**
 * Decode manifest entries from either Avro or JSON format.
 * Supports both new Avro format and legacy JSON format.
 */
export function decodeManifestFromAvroOrJson(data: Uint8Array): ManifestEntry[] {
  if (isAvroFormat(data)) return decodeManifestFromAvro(data)
  return JSON.parse(new TextDecoder().decode(data)) as ManifestEntry[]
}

function decodeManifestFromAvro(data: Uint8Array): ManifestEntry[] {
  const result: ManifestEntry[] = []
  let offset = 4 // Skip magic bytes
  offset = skipAvroMap(data, offset) // Skip header metadata map (includes terminating zero)
  offset += 16 // Skip sync marker
  while (offset < data.length) {
    const blockResult = readAvroBlock(data, offset)
    if (blockResult.count === 0) break
    let blockOffset = 0
    for (let i = 0; i < blockResult.count && blockOffset < blockResult.data.length; i++) {
      const entryResult = decodeManifestEntryFromBlock(blockResult.data, blockOffset)
      result.push(entryResult.manifest)
      blockOffset = entryResult.newOffset
    }
    offset = blockResult.newOffset + 16
  }
  return result
}

function decodeManifestEntryFromBlock(
  data: Uint8Array,
  offset: number
): { manifest: ManifestEntry; newOffset: number } {
  const status = readVarLong(data, offset)
  offset = status.newOffset

  // snapshot_id (union [null, long])
  const snapshotUnion = readVarLong(data, offset)
  offset = snapshotUnion.newOffset
  let snapshotId = 0
  if (snapshotUnion.value === 1) {
    const sid = readVarLong(data, offset)
    offset = sid.newOffset
    snapshotId = sid.value
  }

  // sequence_number (union [null, long])
  const seqUnion = readVarLong(data, offset)
  offset = seqUnion.newOffset
  let sequenceNumber = 0
  if (seqUnion.value === 1) {
    const sn = readVarLong(data, offset)
    offset = sn.newOffset
    sequenceNumber = sn.value
  }

  // file_sequence_number (union [null, long])
  const fileSeqUnion = readVarLong(data, offset)
  offset = fileSeqUnion.newOffset
  let fileSequenceNumber = 0
  if (fileSeqUnion.value === 1) {
    const fsn = readVarLong(data, offset)
    offset = fsn.newOffset
    fileSequenceNumber = fsn.value
  }

  // data_file record
  const content = readVarLong(data, offset)
  offset = content.newOffset

  const filePath = readAvroString(data, offset)
  offset = filePath.newOffset

  const fileFormat = readAvroString(data, offset)
  offset = fileFormat.newOffset

  // For unpartitioned tables, partition is an empty record

  const recordCount = readVarLong(data, offset)
  offset = recordCount.newOffset

  const fileSizeInBytes = readVarLong(data, offset)
  offset = fileSizeInBytes.newOffset

  // Skip optional fields (14 of them for v3)
  for (let i = 0; i < 14; i++) {
    const unionIdx = readVarLong(data, offset)
    offset = unionIdx.newOffset
    if (unionIdx.value === 1) {
      if (i < 6) {
        // Maps: column_sizes through upper_bounds
        let mapCount = readVarLong(data, offset)
        offset = mapCount.newOffset
        if (mapCount.value < 0) {
          mapCount.value = -mapCount.value
          offset = readVarLong(data, offset).newOffset
        }
        for (let j = 0; j < mapCount.value; j++) {
          offset = readVarLong(data, offset).newOffset // key
          if (i >= 4) {
            // bytes value (lower_bounds/upper_bounds)
            const bytesLen = readVarLong(data, offset)
            offset = bytesLen.newOffset + bytesLen.value
          } else {
            offset = readVarLong(data, offset).newOffset // long value
          }
        }
        if (mapCount.value > 0) offset = readVarLong(data, offset).newOffset
      } else if (i === 6) {
        // key_metadata (bytes)
        const bytesLen = readVarLong(data, offset)
        offset = bytesLen.newOffset + bytesLen.value
      } else if (i === 7 || i === 8) {
        // Arrays: split_offsets, equality_ids
        let arrCount = readVarLong(data, offset)
        offset = arrCount.newOffset
        if (arrCount.value < 0) {
          arrCount.value = -arrCount.value
          offset = readVarLong(data, offset).newOffset
        }
        for (let j = 0; j < arrCount.value; j++) {
          offset = readVarLong(data, offset).newOffset
        }
        if (arrCount.value > 0) offset = readVarLong(data, offset).newOffset
      } else if (i === 9 || i === 10 || i === 13 || i === 14) {
        // sort_order_id, first_row_id, content_offset, content_size_in_bytes (int/long)
        offset = readVarLong(data, offset).newOffset
      } else if (i === 11) {
        // referenced_data_file (string)
        const strLen = readVarLong(data, offset)
        offset = strLen.newOffset + strLen.value
      }
    }
  }

  return {
    manifest: {
      status: status.value as 0 | 1 | 2,
      'snapshot-id': snapshotId,
      'sequence-number': sequenceNumber,
      'file-sequence-number': fileSequenceNumber,
      'data-file': {
        content: content.value,
        'file-path': filePath.value,
        'file-format': fileFormat.value.toUpperCase() as 'parquet' | 'avro' | 'orc',
        partition: {},
        'record-count': recordCount.value,
        'file-size-in-bytes': fileSizeInBytes.value,
      },
    },
    newOffset: offset,
  }
}
