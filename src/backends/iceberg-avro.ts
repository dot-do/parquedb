/**
 * Avro Encoding/Decoding for Iceberg Manifest Files
 *
 * This module provides Avro binary encoding and decoding for Iceberg manifest files
 * and manifest lists. This is required for interoperability with DuckDB, Spark,
 * Snowflake, and other tools that expect Avro-encoded manifests.
 *
 * Uses @dotdo/iceberg's Avro utilities for encoding/decoding manifest files.
 *
 * @see https://iceberg.apache.org/spec/
 * @see https://avro.apache.org/docs/current/specification/
 */

import {
  // Avro classes
  AvroEncoder,
  AvroDecoder,
  AvroFileWriter,
  // Schema builders
  createManifestEntrySchema,
  createManifestListSchema,
  // Encoding functions from @dotdo/iceberg
  encodeManifestEntry,
  encodeManifestListEntry,
  // Decoding functions from @dotdo/iceberg
  decodeManifestEntry as _decodeManifestEntryAvro,
  decodeManifestListEntry as _decodeManifestListEntryAvro,
  // Types
  type EncodableManifestEntry,
  type EncodableManifestListEntry,
  type PartitionFieldDef,
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
// Avro Container File Parsing Helpers
// ===========================================================================

/**
 * Parse Avro container file header to extract block data.
 * Returns the offset after the header and sync marker, plus the sync marker itself.
 * @internal Reserved for future use
 */
export function parseAvroContainerHeader(data: Uint8Array): { dataOffset: number; syncMarker: Uint8Array } | null {
  if (!isAvroFormat(data)) return null

  let offset = 4 // Skip magic bytes

  // Read header metadata map
  // Maps are encoded as: count, [key, value], ..., 0
  while (true) {
    const decoder = new AvroDecoder(data.slice(offset))
    const count = decoder.readLong()
    offset += decoder.position

    if (count === 0) break

    let absCount = count
    if (count < 0) {
      absCount = -count
      const sizeDecoder = new AvroDecoder(data.slice(offset))
      sizeDecoder.readLong() // Skip block size
      offset += sizeDecoder.position
    }

    // Read each key-value pair
    for (let i = 0; i < absCount; i++) {
      const kvDecoder = new AvroDecoder(data.slice(offset))
      const key = kvDecoder.readString()
      const value = kvDecoder.readBytes()
      void key
      void value
      offset += kvDecoder.position
    }
  }

  // Read 16-byte sync marker
  const syncMarker = data.slice(offset, offset + 16)
  offset += 16

  return { dataOffset: offset, syncMarker }
}

/**
 * Read a block from an Avro container file.
 * Returns the count, block data, and next offset.
 */
function readAvroBlock(data: Uint8Array, offset: number): { count: number; data: Uint8Array; newOffset: number } {
  const decoder = new AvroDecoder(data.slice(offset))
  const count = decoder.readLong()
  if (count === 0) {
    return { count: 0, data: new Uint8Array(0), newOffset: offset + decoder.position }
  }
  const size = decoder.readLong()
  const blockData = data.slice(offset + decoder.position, offset + decoder.position + size)
  return { count, data: blockData, newOffset: offset + decoder.position + size }
}

// ===========================================================================
// Low-level Avro Decoding Helpers
// ===========================================================================

/**
 * Read a zig-zag encoded variable-length integer from a buffer.
 * Avro uses zig-zag encoding for signed integers.
 */
function readVarLong(data: Uint8Array, offset: number): { value: number; newOffset: number } {
  let value = 0n
  let shift = 0n
  let newOffset = offset

  while (newOffset < data.length) {
    const b = data[newOffset++]!
    value |= BigInt(b & 0x7f) << shift
    if ((b & 0x80) === 0) break
    shift += 7n
  }

  // Zig-zag decode
  const decoded = (value >> 1n) ^ -(value & 1n)
  return { value: Number(decoded), newOffset }
}

/**
 * Skip over an Avro map in the buffer.
 * Maps are encoded as: count, [key, value], ..., 0 (terminating zero block)
 */
function skipAvroMap(data: Uint8Array, offset: number): number {
  while (true) {
    const countResult = readVarLong(data, offset)
    offset = countResult.newOffset

    if (countResult.value === 0) break

    let count = countResult.value
    if (count < 0) {
      // Negative count means block has size prefix
      count = -count
      const sizeResult = readVarLong(data, offset)
      offset = sizeResult.newOffset
    }

    // Skip each key-value pair
    for (let i = 0; i < count; i++) {
      // Skip key (string: length + bytes)
      const keyLenResult = readVarLong(data, offset)
      offset = keyLenResult.newOffset + keyLenResult.value

      // Skip value (bytes: length + bytes)
      const valueLenResult = readVarLong(data, offset)
      offset = valueLenResult.newOffset + valueLenResult.value
    }
  }

  return offset
}

/**
 * Read an Avro string from a buffer.
 * Strings are encoded as: length (varint), UTF-8 bytes
 */
function readAvroString(data: Uint8Array, offset: number): { value: string; newOffset: number } {
  const lenResult = readVarLong(data, offset)
  const strBytes = data.slice(lenResult.newOffset, lenResult.newOffset + lenResult.value)
  const value = new TextDecoder().decode(strBytes)
  return { value, newOffset: lenResult.newOffset + lenResult.value }
}

/**
 * Decode manifest list from either Avro or JSON format.
 * Supports both new Avro format and legacy JSON format.
 */
export function decodeManifestListFromAvroOrJson(data: Uint8Array): ManifestFile[] {
  if (isAvroFormat(data)) return decodeManifestListFromAvro(data)
  try {
    return JSON.parse(new TextDecoder().decode(data)) as ManifestFile[]
  } catch {
    // Invalid JSON in manifest list - return empty array
    return []
  }
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

  // Skip optional first_row_id (v3 field) - union [null, long]
  const firstRowIdUnion = readVarLong(data, offset)
  offset = firstRowIdUnion.newOffset
  if (firstRowIdUnion.value === 1) {
    offset = readVarLong(data, offset).newOffset // skip the long value
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
  try {
    return JSON.parse(new TextDecoder().decode(data)) as ManifestEntry[]
  } catch {
    // Invalid JSON in manifest - return empty array
    return []
  }
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
