/**
 * Compact Encoding for ParqueDB Secondary Index Entries
 *
 * Reduces index entry size from ~36 bytes to ~12-16 bytes:
 *
 * Old Format (v1):
 *   Entry = [type(1)] + [keyLength(4)] + [key(variable)] + [docIdLength(4)] + [docId(26)] + [rowGroup(4)]
 *   ~37 bytes per entry for typical IMDB data
 *
 * New Compact Format (v2):
 *   Entry = [rowGroup(2)] + [rowOffset(varint)] + [docIdLength(1)] + [docId(variable)]
 *   ~12-14 bytes per entry
 *
 * For non-sharded indexes (backward compat), key is retained with compact encoding:
 *   Entry = [keyHash(4)] + [rowGroup(2)] + [rowOffset(varint)] + [docIdLength(1)] + [docId(variable)]
 */

// =============================================================================
// Format Constants
// =============================================================================

/** Version 1: Original format with full key (non-sharded) */
export const FORMAT_VERSION_1 = 0x01

/** Version 2: Sharded format with full key */
export const FORMAT_VERSION_2 = 0x02

/** Version 3: Compact format with varint encoding (no key stored) */
export const FORMAT_VERSION_3 = 0x03

// =============================================================================
// Varint Encoding (LEB128-style)
// =============================================================================

/**
 * Write a variable-length unsigned integer to buffer
 *
 * Uses LEB128-style encoding where:
 * - Values 0-127 use 1 byte
 * - Values 128-16383 use 2 bytes
 * - Values 16384-2097151 use 3 bytes
 * - etc.
 *
 * @param buffer - Target buffer
 * @param offset - Write position
 * @param value - Value to encode (must be non-negative)
 * @returns Number of bytes written
 */
export function writeVarint(buffer: Uint8Array, offset: number, value: number): number {
  if (value < 0) {
    throw new Error('Varint value must be non-negative')
  }

  let bytesWritten = 0
  let v = value

  while (v >= 0x80) {
    buffer[offset + bytesWritten] = (v & 0x7f) | 0x80
    v >>>= 7
    bytesWritten++
  }

  buffer[offset + bytesWritten] = v
  bytesWritten++

  return bytesWritten
}

/**
 * Read a variable-length unsigned integer from buffer
 *
 * @param buffer - Source buffer
 * @param offset - Read position
 * @returns Decoded value and number of bytes read
 */
export function readVarint(
  buffer: Uint8Array,
  offset: number
): { value: number; bytesRead: number } {
  let value = 0
  let shift = 0
  let bytesRead = 0

  while (true) {
    if (offset + bytesRead >= buffer.length) {
      throw new Error('Varint extends beyond buffer')
    }

    const byte = buffer[offset + bytesRead]
    value |= (byte & 0x7f) << shift
    bytesRead++

    if ((byte & 0x80) === 0) {
      break
    }

    shift += 7
    if (shift > 28) {
      throw new Error('Varint too large')
    }
  }

  return { value, bytesRead }
}

/**
 * Calculate the number of bytes needed to encode a varint
 */
export function varintSize(value: number): number {
  if (value < 0) {
    throw new Error('Varint value must be non-negative')
  }

  if (value < 0x80) return 1
  if (value < 0x4000) return 2
  if (value < 0x200000) return 3
  if (value < 0x10000000) return 4
  return 5
}

// =============================================================================
// Compact Entry Encoding
// =============================================================================

export interface CompactEntry {
  /** Row group number (0-65535) */
  rowGroup: number
  /** Row offset within row group */
  rowOffset: number
  /** Document ID */
  docId: string
}

export interface CompactEntryWithKey extends CompactEntry {
  /** 4-byte key hash for non-sharded indexes */
  keyHash: number
}

/**
 * Calculate the size of a compact entry
 */
export function compactEntrySize(entry: CompactEntry): number {
  const docIdBytes = new TextEncoder().encode(entry.docId)
  return (
    2 + // rowGroup (uint16)
    varintSize(entry.rowOffset) + // rowOffset (varint)
    1 + // docIdLength (uint8)
    docIdBytes.length // docId
  )
}

/**
 * Calculate the size of a compact entry with key hash
 */
export function compactEntryWithKeySize(entry: CompactEntryWithKey): number {
  const docIdBytes = new TextEncoder().encode(entry.docId)
  return (
    4 + // keyHash (uint32)
    2 + // rowGroup (uint16)
    varintSize(entry.rowOffset) + // rowOffset (varint)
    1 + // docIdLength (uint8)
    docIdBytes.length // docId
  )
}

/**
 * Write a compact entry to buffer
 *
 * Format: [rowGroup:u16] [rowOffset:varint] [docIdLength:u8] [docId:bytes]
 *
 * @param buffer - Target buffer
 * @param offset - Write position
 * @param entry - Entry to write
 * @returns Number of bytes written
 */
export function writeCompactEntry(
  buffer: Uint8Array,
  offset: number,
  entry: CompactEntry
): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const encoder = new TextEncoder()
  let pos = offset

  // Row group (2 bytes, big-endian)
  view.setUint16(pos, entry.rowGroup, false)
  pos += 2

  // Row offset (varint)
  pos += writeVarint(buffer, pos, entry.rowOffset)

  // Doc ID length (1 byte) + doc ID
  const docIdBytes = encoder.encode(entry.docId)
  if (docIdBytes.length > 255) {
    throw new Error(`Doc ID too long: ${docIdBytes.length} bytes (max 255)`)
  }
  buffer[pos] = docIdBytes.length
  pos += 1
  buffer.set(docIdBytes, pos)
  pos += docIdBytes.length

  return pos - offset
}

/**
 * Read a compact entry from buffer
 *
 * @param buffer - Source buffer
 * @param offset - Read position
 * @returns Decoded entry and number of bytes read
 */
export function readCompactEntry(
  buffer: Uint8Array,
  offset: number
): { entry: CompactEntry; bytesRead: number } {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const decoder = new TextDecoder()
  let pos = offset

  // Row group (2 bytes, big-endian)
  const rowGroup = view.getUint16(pos, false)
  pos += 2

  // Row offset (varint)
  const { value: rowOffset, bytesRead: rowOffsetBytes } = readVarint(buffer, pos)
  pos += rowOffsetBytes

  // Doc ID length (1 byte) + doc ID
  const docIdLength = buffer[pos]
  pos += 1
  const docIdBytes = buffer.slice(pos, pos + docIdLength)
  const docId = decoder.decode(docIdBytes)
  pos += docIdLength

  return {
    entry: { rowGroup, rowOffset, docId },
    bytesRead: pos - offset,
  }
}

/**
 * Write a compact entry with key hash to buffer
 *
 * Format: [keyHash:u32] [rowGroup:u16] [rowOffset:varint] [docIdLength:u8] [docId:bytes]
 *
 * @param buffer - Target buffer
 * @param offset - Write position
 * @param entry - Entry to write
 * @returns Number of bytes written
 */
export function writeCompactEntryWithKey(
  buffer: Uint8Array,
  offset: number,
  entry: CompactEntryWithKey
): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  let pos = offset

  // Key hash (4 bytes, big-endian)
  view.setUint32(pos, entry.keyHash, false)
  pos += 4

  // Write the rest as compact entry
  pos += writeCompactEntry(buffer, pos, entry)

  return pos - offset
}

/**
 * Read a compact entry with key hash from buffer
 *
 * @param buffer - Source buffer
 * @param offset - Read position
 * @returns Decoded entry and number of bytes read
 */
export function readCompactEntryWithKey(
  buffer: Uint8Array,
  offset: number
): { entry: CompactEntryWithKey; bytesRead: number } {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  let pos = offset

  // Key hash (4 bytes, big-endian)
  const keyHash = view.getUint32(pos, false)
  pos += 4

  // Read the rest as compact entry
  const { entry, bytesRead } = readCompactEntry(buffer, pos)
  pos += bytesRead

  return {
    entry: { keyHash, ...entry },
    bytesRead: pos - offset,
  }
}

// =============================================================================
// Index Header
// =============================================================================

export interface CompactIndexHeader {
  /** Format version (FORMAT_VERSION_2 = 0x02) */
  version: number
  /** Total number of entries */
  entryCount: number
  /** Whether entries include key hash */
  hasKeyHash: boolean
}

/**
 * Write compact index header
 *
 * Format: [version:u8] [flags:u8] [entryCount:u32]
 *
 * Flags:
 *   bit 0: hasKeyHash
 *
 * @returns 6 bytes written
 */
export function writeCompactHeader(
  buffer: Uint8Array,
  offset: number,
  header: CompactIndexHeader
): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  let pos = offset

  // Version
  buffer[pos] = header.version
  pos += 1

  // Flags
  let flags = 0
  if (header.hasKeyHash) flags |= 0x01
  buffer[pos] = flags
  pos += 1

  // Entry count
  view.setUint32(pos, header.entryCount, false)
  pos += 4

  return pos - offset
}

/**
 * Read compact index header
 */
export function readCompactHeader(
  buffer: Uint8Array,
  offset: number
): { header: CompactIndexHeader; bytesRead: number } {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  let pos = offset

  // Version
  const version = buffer[pos]
  pos += 1

  // Flags
  const flags = buffer[pos]
  pos += 1
  const hasKeyHash = (flags & 0x01) !== 0

  // Entry count
  const entryCount = view.getUint32(pos, false)
  pos += 4

  return {
    header: { version, entryCount, hasKeyHash },
    bytesRead: pos - offset,
  }
}

// =============================================================================
// Full Index Serialization
// =============================================================================

/**
 * Serialize entries to compact format
 *
 * @param entries - Entries to serialize
 * @param includeKeyHash - Whether to include key hash (for non-sharded indexes)
 * @returns Serialized bytes
 */
export function serializeCompactIndex(
  entries: CompactEntry[],
  includeKeyHash: boolean = false,
  keyHashFn?: (index: number) => number
): Uint8Array {
  // Calculate total size
  const headerSize = 6 // version + flags + entryCount

  let entriesSize = 0
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i]
    if (includeKeyHash) {
      entriesSize += compactEntryWithKeySize({ ...entry, keyHash: keyHashFn?.(i) ?? 0 })
    } else {
      entriesSize += compactEntrySize(entry)
    }
  }

  const buffer = new Uint8Array(headerSize + entriesSize)
  let offset = 0

  // Write header
  offset += writeCompactHeader(buffer, offset, {
    version: FORMAT_VERSION_3,
    entryCount: entries.length,
    hasKeyHash: includeKeyHash,
  })

  // Write entries
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i]
    if (includeKeyHash) {
      offset += writeCompactEntryWithKey(buffer, offset, {
        ...entry,
        keyHash: keyHashFn?.(i) ?? 0,
      })
    } else {
      offset += writeCompactEntry(buffer, offset, entry)
    }
  }

  return buffer
}

/**
 * Deserialize compact format index
 */
export function deserializeCompactIndex(buffer: Uint8Array): {
  entries: CompactEntry[] | CompactEntryWithKey[]
  hasKeyHash: boolean
} {
  let offset = 0

  // Read header
  const { header, bytesRead: headerBytes } = readCompactHeader(buffer, offset)
  offset += headerBytes

  if (header.version !== FORMAT_VERSION_3) {
    throw new Error(`Unsupported compact index version: ${header.version}`)
  }

  const entries: (CompactEntry | CompactEntryWithKey)[] = []

  // Read entries
  for (let i = 0; i < header.entryCount; i++) {
    if (header.hasKeyHash) {
      const { entry, bytesRead } = readCompactEntryWithKey(buffer, offset)
      entries.push(entry)
      offset += bytesRead
    } else {
      const { entry, bytesRead } = readCompactEntry(buffer, offset)
      entries.push(entry)
      offset += bytesRead
    }
  }

  return { entries, hasKeyHash: header.hasKeyHash }
}

// =============================================================================
// Hash Function for Key Hash
// =============================================================================

/**
 * FNV-1a hash function for key hashing
 */
export function fnv1aHash(key: Uint8Array): number {
  let hash = 2166136261
  for (let i = 0; i < key.length; i++) {
    hash ^= key[i]
    hash = (hash * 16777619) >>> 0
  }
  return hash
}
