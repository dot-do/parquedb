/**
 * Parquet VARIANT Binary Decoder
 *
 * Decodes VARIANT binary format (metadata + value) back to JavaScript values.
 * This is the read-side counterpart to hyparquet-writer's encodeVariant.
 *
 * The VARIANT format stores:
 * - metadata: a string dictionary (keys used in object fields)
 * - value: a self-describing binary-encoded value
 *
 * Spec: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 */

const decoder = new TextDecoder()

// =============================================================================
// Public API
// =============================================================================

/**
 * Decode a VARIANT binary pair into a JavaScript value.
 *
 * @param metadata - Binary metadata (string dictionary)
 * @param value - Binary encoded value
 * @returns The decoded JavaScript value (object, array, string, number, boolean, null)
 */
export function decodeVariant(
  metadata: Uint8Array,
  value: Uint8Array,
): unknown {
  const dictionary = decodeMetadata(metadata)
  return decodeValue(value, 0, dictionary).value
}

// =============================================================================
// Metadata (Dictionary) Decoder
// =============================================================================

/**
 * Decode the VARIANT metadata (string dictionary).
 *
 * Format:
 * - header: 1 byte (version in bits 0-3, sorted in bit 4, offset_size_minus_one in bits 6-7)
 * - dictionary_size: offset_size bytes
 * - offsets: (dictionary_size + 1) * offset_size bytes
 * - strings: concatenated UTF-8
 */
function decodeMetadata(metadata: Uint8Array): string[] {
  if (metadata.length < 2) return []

  const header = metadata[0]
  // version is bits 0-3 (should be 1)
  // sorted is bit 4
  const offsetSize = ((header >> 6) & 0x03) + 1

  let pos = 1

  // Dictionary size
  const dictSize = readUnsigned(metadata, pos, offsetSize)
  pos += offsetSize

  if (dictSize === 0) return []

  // Read offsets
  const offsets: number[] = []
  for (let i = 0; i <= dictSize; i++) {
    offsets.push(readUnsigned(metadata, pos, offsetSize))
    pos += offsetSize
  }

  // Read strings
  const stringsStart = pos
  const dictionary: string[] = []
  for (let i = 0; i < dictSize; i++) {
    const start = stringsStart + offsets[i]
    const end = stringsStart + offsets[i + 1]
    dictionary.push(decoder.decode(metadata.slice(start, end)))
  }

  return dictionary
}

// =============================================================================
// Value Decoder
// =============================================================================

/**
 * Decode a VARIANT value at the given offset.
 * Returns the decoded value and the number of bytes consumed.
 */
function decodeValue(
  buf: Uint8Array,
  offset: number,
  dictionary: string[],
): { value: unknown; bytesRead: number } {
  if (buf.length === 0 || offset >= buf.length) {
    return { value: null, bytesRead: 0 }
  }

  const header = buf[offset]
  const basicType = header & 0x03

  switch (basicType) {
    case 0: // Primitive
      return decodePrimitive(buf, offset)
    case 1: // Short string
      return decodeShortString(buf, offset)
    case 2: // Object
      return decodeObject(buf, offset, dictionary)
    case 3: // Array
      return decodeArray(buf, offset, dictionary)
    default:
      return { value: null, bytesRead: 1 }
  }
}

/**
 * Decode a primitive value.
 * Header: basic_type=0 (2 bits), type_id (upper 6 bits)
 */
function decodePrimitive(
  buf: Uint8Array,
  offset: number,
): { value: unknown; bytesRead: number } {
  const header = buf[offset]
  const typeId = (header >> 2) & 0x3F

  switch (typeId) {
    case 0: // null
      return { value: null, bytesRead: 1 }
    case 1: // true
      return { value: true, bytesRead: 1 }
    case 2: // false
      return { value: false, bytesRead: 1 }
    case 3: { // int8
      const val = new DataView(buf.buffer, buf.byteOffset + offset + 1, 1).getInt8(0)
      return { value: val, bytesRead: 2 }
    }
    case 4: { // int16
      const val = new DataView(buf.buffer, buf.byteOffset + offset + 1, 2).getInt16(0, true)
      return { value: val, bytesRead: 3 }
    }
    case 5: { // int32
      const val = new DataView(buf.buffer, buf.byteOffset + offset + 1, 4).getInt32(0, true)
      return { value: val, bytesRead: 5 }
    }
    case 6: { // int64
      const val = new DataView(buf.buffer, buf.byteOffset + offset + 1, 8).getBigInt64(0, true)
      // Convert to number to avoid BigInt propagation (per project constraint)
      return { value: Number(val), bytesRead: 9 }
    }
    case 7: { // double
      const val = new DataView(buf.buffer, buf.byteOffset + offset + 1, 8).getFloat64(0, true)
      return { value: val, bytesRead: 9 }
    }
    case 8: { // decimal4 (not commonly used)
      return { value: null, bytesRead: 5 }
    }
    case 9: { // decimal8 (not commonly used)
      return { value: null, bytesRead: 9 }
    }
    case 10: { // decimal16 (not commonly used)
      return { value: null, bytesRead: 17 }
    }
    case 11: { // date (INT32 days since epoch)
      const days = new DataView(buf.buffer, buf.byteOffset + offset + 1, 4).getInt32(0, true)
      return { value: days, bytesRead: 5 }
    }
    case 12: { // timestamp_micros (INT64)
      const micros = new DataView(buf.buffer, buf.byteOffset + offset + 1, 8).getBigInt64(0, true)
      return { value: Number(micros / 1000n), bytesRead: 9 }
    }
    case 13: { // timestamp_ntz_micros (INT64)
      const micros = new DataView(buf.buffer, buf.byteOffset + offset + 1, 8).getBigInt64(0, true)
      return { value: Number(micros / 1000n), bytesRead: 9 }
    }
    case 14: { // float
      const val = new DataView(buf.buffer, buf.byteOffset + offset + 1, 4).getFloat32(0, true)
      return { value: val, bytesRead: 5 }
    }
    case 15: { // binary
      const len = new DataView(buf.buffer, buf.byteOffset + offset + 1, 4).getUint32(0, true)
      const data = buf.slice(offset + 5, offset + 5 + len)
      return { value: data, bytesRead: 5 + len }
    }
    case 16: { // long string
      const len = new DataView(buf.buffer, buf.byteOffset + offset + 1, 4).getUint32(0, true)
      const str = decoder.decode(buf.slice(offset + 5, offset + 5 + len))
      return { value: str, bytesRead: 5 + len }
    }
    default:
      return { value: null, bytesRead: 1 }
  }
}

/**
 * Decode a short string.
 * Header: basic_type=1 (2 bits), length (upper 6 bits)
 */
function decodeShortString(
  buf: Uint8Array,
  offset: number,
): { value: unknown; bytesRead: number } {
  const header = buf[offset]
  const length = (header >> 2) & 0x3F
  const str = decoder.decode(buf.slice(offset + 1, offset + 1 + length))
  return { value: str, bytesRead: 1 + length }
}

/**
 * Decode an object.
 * Header: basic_type=2, offset_size (2 bits), id_size (2 bits), is_large (1 bit)
 */
function decodeObject(
  buf: Uint8Array,
  offset: number,
  dictionary: string[],
): { value: unknown; bytesRead: number } {
  const header = buf[offset]
  const offsetSize = ((header >> 2) & 0x03) + 1
  const idSize = ((header >> 4) & 0x03) + 1
  const isLarge = (header & 0x40) !== 0

  let pos = offset + 1

  // Number of elements
  let numElements: number
  if (isLarge) {
    numElements = new DataView(buf.buffer, buf.byteOffset + pos, 4).getUint32(0, true)
    pos += 4
  } else {
    numElements = buf[pos]
    pos += 1
  }

  if (numElements === 0) {
    return { value: {}, bytesRead: pos - offset }
  }

  // Field IDs
  const fieldIds: number[] = []
  for (let i = 0; i < numElements; i++) {
    fieldIds.push(readUnsigned(buf, pos, idSize))
    pos += idSize
  }

  // Offsets
  const offsets: number[] = []
  for (let i = 0; i <= numElements; i++) {
    offsets.push(readUnsigned(buf, pos, offsetSize))
    pos += offsetSize
  }

  // Values start at current pos
  const valuesStart = pos
  const result: Record<string, unknown> = {}

  for (let i = 0; i < numElements; i++) {
    const key = dictionary[fieldIds[i]] ?? `_unknown_${fieldIds[i]}`
    const valueOffset = valuesStart + offsets[i]
    const decoded = decodeValue(buf, valueOffset, dictionary)
    result[key] = decoded.value
  }

  // Total bytes consumed: up to the end of the last value
  const totalBytes = valuesStart + offsets[numElements] - offset
  return { value: result, bytesRead: totalBytes }
}

/**
 * Decode an array.
 * Header: basic_type=3, offset_size (2 bits), is_large (1 bit)
 */
function decodeArray(
  buf: Uint8Array,
  offset: number,
  dictionary: string[],
): { value: unknown; bytesRead: number } {
  const header = buf[offset]
  const offsetSize = ((header >> 2) & 0x03) + 1
  const isLarge = (header & 0x10) !== 0

  let pos = offset + 1

  // Number of elements
  let numElements: number
  if (isLarge) {
    numElements = new DataView(buf.buffer, buf.byteOffset + pos, 4).getUint32(0, true)
    pos += 4
  } else {
    numElements = buf[pos]
    pos += 1
  }

  if (numElements === 0) {
    // Empty array: header + numElements + offsets section
    return { value: [], bytesRead: pos + (numElements + 1) * offsetSize - offset }
  }

  // Offsets
  const offsets: number[] = []
  for (let i = 0; i <= numElements; i++) {
    offsets.push(readUnsigned(buf, pos, offsetSize))
    pos += offsetSize
  }

  // Values start at current pos
  const valuesStart = pos
  const result: unknown[] = []

  for (let i = 0; i < numElements; i++) {
    const valueOffset = valuesStart + offsets[i]
    const decoded = decodeValue(buf, valueOffset, dictionary)
    result.push(decoded.value)
  }

  const totalBytes = valuesStart + offsets[numElements] - offset
  return { value: result, bytesRead: totalBytes }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Read an unsigned integer in little-endian format.
 * @internal Exported for testing; not part of the public API.
 */
export function readUnsigned(buf: Uint8Array, pos: number, byteWidth: number): number {
  if (byteWidth > 4) throw new Error('readUnsigned: byteWidth must be <= 4')
  let value = 0
  for (let i = 0; i < byteWidth; i++) {
    value |= buf[pos + i] << (i * 8)
  }
  return value >>> 0 // Ensure unsigned
}
