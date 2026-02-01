/**
 * Variant encoding/decoding for semi-structured data in Parquet
 *
 * Implements a subset of the Variant type spec for efficient storage
 * of flexible JSON-like data within Parquet files.
 *
 * @see https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 */

// =============================================================================
// Constants
// =============================================================================

/** Variant type tags */
const enum VariantType {
  NULL = 0,
  BOOLEAN_TRUE = 1,
  BOOLEAN_FALSE = 2,
  INT8 = 3,
  INT16 = 4,
  INT32 = 5,
  INT64 = 6,
  FLOAT32 = 7,
  FLOAT64 = 8,
  DECIMAL4 = 9,
  DECIMAL8 = 10,
  DECIMAL16 = 11,
  DATE = 12,
  TIMESTAMP = 13,
  TIMESTAMP_NTZ = 14,
  BINARY = 15,
  STRING = 16,
  ARRAY = 17,
  OBJECT = 18,
}

/** Magic bytes for variant header */
const VARIANT_MAGIC = 0x56; // 'V'

/** Current variant version */
const VARIANT_VERSION = 1;

// =============================================================================
// Encoding Functions
// =============================================================================

/**
 * Encode a value as Variant binary format
 *
 * The format is:
 * - 1 byte: magic byte (0x56 = 'V')
 * - 1 byte: version
 * - 1 byte: type tag
 * - N bytes: type-specific payload
 *
 * @param value - The value to encode
 * @returns Encoded Variant as Uint8Array
 */
export function encodeVariant(value: unknown): Uint8Array {
  const encoder = new VariantEncoder();
  encoder.writeValue(value);
  return encoder.finish();
}

/**
 * Decode Variant binary to a JavaScript value
 *
 * @param data - The Variant-encoded data
 * @returns The decoded value
 */
export function decodeVariant(data: Uint8Array): unknown {
  const decoder = new VariantDecoder(data);
  return decoder.readValue();
}

/**
 * Shred fields from an object for columnar storage
 *
 * Extracts specified fields for separate column storage while
 * keeping the remaining data in a Variant-encoded column.
 *
 * @param obj - The object to shred
 * @param shredFields - Fields to extract for columnar storage
 * @returns Object with shredded fields and remaining data
 */
export function shredObject(
  obj: Record<string, unknown>,
  shredFields: string[]
): { shredded: Record<string, unknown>; remaining: Record<string, unknown> } {
  const shredded: Record<string, unknown> = {};
  const remaining: Record<string, unknown> = {};

  const shredSet = new Set(shredFields);

  for (const [key, value] of Object.entries(obj)) {
    if (shredSet.has(key)) {
      shredded[key] = value;
    } else {
      remaining[key] = value;
    }
  }

  return { shredded, remaining };
}

/**
 * Merge shredded columns back with Variant data
 *
 * @param shredded - The shredded column values
 * @param remaining - The Variant-decoded remaining data
 * @returns Merged object
 */
export function mergeShredded(
  shredded: Record<string, unknown>,
  remaining: Record<string, unknown>
): Record<string, unknown> {
  return { ...remaining, ...shredded };
}

// =============================================================================
// Variant Encoder
// =============================================================================

/**
 * Encoder for Variant format
 */
class VariantEncoder {
  private buffer: number[] = [];
  private textEncoder = new TextEncoder();

  constructor() {
    // Write header
    this.buffer.push(VARIANT_MAGIC);
    this.buffer.push(VARIANT_VERSION);
  }

  writeValue(value: unknown): void {
    if (value === null || value === undefined) {
      this.writeNull();
    } else if (typeof value === 'boolean') {
      this.writeBoolean(value);
    } else if (typeof value === 'number') {
      this.writeNumber(value);
    } else if (typeof value === 'string') {
      this.writeString(value);
    } else if (typeof value === 'bigint') {
      this.writeBigInt(value);
    } else if (value instanceof Date) {
      this.writeDate(value);
    } else if (value instanceof Uint8Array) {
      this.writeBinary(value);
    } else if (Array.isArray(value)) {
      this.writeArray(value);
    } else if (typeof value === 'object') {
      this.writeObject(value as Record<string, unknown>);
    } else {
      // Fallback to string representation
      this.writeString(String(value));
    }
  }

  private writeNull(): void {
    this.buffer.push(VariantType.NULL);
  }

  private writeBoolean(value: boolean): void {
    this.buffer.push(value ? VariantType.BOOLEAN_TRUE : VariantType.BOOLEAN_FALSE);
  }

  private writeNumber(value: number): void {
    if (Number.isInteger(value)) {
      // Use smallest integer type that fits
      if (value >= -128 && value <= 127) {
        this.buffer.push(VariantType.INT8);
        this.writeInt8(value);
      } else if (value >= -32768 && value <= 32767) {
        this.buffer.push(VariantType.INT16);
        this.writeInt16(value);
      } else if (value >= -2147483648 && value <= 2147483647) {
        this.buffer.push(VariantType.INT32);
        this.writeInt32(value);
      } else {
        // Large integer, use float64 for safety
        this.buffer.push(VariantType.FLOAT64);
        this.writeFloat64(value);
      }
    } else {
      // Floating point
      this.buffer.push(VariantType.FLOAT64);
      this.writeFloat64(value);
    }
  }

  private writeBigInt(value: bigint): void {
    this.buffer.push(VariantType.INT64);
    this.writeInt64(value);
  }

  private writeString(value: string): void {
    this.buffer.push(VariantType.STRING);
    const encoded = this.textEncoder.encode(value);
    this.writeLength(encoded.length);
    for (let i = 0; i < encoded.length; i++) {
      this.buffer.push(encoded[i]!);  // loop bounds ensure valid index
    }
  }

  private writeBinary(value: Uint8Array): void {
    this.buffer.push(VariantType.BINARY);
    this.writeLength(value.length);
    for (let i = 0; i < value.length; i++) {
      this.buffer.push(value[i]!);  // loop bounds ensure valid index
    }
  }

  private writeDate(value: Date): void {
    this.buffer.push(VariantType.TIMESTAMP);
    // Store as milliseconds since epoch
    this.writeInt64(BigInt(value.getTime()));
  }

  private writeArray(value: unknown[]): void {
    this.buffer.push(VariantType.ARRAY);
    this.writeLength(value.length);
    for (const item of value) {
      this.writeValue(item);
    }
  }

  private writeObject(value: Record<string, unknown>): void {
    this.buffer.push(VariantType.OBJECT);
    const entries = Object.entries(value);
    this.writeLength(entries.length);
    for (const [key, val] of entries) {
      // Write key as string (without type tag)
      const encoded = this.textEncoder.encode(key);
      this.writeLength(encoded.length);
      for (let i = 0; i < encoded.length; i++) {
        this.buffer.push(encoded[i]!);  // loop bounds ensure valid index
      }
      // Write value with type tag
      this.writeValue(val);
    }
  }

  private writeLength(length: number): void {
    // Variable-length encoding (similar to protobuf varint)
    while (length >= 0x80) {
      this.buffer.push((length & 0x7f) | 0x80);
      length >>>= 7;
    }
    this.buffer.push(length);
  }

  private writeInt8(value: number): void {
    this.buffer.push(value & 0xff);
  }

  private writeInt16(value: number): void {
    this.buffer.push(value & 0xff);
    this.buffer.push((value >> 8) & 0xff);
  }

  private writeInt32(value: number): void {
    this.buffer.push(value & 0xff);
    this.buffer.push((value >> 8) & 0xff);
    this.buffer.push((value >> 16) & 0xff);
    this.buffer.push((value >> 24) & 0xff);
  }

  private writeInt64(value: bigint): void {
    for (let i = 0; i < 8; i++) {
      this.buffer.push(Number(value & BigInt(0xff)));
      value >>= BigInt(8);
    }
  }

  private writeFloat64(value: number): void {
    const arr = new Float64Array([value]);
    const bytes = new Uint8Array(arr.buffer);
    for (let i = 0; i < bytes.length; i++) {
      this.buffer.push(bytes[i]!);  // loop bounds ensure valid index
    }
  }

  finish(): Uint8Array {
    return new Uint8Array(this.buffer);
  }
}

// =============================================================================
// Variant Decoder
// =============================================================================

/**
 * Decoder for Variant format
 */
class VariantDecoder {
  private data: Uint8Array;
  private offset = 0;
  private textDecoder = new TextDecoder();

  constructor(data: Uint8Array) {
    this.data = data;

    // Validate header
    if (data.length < 2) {
      throw new Error('Invalid Variant: too short');
    }

    const magic = this.readByte();
    if (magic !== VARIANT_MAGIC) {
      throw new Error(`Invalid Variant magic byte: ${magic}`);
    }

    const version = this.readByte();
    if (version !== VARIANT_VERSION) {
      throw new Error(`Unsupported Variant version: ${version}`);
    }
  }

  readValue(): unknown {
    if (this.offset >= this.data.length) {
      throw new Error('Unexpected end of Variant data');
    }

    const type = this.readByte();

    switch (type) {
      case VariantType.NULL:
        return null;
      case VariantType.BOOLEAN_TRUE:
        return true;
      case VariantType.BOOLEAN_FALSE:
        return false;
      case VariantType.INT8:
        return this.readInt8();
      case VariantType.INT16:
        return this.readInt16();
      case VariantType.INT32:
        return this.readInt32();
      case VariantType.INT64:
        return this.readInt64();
      case VariantType.FLOAT32:
        return this.readFloat32();
      case VariantType.FLOAT64:
        return this.readFloat64();
      case VariantType.DATE:
        return new Date(this.readInt32() * 86400000);
      case VariantType.TIMESTAMP:
      case VariantType.TIMESTAMP_NTZ:
        return new Date(Number(this.readInt64AsBigInt()));
      case VariantType.STRING:
        return this.readString();
      case VariantType.BINARY:
        return this.readBinary();
      case VariantType.ARRAY:
        return this.readArray();
      case VariantType.OBJECT:
        return this.readObject();
      default:
        throw new Error(`Unknown Variant type: ${type}`);
    }
  }

  private readByte(): number {
    const byte = this.data[this.offset++]
    if (byte === undefined) {
      throw new Error('Unexpected end of Variant data')
    }
    return byte
  }

  private readLength(): number {
    let result = 0;
    let shift = 0;
    let byte: number;

    do {
      byte = this.readByte();
      result |= (byte & 0x7f) << shift;
      shift += 7;
    } while (byte >= 0x80);

    return result;
  }

  private readInt8(): number {
    const value = this.data[this.offset]
    if (value === undefined) throw new Error('Unexpected end of Variant data')
    this.offset += 1
    return value > 127 ? value - 256 : value
  }

  private readInt16(): number {
    const b0 = this.data[this.offset]
    const b1 = this.data[this.offset + 1]
    if (b0 === undefined || b1 === undefined) throw new Error('Unexpected end of Variant data')
    const value = b0 | (b1 << 8)
    this.offset += 2
    return value > 32767 ? value - 65536 : value
  }

  private readInt32(): number {
    const b0 = this.data[this.offset]
    const b1 = this.data[this.offset + 1]
    const b2 = this.data[this.offset + 2]
    const b3 = this.data[this.offset + 3]
    if (b0 === undefined || b1 === undefined || b2 === undefined || b3 === undefined) {
      throw new Error('Unexpected end of Variant data')
    }
    const value = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
    this.offset += 4
    return value
  }

  private readInt64(): number {
    // Read as BigInt and convert to Number (may lose precision for very large values)
    return Number(this.readInt64AsBigInt())
  }

  private readInt64AsBigInt(): bigint {
    let result = BigInt(0)
    for (let i = 0; i < 8; i++) {
      const byte = this.data[this.offset + i]
      if (byte === undefined) throw new Error('Unexpected end of Variant data')
      result |= BigInt(byte) << BigInt(i * 8)
    }
    this.offset += 8
    // Handle sign
    if (result > BigInt('9223372036854775807')) {
      result -= BigInt('18446744073709551616')
    }
    return result
  }

  private readFloat32(): number {
    const arr = new Float32Array(
      this.data.buffer.slice(this.offset, this.offset + 4)
    )
    this.offset += 4
    return arr[0] ?? 0  // Float32Array always has length 1
  }

  private readFloat64(): number {
    const arr = new Float64Array(
      new Uint8Array(this.data.slice(this.offset, this.offset + 8)).buffer
    )
    this.offset += 8
    return arr[0] ?? 0  // Float64Array always has length 1
  }

  private readString(): string {
    const length = this.readLength();
    const bytes = this.data.slice(this.offset, this.offset + length);
    this.offset += length;
    return this.textDecoder.decode(bytes);
  }

  private readBinary(): Uint8Array {
    const length = this.readLength();
    const bytes = this.data.slice(this.offset, this.offset + length);
    this.offset += length;
    return bytes;
  }

  private readArray(): unknown[] {
    const length = this.readLength();
    const result: unknown[] = [];
    for (let i = 0; i < length; i++) {
      result.push(this.readValue());
    }
    return result;
  }

  private readObject(): Record<string, unknown> {
    const length = this.readLength();
    const result: Record<string, unknown> = {};
    for (let i = 0; i < length; i++) {
      const keyLength = this.readLength();
      const keyBytes = this.data.slice(this.offset, this.offset + keyLength);
      this.offset += keyLength;
      const key = this.textDecoder.decode(keyBytes);
      result[key] = this.readValue();
    }
    return result;
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Check if a value can be encoded as Variant
 */
export function isEncodable(value: unknown): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === 'boolean') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value === 'string') return true;
  if (typeof value === 'bigint') return true;
  if (value instanceof Date) return !isNaN(value.getTime());
  if (value instanceof Uint8Array) return true;
  if (Array.isArray(value)) return value.every(isEncodable);
  if (typeof value === 'object') {
    return Object.values(value).every(isEncodable);
  }
  return false;
}

/**
 * Get the estimated size of a Variant-encoded value
 */
export function estimateVariantSize(value: unknown): number {
  // Header
  let size = 2;

  if (value === null || value === undefined) {
    return size + 1;
  }

  if (typeof value === 'boolean') {
    return size + 1;
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      if (value >= -128 && value <= 127) return size + 2;
      if (value >= -32768 && value <= 32767) return size + 3;
      if (value >= -2147483648 && value <= 2147483647) return size + 5;
    }
    return size + 9; // float64
  }

  if (typeof value === 'bigint') {
    return size + 9;
  }

  if (typeof value === 'string') {
    const encoded = new TextEncoder().encode(value);
    return size + 1 + varintSize(encoded.length) + encoded.length;
  }

  if (value instanceof Date) {
    return size + 9;
  }

  if (value instanceof Uint8Array) {
    return size + 1 + varintSize(value.length) + value.length;
  }

  if (Array.isArray(value)) {
    let arraySize = size + 1 + varintSize(value.length);
    for (const item of value) {
      arraySize += estimateVariantSize(item) - 2; // Subtract header for nested
    }
    return arraySize;
  }

  if (typeof value === 'object') {
    const entries = Object.entries(value);
    let objSize = size + 1 + varintSize(entries.length);
    for (const [key, val] of entries) {
      const keyBytes = new TextEncoder().encode(key);
      objSize += varintSize(keyBytes.length) + keyBytes.length;
      objSize += estimateVariantSize(val) - 2; // Subtract header for nested
    }
    return objSize;
  }

  return size + 1;
}

/**
 * Calculate varint size for a number
 */
function varintSize(value: number): number {
  let size = 0;
  do {
    size++;
    value >>>= 7;
  } while (value > 0);
  return size;
}

/**
 * Deep equality check for Variant values
 */
export function variantEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }

  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!variantEquals(a[i], b[i])) return false;
    }
    return true;
  }

  if (typeof a === 'object' && typeof b === 'object') {
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b as object);
    if (aKeys.length !== bKeys.length) return false;
    for (const key of aKeys) {
      if (!variantEquals((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key])) {
        return false;
      }
    }
    return true;
  }

  return false;
}
