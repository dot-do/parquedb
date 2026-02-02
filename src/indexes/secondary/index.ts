/**
 * Secondary Index Exports
 *
 * NOTE: Hash and SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for equality and range queries.
 *
 * Key encoder utilities are still exported for backward compatibility.
 */

// Key Encoder (kept for backward compatibility with existing serialized data)
export {
  encodeKey,
  decodeKey,
  compareKeys,
  createBoundaryKey,
  hashKey,
  keyToHex,
  hexToKey,
  encodeCompositeKey,
  decodeCompositeKey,
} from './key-encoder'
