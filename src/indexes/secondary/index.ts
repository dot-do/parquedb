/**
 * Secondary Index Exports
 *
 * Hash indexes for ParqueDB
 *
 * NOTE: SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for range queries.
 */

// Key Encoder
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

// Hash Index
export { HashIndex, buildHashIndex } from './hash'

// Sharded Hash Index
export {
  ShardedHashIndex,
  loadShardedHashIndex,
  type ShardManifest,
  type ShardInfo,
} from './sharded-hash'
