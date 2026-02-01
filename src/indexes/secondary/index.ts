/**
 * Secondary Index Exports
 *
 * Hash and SST (B-tree) indexes for ParqueDB
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

// SST Index
export { SSTIndex, buildSSTIndex } from './sst'

// Sharded Indexes
export {
  ShardedHashIndex,
  loadShardedHashIndex,
  type ShardManifest,
  type ShardInfo,
} from './sharded-hash'

export {
  ShardedSSTIndex,
  loadShardedSSTIndex,
} from './sharded-sst'
