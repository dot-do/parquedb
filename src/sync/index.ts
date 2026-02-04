/**
 * ParqueDB Sync Module
 *
 * This module provides:
 * - Push/Pull/Sync operations for syncing databases between local and remote storage
 * - Content-addressed commit storage
 * - Branch and tag reference management
 * - Database state snapshots
 * - Deterministic hashing
 * - Event-based merge engine with conflict detection and resolution
 */

// Push/Pull/Sync operations
export * from './manifest'
export * from './engine'
export * from './client'

// Git-inspired version control
export * from './hash'
export * from './commit'
export * from './refs'
export * from './branch-manager'
export * from './state-store'
export {
  type ObjectStore,
  computeObjectHash,
  getObjectPath,
  createObjectStore,
  saveObject,
  loadObject as loadObjectFromStore,
} from './object-store'

// Event-based merge engine
export * from './commutative-ops'
export * from './conflict-detection'
export * from './conflict-resolution'
export * from './event-merge'
export {
  type MergeStatus,
  type ConflictResolutionStrategy,
  type MergeState,
  loadMergeState,
  saveMergeState,
  clearMergeState,
  hasMergeInProgress,
  addConflict,
  resolveConflict as resolveMergeConflict,
  getUnresolvedConflicts as getUnresolvedMergeConflicts,
  getConflictsByPattern,
  allConflictsResolved,
  createMergeState,
} from './merge-state'
export * from './merge-commit'
export * from './merge-engine'

// Common ancestor algorithm
export * from './common-ancestor'

// Distributed locking
export * from './lock'

// Schema evolution and snapshots
export * from './schema-snapshot'
export * from './schema-evolution'
