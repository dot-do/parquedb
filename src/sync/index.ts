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

// Git-inspired version control
export * from './hash'
export * from './commit'
export * from './refs'
export * from './branch-manager'

// Event-based merge engine
export * from './commutative-ops'
export * from './conflict-detection'
export * from './conflict-resolution'
export * from './event-merge'
