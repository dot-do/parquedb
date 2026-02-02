/**
 * ParqueDB Module
 *
 * This is the main entry point for the ParqueDB module.
 * It re-exports all public types and the ParqueDB class.
 */

// Re-export types
export * from './types'

// Re-export validation utilities
export { validateNamespace, validateFilter, validateUpdateOperators, normalizeNamespace } from './validation'

// Re-export store utilities (for testing)
export { getEntityStore, getEventStore, getArchivedEventStore, getSnapshotStore, getQueryStatsStore, clearGlobalState } from './store'

// Re-export collection
export { CollectionImpl } from './collection'

// Re-export core
export { ParqueDBImpl } from './core'
