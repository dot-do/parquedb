/**
 * ParqueDB Module
 *
 * This is the main entry point for the ParqueDB module.
 * It re-exports all public types and the ParqueDB class.
 */

// Re-export types
export * from './types'

// Re-export validation utilities
export {
  validateNamespace,
  validateFilter,
  validateUpdateOperators,
  normalizeNamespace,
  validateEntityId,
  validateLocalId,
  normalizeEntityId,
  toFullId,
} from './validation'

// Re-export store utilities (for testing)
export { getEntityStore, getEventStore, getArchivedEventStore, getSnapshotStore, getQueryStatsStore, clearGlobalState } from './store'

// Re-export collection
export { CollectionImpl } from './collection'

// Re-export collections manager
export { CollectionManager, createCollection } from './collections'
export type { CollectionManagerContext } from './collections'

// Re-export snapshot manager
export { SnapshotManagerImpl } from './snapshots'

// Re-export event log
export { EventLogImpl } from './events'

// Re-export core
export { ParqueDBImpl } from './core'

// Re-export entity operations types and helpers
export {
  deriveTypeFromNamespace,
  isFieldRequired,
  hasDefault,
  validateFieldType,
  applySchemaDefaults,
  type EntityOperationsContext,
} from './entity-operations'

// Re-export relationship operations types and helpers
export {
  indexRelationshipsForEntity,
  unindexRelationshipsForEntity,
  applyRelationshipOperators,
  parseReverseRelation,
  getReverseRelatedIds,
  hydrateEntity,
  applyMaxInboundToEntity,
  getRelatedEntities,
  type RelationshipOperationsContext,
} from './relationship-operations'

// Re-export event operations types and helpers
export {
  recordEvent,
  flushEvents,
  archiveEvents as archiveEventsOp,
  reconstructEntityAtTime,
  reconstructFromEvents,
  binarySearchLastEventBeforeTime,
  getEntityHistory,
  getEntityAtVersion,
  computeDiff,
  revertEntity,
  type EventOperationsContext,
} from './event-operations'

// Re-export schema operations types and helpers
export {
  registerSchema,
  validateAgainstSchema,
  legacyValidateAgainstSchema,
  hasTypeSchema,
  getTypeSchema,
  getFieldSchema,
  isRelationshipField,
  isReverseRelationshipField,
  getRelationshipFields,
  getReverseRelationshipFields,
  type SchemaOperationsContext,
} from './schema-operations'
