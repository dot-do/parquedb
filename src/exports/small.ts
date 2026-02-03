/**
 * ParqueDB Small Export (~50KB target)
 *
 * A minimal but complete ParqueDB package that includes:
 * - Core CRUD operations (ParqueDB, Collection)
 * - MemoryBackend (for testing) and basic storage types
 * - Basic filters and update operators
 * - Basic relationship support
 *
 * Excludes:
 * - Materialized Views
 * - Secondary indexes (FTS, Vector, Bloom)
 * - Time-travel
 * - RPC/Client
 * - Observability hooks
 * - SQL integration
 * - Migration utilities
 * - Embeddings
 * - Iceberg/Delta backends
 *
 * @packageDocumentation
 * @module parquedb/small
 */

// =============================================================================
// Core Classes
// =============================================================================

export {
  ParqueDB,
  type ParqueDBConfig,
  type EventLogConfig,
  DEFAULT_EVENT_LOG_CONFIG,
} from '../ParqueDB'

export {
  Collection,
  clearGlobalStorage,
} from '../Collection'

// =============================================================================
// Core Types (Entity)
// =============================================================================

export {
  // EntityId types and helpers
  type Namespace,
  type Id,
  type EntityId,
  entityId,
  parseEntityId,
  isValidEntityId,
  isValidNamespace,
  isValidId,
  toEntityId,
  toEntityIdOrNull,
  toNamespace,
  toId,
  asEntityId,
  asNamespace,
  asId,
  SYSTEM_ACTOR,

  // Entity types
  type Entity,
  type EntityRef,
  type EntityRecord,
  type AuditFields,
  type CreateInput,
  type ReplaceInput,

  // Relationship types
  type RelLink,
  type RelSet,
  type Relationship,
  type MatchMode,

  // Result types
  type UpdateResult,
  type DeleteResult,
  type PaginatedResult,

  // Variant types
  type Variant,
  type VariantValue,
  type VariantPrimitive,
} from '../types/entity'

// =============================================================================
// Core Types (Filter)
// =============================================================================

export {
  // Main filter type
  type Filter,
  type FieldFilter,
  type FieldOperator,

  // Comparison operators
  type EqOperator,
  type NeOperator,
  type GtOperator,
  type GteOperator,
  type LtOperator,
  type LteOperator,
  type InOperator,
  type NinOperator,
  type ComparisonOperator,

  // String operators
  type RegexOperator,
  type StartsWithOperator,
  type EndsWithOperator,
  type ContainsOperator,
  type StringOperator,

  // Array operators
  type AllOperator,
  type ElemMatchOperator,
  type SizeOperator,
  type ArrayOperator,

  // Existence operators
  type ExistsOperator,
  type TypeOperator,
  type ExistenceOperator,

  // Logical operators
  type AndOperator,
  type OrOperator,
  type NotOperator,
  type NorOperator,
  type LogicalOperator,

  // Type guards
  isComparisonOperator,
  isStringOperator,
  isArrayOperator,
  isExistenceOperator,
  isFieldOperator,
  hasLogicalOperators,
  hasSpecialOperators,
} from '../types/filter'

// =============================================================================
// Core Types (Update)
// =============================================================================

export {
  // Update input
  type UpdateInput,
  type Update,

  // Field operators
  type SetOperator,
  type UnsetOperator,
  type RenameOperator,
  type SetOnInsertOperator,

  // Numeric operators
  type IncOperator,
  type MulOperator,
  type MinOperator,
  type MaxOperator,

  // Array operators
  type PushOperator,
  type PushModifiers,
  type PullOperator,
  type PullAllOperator,
  type AddToSetOperator,
  type PopOperator,

  // Date operators
  type CurrentDateOperator,

  // Relationship operators
  type LinkOperator,
  type UnlinkOperator,

  // Type guards
  hasFieldOperators,
  hasNumericOperators,
  hasArrayOperators,
  hasRelationshipOperators,
  getUpdateOperatorTypes,
} from '../types/update'

// =============================================================================
// Core Types (Options)
// =============================================================================

export {
  // Sort and projection
  type SortDirection,
  type SortSpec,
  type Projection,
  normalizeSortDirection,

  // Populate
  type PopulateOptions,
  type PopulateSpec,

  // Find/Get options
  type FindOptions,
  type GetOptions,
  type RelatedOptions,

  // Write options
  type CreateOptions,
  type UpdateOptions,
  type DeleteOptions,
  type BulkOptions,
  type ValidationMode,

  // History options
  type HistoryOptions,
} from '../types/options'

// =============================================================================
// Storage Backends
// =============================================================================

export {
  MemoryBackend,
} from '../storage/MemoryBackend'

export type {
  StorageBackend,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
} from '../types/storage'

// =============================================================================
// Query Utilities
// =============================================================================

export {
  matchesFilter,
  createPredicate,
} from '../query/filter'

export {
  applyUpdate,
} from '../query/update'

// =============================================================================
// Mutation Operators
// =============================================================================

export {
  applyOperators,
  getField,
  setField,
  unsetField,
  validateUpdateOperators,
} from '../mutation/operators'

// =============================================================================
// Utility Functions
// =============================================================================

export {
  // Comparison utilities
  isNullish,
  isDefined,
  deepEqual,
  compareValues,
  getNestedValue,
  deepClone,
  getValueType,

  // ID generation
  generateId,
  generateULID,
  getUUID,
} from '../utils'

// =============================================================================
// Errors (minimal set)
// =============================================================================

export {
  ParqueDBError,
  ErrorCode,
  ValidationError as ParqueDBValidationError,
  NotFoundError as ParqueDBNotFoundError,
  EntityNotFoundError as ParqueDBEntityNotFoundError,
  ConflictError,
  VersionConflictError as ParqueDBVersionConflictError,
  AlreadyExistsError as ParqueDBAlreadyExistsError,
  isParqueDBError,
  isValidationError,
  isNotFoundError,
  isEntityNotFoundError,
  isConflictError,
  isVersionConflictError,
  wrapError,
} from '../errors'

// =============================================================================
// Version
// =============================================================================

export const VERSION = '0.1.0'
export const EXPORT_TYPE = 'small' as const
