/**
 * Shared types for sync module
 *
 * This file contains type definitions shared between commit.ts and schema-snapshot.ts
 * to avoid circular dependencies.
 */

// =============================================================================
// Schema Snapshot Types
// =============================================================================

/**
 * Relationship metadata for a field
 */
export interface SchemaFieldRelationship {
  readonly target: string          // Target collection name
  readonly reverse?: string | undefined        // Reverse relationship name
  readonly direction: 'outbound' | 'inbound'  // -> or <-
}

/**
 * Field definition snapshot
 */
export interface SchemaFieldSnapshot {
  readonly name: string
  readonly type: string              // 'string', 'int', 'boolean', 'string!', 'int?', '-> User', etc.
  readonly required: boolean         // true if field has '!' modifier
  readonly indexed: boolean          // true if field has '#' modifier
  readonly unique: boolean           // true if field has '@' modifier
  readonly array: boolean            // true if field has '[]'
  readonly default?: unknown | undefined
  readonly relationship?: SchemaFieldRelationship | undefined
}

/**
 * Collection schema options
 */
export interface CollectionSchemaOptions {
  readonly includeDataVariant?: boolean | undefined
}

/**
 * Collection schema snapshot
 */
export interface CollectionSchemaSnapshot {
  readonly name: string
  readonly hash: string              // SHA256 of collection schema
  readonly fields: readonly SchemaFieldSnapshot[]
  readonly version: number           // Incrementing version for this collection
  readonly options?: CollectionSchemaOptions | undefined
}

/**
 * Complete schema snapshot at a point in time
 */
export interface SchemaSnapshot {
  readonly hash: string              // SHA256 of full schema
  readonly configHash: string        // SHA256 of parquedb.config.ts content (if available)
  readonly collections: Readonly<Record<string, CollectionSchemaSnapshot>>
  readonly capturedAt: number        // Timestamp
  commitHash?: string | undefined                // Associated commit hash (mutable for backward compatibility)
}

// =============================================================================
// Commit Types
// =============================================================================

/**
 * Collection state in a commit
 */
export interface CollectionState {
  readonly dataHash: string            // SHA256 of data.parquet
  readonly schemaHash: string          // SHA256 of schema
  readonly rowCount: number
}

/**
 * Relationship state in a commit
 */
export interface RelationshipState {
  readonly forwardHash: string
  readonly reverseHash: string
}

/**
 * Event log position in a commit
 */
export interface EventLogPosition {
  readonly segmentId: string
  readonly offset: number
}

/**
 * Database state in a commit
 */
export interface CommitState {
  readonly collections: Readonly<Record<string, CollectionState>>
  readonly relationships: RelationshipState
  readonly eventLogPosition: EventLogPosition
  readonly schema?: SchemaSnapshot | undefined   // Schema snapshot at this commit
}

/**
 * Represents a database commit with state snapshot
 */
export interface DatabaseCommit {
  readonly hash: string                    // SHA256 of commit contents (excluding hash field)
  readonly parents: readonly string[]      // Parent commit hashes (empty for initial, 2 for merge)
  readonly timestamp: number
  readonly author: string
  readonly message: string
  readonly state: CommitState
}

/**
 * Options for creating a commit
 */
export interface CommitOptions {
  readonly message: string
  readonly author?: string | undefined
  readonly parents?: readonly string[] | undefined
}

/**
 * Database state snapshot for commit creation
 * Note: Mutable because it's built incrementally during snapshot
 */
export interface DatabaseState {
  collections: Record<string, CollectionState>
  relationships: RelationshipState
  eventLogPosition: EventLogPosition
  schema?: SchemaSnapshot | undefined   // Optional schema snapshot
}
