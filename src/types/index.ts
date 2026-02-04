/**
 * ParqueDB Type Definitions
 *
 * This module consolidates all type definitions for ParqueDB, a hybrid
 * relational/document/graph database built on Apache Parquet.
 *
 * ## Module Overview
 *
 * | Module        | Description                                                    |
 * |---------------|----------------------------------------------------------------|
 * | entity        | Core entity types: Entity, EntityId, AuditFields, Relationship |
 * | filter        | MongoDB-style query operators: $eq, $gt, $in, $or, $text, etc  |
 * | update        | MongoDB-style update operators: $set, $inc, $push, $link, etc  |
 * | options       | Operation options: FindOptions, CreateOptions, etc             |
 * | schema        | Schema definitions: TypeDefinition, FieldDefinition, etc       |
 * | storage       | Storage backend interface: StorageBackend, FileStat, etc       |
 * | integrations  | GraphDL/IceType schema integration, RPC types                  |
 * | worker        | Cloudflare Worker types: Env, DO interfaces, flush config      |
 * | result        | Type-safe error handling: Result<T, E>, Ok, Err                |
 * | visibility    | Access control: public, unlisted, private, CORS config         |
 * | cast          | Type cast utilities to eliminate double assertions             |
 *
 * ## Quick Start
 *
 * @example
 * ```typescript
 * import type { Entity, Filter, Update, FindOptions, Schema } from 'parquedb'
 *
 * // Define a typed entity
 * interface Post { title: string; content: string; views: number }
 * const post: Entity<Post> = await db.Posts.get('post-1')
 *
 * // Query with MongoDB-style filters
 * const filter: Filter = { status: 'published', views: { $gte: 100 } }
 *
 * // Update with operators
 * const update: Update = { $set: { status: 'featured' }, $inc: { views: 1 } }
 * ```
 *
 * ## Dependency Order (to prevent circular imports)
 *
 * | Layer | Files                                          | Dependencies         |
 * |-------|------------------------------------------------|----------------------|
 * | 0     | result, visibility, storage, entity, filter    | None                 |
 * | 1     | schema, update, options                        | Layer 0 only         |
 * | 2     | worker, integrations                           | Layer 0-1, externals |
 * | 3     | cast                                           | All layers           |
 *
 * When adding new type files:
 * 1. Import directly from specific files, not from './index'
 * 2. Use `import type` for type-only imports
 * 3. Document the dependency layer in this header
 *
 * @module types
 */

// =============================================================================
// Layer 0: Base types with no internal dependencies
// =============================================================================

// Result type (type-safe error handling)
export * from './result'

// Visibility types (public, unlisted, private)
export * from './visibility'

// Storage types (backend interface, paths)
export * from './storage'

// Entity types (core entity structure, branded IDs, audit fields)
export * from './entity'

// Filter types (MongoDB-style query operators)
export * from './filter'

// Common interfaces (typed replacements for Record<string, unknown>)
export * from './common'

// =============================================================================
// Layer 1: Types that depend only on Layer 0
// =============================================================================

// Schema types (type definitions, field definitions)
// Depends on: visibility
export * from './schema'

// Update types (MongoDB-style update operators)
// Depends on: entity, filter
export * from './update'

// Options types (find, get, create, update, delete options)
// Depends on: entity, filter
export * from './options'

// =============================================================================
// Layer 2: Types with external or deeper dependencies
// =============================================================================

// Worker types (Env, DO types, RPC types)
// Depends on: ../embeddings/workers-ai
export * from './worker'

// Integration types (GraphDL, IceType, capnweb)
// Depends on: schema, @graphdl/core, @icetype/core
export * from './integrations'

// =============================================================================
// Layer 3: Utility types that depend on multiple layers
// =============================================================================

// Type cast utilities (eliminates double assertions)
// Depends on: entity, storage, worker
export * from './cast'
