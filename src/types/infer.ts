/**
 * Compile-time type inference for ParqueDB schema strings
 *
 * This module provides template literal types that parse schema definition strings
 * at compile time and infer the correct TypeScript types for entity fields.
 *
 * @example
 * ```typescript
 * // Schema string -> TypeScript type
 * 'string!'      -> string
 * 'string'       -> string | undefined
 * 'int!'         -> number
 * '-> User'      -> string | null
 * '<- Post.author[]' -> string[]
 * ```
 *
 * @module types/infer
 */

import type { Entity, EntityData } from './entity'
import type { Collection } from '../ParqueDB/types'
import type { SQLExecutor } from '../integrations/sql'
import type { ParqueDB } from '../ParqueDB'

// =============================================================================
// 1. Extract base type (strip modifiers)
// =============================================================================

/**
 * Strips modifiers from a schema type string to get the base type
 *
 * Modifiers that are stripped:
 * - `!` - required
 * - `#` - indexed
 * - `##` - unique indexed
 * - `?` - optional (explicit)
 * - `[]` - array
 * - `= value` - default value
 *
 * @example
 * ```typescript
 * ExtractBaseType<'string!##'>  // 'string'
 * ExtractBaseType<'int[]!'>     // 'int'
 * ExtractBaseType<'date = now'> // 'date'
 * ```
 */
type ExtractBaseType<S extends string> =
  // Handle default values first (they can have spaces)
  S extends `${infer Base} = ${string}` ? ExtractBaseType<Base> :
  // Required + unique index
  S extends `${infer Base}!##` ? Base :
  // Unique index + required (alternate order)
  S extends `${infer Base}##!` ? Base :
  // Unique index only
  S extends `${infer Base}##` ? Base :
  // Required + index
  S extends `${infer Base}!#` ? Base :
  // Index + required (alternate order)
  S extends `${infer Base}#!` ? Base :
  // Index only
  S extends `${infer Base}#` ? Base :
  // Array required
  S extends `${infer Base}[]!` ? Base :
  // Array
  S extends `${infer Base}[]` ? Base :
  // Required only
  S extends `${infer Base}!` ? Base :
  // Optional only
  S extends `${infer Base}?` ? Base :
  // No modifiers
  S

// =============================================================================
// 2. Check modifiers
// =============================================================================

/**
 * Determines if a schema type string represents a required field
 *
 * Required fields:
 * - End with `!` (e.g., 'string!', 'int!#')
 * - Arrays with `!` (e.g., 'string[]!')
 *
 * NOT required:
 * - Explicit optional `?`
 * - No modifier (implicitly optional)
 *
 * @example
 * ```typescript
 * IsRequired<'string!'>   // true
 * IsRequired<'string!#'>  // true
 * IsRequired<'string'>    // false
 * IsRequired<'string?'>   // false
 * ```
 */
type IsRequired<S extends string> =
  // Explicit optional overrides everything
  S extends `${string}?` ? false :
  // Check for required marker
  S extends `${string}!##` ? true :
  S extends `${string}##!` ? true :
  S extends `${string}!#` ? true :
  S extends `${string}#!` ? true :
  S extends `${string}[]!` ? true :
  S extends `${string}!` ? true :
  // Default: not required
  false

/**
 * Determines if a schema type string represents an array field
 *
 * @example
 * ```typescript
 * IsArray<'string[]'>   // true
 * IsArray<'string[]!'>  // true
 * IsArray<'string'>     // false
 * ```
 */
type IsArray<S extends string> =
  S extends `${string}[]${string}` ? true : false

// =============================================================================
// 3. Map base types to TypeScript
// =============================================================================

/**
 * Maps a base schema type string to its TypeScript equivalent
 *
 * | Schema Type | TypeScript |
 * |-------------|------------|
 * | string, text, markdown, uuid, email, url | string |
 * | varchar(n), char(n) | string |
 * | int, float, double, number | number |
 * | decimal(p,s) | number |
 * | boolean, bool | boolean |
 * | date, datetime, timestamp | Date |
 * | json | unknown |
 * | binary | Uint8Array |
 * | vector(n) | number[] |
 * | enum(a,b,c) | 'a' \| 'b' \| 'c' |
 */
type BaseTypeToTS<T extends string> =
  // String types
  T extends 'string' | 'text' | 'markdown' | 'uuid' | 'email' | 'url' ? string :
  T extends `varchar(${number})` ? string :
  T extends `char(${number})` ? string :
  // Numeric types
  T extends 'int' | 'integer' | 'float' | 'double' | 'number' ? number :
  T extends `decimal(${number},${number})` ? number :
  T extends `decimal(${number})` ? number :
  // Boolean types
  T extends 'boolean' | 'bool' ? boolean :
  // Date types
  T extends 'date' | 'datetime' | 'timestamp' ? Date :
  // JSON type
  T extends 'json' ? unknown :
  // Binary type
  T extends 'binary' | 'blob' ? Uint8Array :
  // Vector type
  T extends `vector(${number})` ? number[] :
  // Enum type
  T extends `enum(${infer Values})` ? ParseEnumValues<Values> :
  // Unknown type - fall back to unknown
  unknown

/**
 * Parses comma-separated enum values into a union type
 *
 * @example
 * ```typescript
 * ParseEnumValues<'draft,published,archived'> // 'draft' | 'published' | 'archived'
 * ```
 */
type ParseEnumValues<S extends string> =
  S extends `${infer First},${infer Rest}`
    ? Trim<First> | ParseEnumValues<Rest>
    : Trim<S>

/**
 * Trims leading and trailing whitespace from a string type
 */
type Trim<S extends string> =
  S extends ` ${infer R}` ? Trim<R> :
  S extends `${infer R} ` ? Trim<R> :
  S

// =============================================================================
// 4. Relationship type inference
// =============================================================================

/**
 * Parses forward relationship strings (-> Target)
 *
 * | Pattern | TypeScript |
 * |---------|------------|
 * | `-> User` | string \| null |
 * | `-> User[]` | string[] |
 * | `-> User.posts[]` | string[] |
 * | `-> User.post` | string \| null |
 *
 * Forward relationships point to another entity type.
 */
type ParseForwardRelation<S extends string> =
  // -> Target.field[] (explicit array via field)
  S extends `-> ${string}.${string}[]` ? string[] :
  // -> Target.field (singular via field)
  S extends `-> ${string}.${string}` ? string | null :
  // -> Target[] (explicit array)
  S extends `-> ${string}[]` ? string[] :
  // -> Target (singular)
  S extends `-> ${string}` ? string | null :
  never

/**
 * Parses backward/reverse relationship strings (<- Source.field)
 *
 * | Pattern | TypeScript |
 * |---------|------------|
 * | `<- Post.author` | string[] |
 * | `<- Post.author[]` | string[] |
 *
 * Backward relationships always return arrays since they represent
 * "all entities that reference this one".
 */
type ParseBackwardRelation<S extends string> =
  // <- Source.field[] (explicit array)
  S extends `<- ${string}.${string}[]` ? string[] :
  // <- Source.field (implicit array)
  S extends `<- ${string}.${string}` ? string[] :
  // <- Source[] (explicit array)
  S extends `<- ${string}[]` ? string[] :
  // <- Source (implicit array)
  S extends `<- ${string}` ? string[] :
  never

/**
 * Parses fuzzy relationship strings (~> Target, <~ Source)
 *
 * Fuzzy relationships are established through similarity matching
 * rather than explicit user links.
 *
 * | Pattern | TypeScript |
 * |---------|------------|
 * | `~> Topic` | string \| null |
 * | `~> Topic[]` | string[] |
 * | `<~ Article` | string[] |
 * | `<~ Article[]` | string[] |
 */
type ParseFuzzyRelation<S extends string> =
  // Forward fuzzy array
  S extends `~> ${string}[]` ? string[] :
  // Forward fuzzy singular
  S extends `~> ${string}` ? string | null :
  // Backward fuzzy array
  S extends `<~ ${string}[]` ? string[] :
  // Backward fuzzy singular (still returns array)
  S extends `<~ ${string}` ? string[] :
  never

// =============================================================================
// 5. Main field type inference
// =============================================================================

/**
 * Infers the TypeScript type for a schema field definition string
 *
 * This is the main type utility for converting schema strings to TypeScript types.
 *
 * @example
 * ```typescript
 * InferFieldType<'string!'>        // string
 * InferFieldType<'string'>         // string | undefined
 * InferFieldType<'int!#'>          // number
 * InferFieldType<'-> User'>        // string | null
 * InferFieldType<'<- Post.author[]'> // string[]
 * InferFieldType<'boolean = false'> // boolean | undefined
 * ```
 */
export type InferFieldType<S extends string> =
  // Forward relationships (-> Target)
  S extends `-> ${string}` ? ParseForwardRelation<S> :
  // Backward relationships (<- Source.field)
  S extends `<- ${string}` ? ParseBackwardRelation<S> :
  // Forward fuzzy relationships (~> Target)
  S extends `~> ${string}` ? ParseFuzzyRelation<S> :
  // Backward fuzzy relationships (<~ Source)
  S extends `<~ ${string}` ? ParseFuzzyRelation<S> :
  // Regular field types
  IsArray<S> extends true
    ? BaseTypeToTS<ExtractBaseType<S>>[]
    : IsRequired<S> extends true
      ? BaseTypeToTS<ExtractBaseType<S>>
      : BaseTypeToTS<ExtractBaseType<S>> | undefined

// =============================================================================
// 6. Entity data inference
// =============================================================================

/**
 * Infers the entity data shape from a schema definition object
 *
 * Filters out $-prefixed keys (config fields like $id, $layout, $options)
 * and infers types for all field definitions.
 *
 * @example
 * ```typescript
 * type PostSchema = {
 *   title: 'string!'
 *   content: 'text'
 *   views: 'int = 0'
 *   author: '-> User'
 *   $id: 'slug'  // Excluded
 * }
 *
 * type PostData = InferEntityData<PostSchema>
 * // {
 * //   title: string
 * //   content: string | undefined
 * //   views: number | undefined
 * //   author: string | null
 * // }
 * ```
 */
export type InferEntityData<TSchema> = TSchema extends Record<string, unknown>
  ? {
      [K in keyof TSchema as K extends `$${string}` ? never : K]: TSchema[K] extends string
        ? InferFieldType<TSchema[K]>
        : TSchema[K] extends { type: infer T extends string }
          ? InferFieldType<T>
          : unknown
    }
  : EntityData

/**
 * Infers the full Entity type from a schema definition
 *
 * Combines the inferred data fields with the base Entity fields
 * ($id, $type, name, audit fields, etc.)
 */
export type InferEntity<TSchema> = Entity<InferEntityData<TSchema>>

// =============================================================================
// 7. Collection Create Input (ergonomic for typed collections)
// =============================================================================

/**
 * Checks if a schema field string is a backward relationship
 * Backward relationships (<-) and backward fuzzy (<~) are computed, not set on create
 */
type IsBackwardRelationship<S extends string> =
  S extends `<- ${string}` ? true :
  S extends `<~ ${string}` ? true :
  false

/**
 * Infers the data shape for create operations from a schema
 *
 * This differs from InferEntityData in that it:
 * - Excludes backward relationships (`<-`, `<~`) since they're computed
 * - Excludes $-prefixed config fields
 * - Only includes fields that can be set on create
 */
export type InferCreateData<TSchema> = TSchema extends Record<string, unknown>
  ? {
      [K in keyof TSchema as
        // Exclude $-prefixed config fields
        K extends `$${string}` ? never :
        // Exclude backward relationships (computed fields)
        TSchema[K] extends string
          ? IsBackwardRelationship<TSchema[K]> extends true ? never : K
          : K
      ]: TSchema[K] extends string
        ? InferFieldType<TSchema[K]>
        : TSchema[K] extends { type: infer T extends string }
          ? InferFieldType<T>
          : unknown
    }
  : EntityData

/**
 * Determines which fields from T are required (not optional/undefined)
 */
type RequiredKeys<T> = {
  [K in keyof T]-?: undefined extends T[K] ? never : K
}[keyof T]

/**
 * Determines which fields from T are optional (can be undefined)
 */
type OptionalKeys<T> = {
  [K in keyof T]-?: undefined extends T[K] ? K : never
}[keyof T]

/** Reserved entity fields that shouldn't be in create input */
type ReservedCreateFields = '$id' | '$type' | 'name' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy' | 'deletedAt' | 'deletedBy' | 'version'

/**
 * Input type for Collection.create() that makes $type and name optional
 *
 * When using typed collections (e.g., db.Post.create()), requiring $type is
 * redundant since the type is known from the collection. Similarly, name can
 * often be derived from common fields like title, label, etc.
 *
 * This type:
 * - Makes `$type` optional (can be inferred from collection)
 * - Makes `name` optional (can be derived from data fields)
 * - Requires all required fields from the entity schema
 * - Makes optional fields optional
 * - Excludes backward relationships (computed fields)
 *
 * @example
 * ```typescript
 * // With CollectionCreateInput, this works:
 * await db.Post.create({
 *   slug: 'hello-world',
 *   title: 'Hello World',
 *   content: 'My content'
 * })
 *
 * // No need for $type or name!
 * // Backward relationships like 'posts' are excluded
 * ```
 */
export type CollectionCreateInput<T extends EntityData = EntityData> =
  // For EntityData (untyped), fall back to loose typing
  unknown extends T[keyof T]
    ? { $type?: string; name?: string; [key: string]: unknown }
    : // For typed collections, make $type and name optional
      {
        /** Entity type (optional - derived from collection name) */
        $type?: string
        /** Display name (optional - derived from title/label/name field) */
        name?: string
      } & {
        // Required fields from T (excluding reserved fields)
        [K in RequiredKeys<T> as K extends ReservedCreateFields ? never : K]: T[K]
      } & {
        // Optional fields from T (excluding reserved fields)
        [K in OptionalKeys<T> as K extends ReservedCreateFields ? never : K]?: T[K]
      }

// =============================================================================
// 8. Typed DB instance
// =============================================================================

/**
 * Input schema for the DB() factory function
 *
 * Each key is a collection name, each value is either:
 * - A schema object with field definitions
 * - 'flexible' for schema-less mode
 */
export type DBSchemaInput = Record<string, Record<string, unknown> | 'flexible'>

/**
 * A typed collection with separate types for entity data (read) and create input (write)
 *
 * This interface extends Collection to properly type the create method
 * with fields that can actually be set (excluding computed backward relationships).
 */
export interface TypedCollection<TEntity extends EntityData, TCreate extends EntityData> extends Omit<Collection<TEntity>, 'create'> {
  /**
   * Create a new entity
   *
   * @param data - Entity data (only settable fields, excludes backward relationships)
   * @param options - Create options
   * @returns Created entity with all fields including computed ones
   */
  create(data: CollectionCreateInput<TCreate>, options?: import('./options').CreateOptions): Promise<Entity<TEntity>>
}

/**
 * Infers typed Collection interfaces from a database schema
 *
 * For each collection:
 * - Read operations (find, get) return entities with ALL fields including backward relationships
 * - Create operations only accept settable fields (excludes backward relationships)
 *
 * @example
 * ```typescript
 * type Schema = {
 *   User: { email: 'string!', name: 'string', posts: '<- Post.author[]' }
 *   Post: { title: 'string!', author: '-> User' }
 * }
 *
 * // db.User.find() returns entities with { email, name, posts }
 * // db.User.create() only requires { email, name } - posts is computed
 * ```
 */
export type InferCollections<TSchema extends DBSchemaInput> = {
  [K in keyof TSchema]: TSchema[K] extends 'flexible'
    ? Collection<EntityData>
    : TSchema[K] extends Record<string, unknown>
      ? TypedCollection<InferEntityData<TSchema[K]>, InferCreateData<TSchema[K]>>
      : Collection<EntityData>
}

/**
 * A typed ParqueDB instance with inferred collection types
 *
 * This is the return type of DB<TSchema>() when called with a typed schema.
 * It provides:
 * - All base ParqueDB methods
 * - Typed collection accessors (e.g., db.User, db.Post)
 * - SQL executor
 *
 * @example
 * ```typescript
 * const db = DB({
 *   User: { email: 'string!#', name: 'string' },
 *   Post: { title: 'string!', author: '-> User' }
 * })
 *
 * // db.User is typed as Collection<{ email: string; name: string | undefined }>
 * const users = await db.User.find()
 * users.items[0].email  // string (properly typed!)
 *
 * // db.Post is typed as Collection<{ title: string; author: string | null }>
 * const posts = await db.Post.find()
 * posts.items[0].title  // string
 * posts.items[0].author // string | null
 * ```
 */
export type TypedDBInstance<TSchema extends DBSchemaInput> =
  ParqueDB &
  InferCollections<TSchema> &
  { sql: SQLExecutor }
