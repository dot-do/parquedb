/**
 * Integration types for GraphDL and IceType
 * These provide optional schema definition via external packages
 *
 * Updated for @graphdl/core@0.3.0 and @icetype/core@0.2.0
 */

// =============================================================================
// GraphDL Integration (@graphdl/core)
// =============================================================================

// Re-export types from @graphdl/core for convenience
export type {
  ParsedGraph as GraphDLSchema,
  ParsedEntity as GraphDLEntity,
  ParsedField as GraphDLField,
  RelationshipOperator as GraphDLOperator,
  RelationshipDirection as GraphDLDirection,
  RelationshipMatchMode as GraphDLMatchMode,
  EntityDirectives,
} from '@graphdl/core'

// Re-export Graph function for convenience
export { Graph } from '@graphdl/core'

/**
 * Legacy GraphDLRelation interface for backward compatibility
 * Maps to the new ParsedField relation properties
 */
export interface GraphDLRelation {
  operator: import('@graphdl/core').RelationshipOperator
  direction: import('@graphdl/core').RelationshipDirection
  matchMode: import('@graphdl/core').RelationshipMatchMode
  targetType: string
  backref?: string
  threshold?: number  // For fuzzy matches
  isArray: boolean
}

/**
 * Convert GraphDL schema (ParsedGraph) to ParqueDB schema
 *
 * @param graphdl - A ParsedGraph from Graph() function in @graphdl/core
 * @returns ParqueDB Schema object
 */
export function fromGraphDL(graphdl: import('@graphdl/core').ParsedGraph): import('./schema').Schema {
  // Validate input
  if (!graphdl || !graphdl.entities) {
    throw new Error('Invalid GraphDL schema: missing entities map')
  }

  const schema: import('./schema').Schema = {}

  for (const [name, entity] of graphdl.entities) {
    const typeDef: import('./schema').TypeDefinition = {}

    if (entity.$type) {
      typeDef.$type = entity.$type
    }

    // Handle directives from the new API
    if (entity.directives) {
      if (entity.directives.$partitionBy) {
        typeDef.$shred = entity.directives.$partitionBy as string[]
      }
      if (entity.directives.$index) {
        typeDef.$indexes = (entity.directives.$index as string[][]).map((fields: string[], i: number) => ({
          name: `idx_${name}_${i}`,
          fields: fields.map((f: string) => ({ field: f })),
        }))
      }
    }

    for (const [fieldName, field] of entity.fields) {
      if (field.isRelation) {
        // Convert to ParqueDB relationship string using new API properties
        const operator = field.operator || '->'
        const target = field.relatedType || field.type
        const backref = field.backref || fieldName
        const array = field.isArray ? '[]' : ''

        typeDef[fieldName] = `${operator} ${target}.${backref}${array}`
      } else {
        // Regular field
        // isRequired means required (! modifier in new API)
        const required = field.isRequired ? '!' : ''
        const array = field.isArray ? '[]' : ''
        let typeStr = `${field.type}${array}${required}`

        // Handle default values
        if (field.default !== undefined) {
          typeStr += ` = ${JSON.stringify(field.default)}`
        }

        typeDef[fieldName] = typeStr
      }
    }

    schema[name] = typeDef
  }

  return schema
}

// =============================================================================
// IceType Integration (@icetype/core)
// =============================================================================

// Re-export types from @icetype/core for convenience
export type {
  IceTypeSchema,
  FieldDefinition as IceTypeFieldDefinition,
  RelationDefinition as IceTypeRelationDefinition,
  SchemaDirectives as IceTypeSchemaDirectives,
  VectorDirective,
  IndexDirective,
} from '@icetype/core'

// Re-export compiler functions
export { graphToIceType, compile as compileIceType } from '@icetype/core'

/**
 * Legacy IceTypeField interface for backward compatibility
 * Maps to the new FieldDefinition from @icetype/core
 */
export interface IceTypeField {
  name: string
  type: string
  required: boolean
  default?: unknown
  relation?: {
    operator: import('@icetype/core').RelationOperator
    targetType: string
    inverse?: string
  }
}

/**
 * Legacy IceTypeParsedSchema interface for backward compatibility
 * In the new API, use graphToIceType() which returns Map<string, IceTypeSchema>
 */
export interface IceTypeParsedSchema {
  schemas: Map<string, import('@icetype/core').IceTypeSchema>
  getSchema(name: string): import('@icetype/core').IceTypeSchema | undefined
}

/**
 * Convert IceType schema to ParqueDB schema
 *
 * Accepts either:
 * - Legacy IceTypeParsedSchema (for backward compatibility)
 * - Map<string, IceTypeSchema> from graphToIceType() (new API)
 *
 * @param icetype - IceType schema collection
 * @returns ParqueDB Schema object
 */
export function fromIceType(
  icetype: IceTypeParsedSchema | Map<string, import('@icetype/core').IceTypeSchema>
): import('./schema').Schema {
  // Handle both legacy and new API
  let schemas: Map<string, import('@icetype/core').IceTypeSchema>

  if (icetype instanceof Map) {
    // New API: direct Map from graphToIceType()
    schemas = icetype
  } else if (icetype && 'schemas' in icetype && icetype.schemas instanceof Map) {
    // Legacy API: IceTypeParsedSchema wrapper
    schemas = icetype.schemas
  } else {
    throw new Error('Invalid IceType schema: must be a Map or have a schemas Map property')
  }

  const schema: import('./schema').Schema = {}

  // Extended IceTypeSchema type for legacy properties
  type ExtendedIceSchema = import('@icetype/core').IceTypeSchema & {
    $type?: string
    $description?: string
    $index?: unknown[]
    $unique?: string[][]
    $fts?: (string | { field: string; language?: string; weight?: number })[]
    $vector?: { field: string; dimensions: number; metric?: string }[]
  }

  for (const [name, iceSchema] of schemas) {
    const typeDef: import('./schema').TypeDefinition = {}
    const extSchema = iceSchema as ExtendedIceSchema

    // Handle directives from the new IceTypeSchema structure
    const directives = iceSchema.directives || {}

    // Preserve $type if it was passed through
    if (extSchema.$type) {
      typeDef.$type = extSchema.$type
    }

    // Handle $description
    if (extSchema.$description) {
      typeDef.$description = extSchema.$description
    }

    // Handle partitioning hint from directives
    if (directives.partitionBy && directives.partitionBy.length > 0) {
      typeDef.$shred = directives.partitionBy
    }

    // Handle indexes - prefer legacy $index format for tests, fall back to directives.index
    const legacyIndex = extSchema.$index
    if (legacyIndex && Array.isArray(legacyIndex) && legacyIndex.length > 0) {
      typeDef.$indexes = []
      for (let i = 0; i < legacyIndex.length; i++) {
        const indexDef = legacyIndex[i]
        if (Array.isArray(indexDef)) {
          typeDef.$indexes.push({
            name: `idx_${name}_${i}`,
            fields: indexDef.map((f: string) => ({ field: f })),
          })
        } else if (typeof indexDef === 'object' && indexDef !== null) {
          const def = indexDef as { name?: string; fields?: unknown[]; unique?: boolean; sparse?: boolean }
          const idx: import('./schema').IndexDefinition = {
            name: def.name || `idx_${name}_${i}`,
            fields: (def.fields || []).map((f: unknown) =>
              typeof f === 'string' ? { field: f } : f as { field: string }
            ),
          }
          if (def.unique) idx.unique = true
          if (def.sparse) idx.sparse = true
          typeDef.$indexes.push(idx)
        }
      }
    } else if (directives.index && directives.index.length > 0) {
      // Fall back to new directives format
      typeDef.$indexes = directives.index.map((indexDef, i: number) => {
        const idx: import('./schema').IndexDefinition = {
          name: indexDef.name || `idx_${name}_${i}`,
          fields: indexDef.fields.map((f: string) => ({ field: f })),
        }
        if (indexDef.unique) idx.unique = true
        return idx
      })
    }

    // Handle $unique constraints - convert to unique indexes
    const legacyUnique = extSchema.$unique
    if (legacyUnique && Array.isArray(legacyUnique) && legacyUnique.length > 0) {
      if (!typeDef.$indexes) {
        typeDef.$indexes = []
      }
      for (const uniqueFields of legacyUnique) {
        typeDef.$indexes.push({
          name: `unique_${name}_${uniqueFields.join('_')}`,
          fields: uniqueFields.map((f: string) => ({ field: f })),
          unique: true,
        })
      }
    }

    // Handle fields from the new IceTypeSchema.fields (Map<string, FieldDefinition>)
    for (const [fieldName, field] of iceSchema.fields) {
      // In the new API, field is FieldDefinition from @icetype/core
      const iceField = field as import('@icetype/core').FieldDefinition & {
        description?: string
        validation?: {
          min?: number
          max?: number
          minLength?: number
          maxLength?: number
          pattern?: string
        }
      }

      if (iceField.relation) {
        // Relationship field
        const rel = iceField.relation as import('@icetype/core').RelationDefinition & { threshold?: number }
        const inverse = rel.inverse || fieldName
        const arraySuffix = iceField.isArray ? '[]' : ''
        const relString = `${rel.operator} ${rel.targetType}.${inverse}${arraySuffix}`

        // Handle threshold for fuzzy relationships
        if (rel.threshold !== undefined) {
          typeDef[fieldName] = {
            type: relString,
            threshold: rel.threshold,
          } as import('./schema').FieldDefinition
        } else {
          typeDef[fieldName] = relString
        }
      } else {
        // Regular field
        const arraySuffix = iceField.isArray ? '[]' : ''
        // In new API: modifier '!' means required, '#' means indexed, '?' means optional
        const required = iceField.modifier === '!' ? '!' : ''
        let typeStr = `${iceField.type}${arraySuffix}${required}`

        if (iceField.defaultValue !== undefined) {
          typeStr += ` = ${JSON.stringify(iceField.defaultValue)}`
        }

        // Check if we need object format for description/validation
        if (iceField.description || iceField.validation) {
          const fieldDef: import('./schema').FieldDefinition = {
            type: typeStr as import('./schema').FieldTypeString,
          }
          if (iceField.description) {
            fieldDef.description = iceField.description
          }
          if (iceField.validation) {
            if (iceField.validation.min !== undefined) fieldDef.min = iceField.validation.min
            if (iceField.validation.max !== undefined) fieldDef.max = iceField.validation.max
            if (iceField.validation.minLength !== undefined) fieldDef.minLength = iceField.validation.minLength
            if (iceField.validation.maxLength !== undefined) fieldDef.maxLength = iceField.validation.maxLength
            if (iceField.validation.pattern !== undefined) fieldDef.pattern = iceField.validation.pattern
          }
          typeDef[fieldName] = fieldDef
        } else {
          typeDef[fieldName] = typeStr
        }
      }
    }

    // Handle FTS indexes - prefer legacy $fts format for extended options
    const legacyFts = extSchema.$fts
    if (legacyFts && Array.isArray(legacyFts)) {
      for (const ftsEntry of legacyFts) {
        if (typeof ftsEntry === 'string') {
          // Simple string format
          if (typeof typeDef[ftsEntry] === 'string') {
            typeDef[ftsEntry] = { type: typeDef[ftsEntry] as string, index: 'fts' }
          }
        } else if (typeof ftsEntry === 'object') {
          // Extended object format: { field: 'content', language: 'english', weight: 1.0 }
          const ftsObj = ftsEntry as { field: string; language?: string; weight?: number }
          const existing = typeDef[ftsObj.field]
          const type = typeof existing === 'string' ? existing :
                       typeof existing === 'object' && existing !== null ? (existing as { type?: string }).type || 'text' : 'text'
          const ftsFieldDef: import('./schema').FieldDefinition = {
            type,
            index: 'fts',
          }
          if (ftsObj.language || ftsObj.weight) {
            ftsFieldDef.ftsOptions = { language: ftsObj.language, weight: ftsObj.weight }
          }
          typeDef[ftsObj.field] = ftsFieldDef
        }
      }
    } else if (directives.fts && directives.fts.length > 0) {
      // Fall back to new directives format (only string field names)
      for (const ftsField of directives.fts) {
        if (typeof typeDef[ftsField] === 'string') {
          typeDef[ftsField] = { type: typeDef[ftsField] as string, index: 'fts' }
        }
      }
    }

    // Handle vector indexes from directives
    const vectorDefs = directives.vector || extSchema.$vector
    if (vectorDefs && Array.isArray(vectorDefs)) {
      for (const vec of vectorDefs) {
        // Validate dimensions
        if (vec.dimensions <= 0 || !Number.isInteger(vec.dimensions)) {
          throw new Error(`Invalid vector dimensions: ${vec.dimensions}. Must be a positive integer.`)
        }

        const vecDef: import('./schema').FieldDefinition = {
          type: `vector(${vec.dimensions})`,
          index: 'vector',
          dimensions: vec.dimensions,
        }

        // Handle metric type
        if (vec.metric && ['cosine', 'euclidean', 'dotProduct'].includes(vec.metric)) {
          vecDef.metric = vec.metric as 'cosine' | 'euclidean' | 'dotProduct'
        }

        typeDef[vec.field] = vecDef
      }
    }

    schema[name] = typeDef
  }

  return schema
}

// =============================================================================
// Schema Loader
// =============================================================================

/** Schema source type */
export type SchemaSource =
  | { type: 'parquedb'; schema: import('./schema').Schema }
  | { type: 'graphdl'; schema: import('@graphdl/core').ParsedGraph }
  | { type: 'icetype'; schema: IceTypeParsedSchema | Map<string, import('@icetype/core').IceTypeSchema> }
  | { type: 'json'; path: string }
  | { type: 'typescript'; path: string }

/**
 * Load schema from various sources
 *
 * Supports:
 * - Native ParqueDB schema objects
 * - GraphDL ParsedGraph from Graph() function
 * - IceType schemas (both legacy and new Map<string, IceTypeSchema>)
 * - JSON files (planned)
 * - TypeScript files (planned)
 */
export async function loadSchema(source: SchemaSource): Promise<import('./schema').Schema> {
  switch (source.type) {
    case 'parquedb':
      return source.schema

    case 'graphdl':
      return fromGraphDL(source.schema)

    case 'icetype':
      return fromIceType(source.schema)

    case 'json':
      // Would load from JSON file
      throw new Error('JSON schema loading not yet implemented')

    case 'typescript':
      // Would load from TS file
      throw new Error('TypeScript schema loading not yet implemented')

    default:
      throw new Error(`Unknown schema source type: ${(source as { type: string }).type}`)
  }
}

// =============================================================================
// RPC Integration (capnweb)
// =============================================================================

/**
 * Marker interface for RpcTarget
 * Actual implementation comes from capnweb
 */
export interface RpcTargetMarker {
  readonly __rpcTarget: true
}

/**
 * Marker interface for RpcPromise
 * Actual implementation comes from capnweb
 */
export interface RpcPromiseMarker<T> extends Promise<T> {
  /** Chain method calls without awaiting */
  readonly __rpcPromise: true

  /**
   * Map over results on the remote side
   * The callback runs on the server, not the client
   */
  map<U>(fn: (value: T extends (infer E)[] ? E : T) => U | Promise<U>): RpcPromiseMarker<T extends unknown[] ? U[] : U>
}

/**
 * Type helper for RPC-enabled methods
 */
export type RpcMethod<TArgs extends unknown[], TResult> = (
  ...args: TArgs
) => RpcPromiseMarker<TResult>

// =============================================================================
// Variant Shredding Configuration
// =============================================================================

/** Shredding configuration for Variant columns */
export interface ShredConfig {
  /** Fields to always shred */
  always: string[]

  /** Fields to auto-detect for shredding */
  auto: boolean

  /** Threshold for auto-shredding (cardinality) */
  autoThreshold?: number

  /** Types that should always be shredded */
  shredTypes: string[]
}

/** Default shredding configuration */
export const DEFAULT_SHRED_CONFIG: ShredConfig = {
  always: [],
  auto: true,
  autoThreshold: 1000,
  shredTypes: ['enum', 'boolean', 'date', 'datetime', 'timestamp', 'int', 'float'],
}

/**
 * Determine which fields should be shredded
 */
export function determineShredFields(
  typeDef: import('./schema').TypeDefinition,
  config: ShredConfig = DEFAULT_SHRED_CONFIG
): string[] {
  const shredFields = new Set<string>(config.always)

  // Add explicitly marked shred fields
  if (typeDef.$shred) {
    for (const field of typeDef.$shred) {
      shredFields.add(field)
    }
  }

  // Auto-detect based on field types
  if (config.auto) {
    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (fieldName.startsWith('$')) continue
      if (typeof fieldDef === 'string') {
        // Check if type should be shredded
        for (const shredType of config.shredTypes) {
          if (fieldDef.startsWith(shredType)) {
            shredFields.add(fieldName)
            break
          }
        }
        // Check for indexed fields
        if (fieldDef.includes('index')) {
          shredFields.add(fieldName)
        }
      } else if (typeof fieldDef === 'object' && fieldDef !== null) {
        const def = fieldDef as import('./schema').FieldDefinition
        if (def.index) {
          shredFields.add(fieldName)
        }
        if (def.type && config.shredTypes.some(t => def.type.startsWith(t))) {
          shredFields.add(fieldName)
        }
      }
    }
  }

  return Array.from(shredFields)
}
