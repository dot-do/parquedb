/**
 * Integration types for GraphDL and IceType
 * These provide optional schema definition via external packages
 */

// =============================================================================
// GraphDL Integration
// =============================================================================

/**
 * GraphDL relationship operators
 * From: https://github.com/primitives-org/graphdl
 */
export type GraphDLOperator = '->' | '~>' | '<-' | '<~'

/** GraphDL relationship direction */
export type GraphDLDirection = 'forward' | 'backward'

/** GraphDL match mode */
export type GraphDLMatchMode = 'exact' | 'fuzzy'

/** Parsed GraphDL relationship */
export interface GraphDLRelation {
  operator: GraphDLOperator
  direction: GraphDLDirection
  matchMode: GraphDLMatchMode
  targetType: string
  backref?: string
  threshold?: number  // For fuzzy matches
  isArray: boolean
}

/** GraphDL parsed entity */
export interface GraphDLEntity {
  name: string
  $type?: string
  fields: Map<string, GraphDLField>
}

/** GraphDL parsed field */
export interface GraphDLField {
  name: string
  type: string
  isArray: boolean
  isOptional: boolean
  isRelation: boolean
  relation?: GraphDLRelation
}

/** GraphDL schema (from Graph() function) */
export interface GraphDLSchema {
  entities: Map<string, GraphDLEntity>
  getEntity(name: string): GraphDLEntity | undefined
  getRelationships(): GraphDLRelation[]
}

/**
 * Convert GraphDL schema to ParqueDB schema
 */
export function fromGraphDL(graphdl: GraphDLSchema): import('./schema').Schema {
  const schema: import('./schema').Schema = {}

  for (const [name, entity] of graphdl.entities) {
    const typeDef: import('./schema').TypeDefinition = {}

    if (entity.$type) {
      typeDef.$type = entity.$type
    }

    for (const [fieldName, field] of entity.fields) {
      if (field.isRelation && field.relation) {
        // Convert to ParqueDB relationship string
        const rel = field.relation
        const operator = rel.operator
        const target = rel.targetType
        const backref = rel.backref || fieldName
        const array = field.isArray ? '[]' : ''

        typeDef[fieldName] = `${operator} ${target}.${backref}${array}`
      } else {
        // Regular field
        const required = !field.isOptional ? '!' : ''
        const array = field.isArray ? '[]' : ''
        typeDef[fieldName] = `${field.type}${array}${required}`
      }
    }

    schema[name] = typeDef
  }

  return schema
}

// =============================================================================
// IceType Integration
// =============================================================================

/**
 * IceType field definition
 * From: https://github.com/primitives-org/icetype
 */
export interface IceTypeField {
  name: string
  type: string
  required: boolean
  default?: unknown
  relation?: {
    operator: GraphDLOperator
    targetType: string
    inverse?: string
  }
}

/** IceType schema definition */
export interface IceTypeSchema {
  $type?: string
  $partitionBy?: string[]
  $index?: string[][]
  $fts?: string[]
  $vector?: { field: string; dimensions: number }[]
  fields: Map<string, IceTypeField>
}

/** IceType parsed schema collection */
export interface IceTypeParsedSchema {
  schemas: Map<string, IceTypeSchema>
  getSchema(name: string): IceTypeSchema | undefined
}

/**
 * Convert IceType schema to ParqueDB schema
 */
export function fromIceType(icetype: IceTypeParsedSchema): import('./schema').Schema {
  const schema: import('./schema').Schema = {}

  for (const [name, iceSchema] of icetype.schemas) {
    const typeDef: import('./schema').TypeDefinition = {}

    if (iceSchema.$type) {
      typeDef.$type = iceSchema.$type
    }

    // Handle partitioning hint (stored in metadata)
    if (iceSchema.$partitionBy) {
      typeDef.$shred = iceSchema.$partitionBy
    }

    // Handle indexes
    if (iceSchema.$index) {
      typeDef.$indexes = iceSchema.$index.map((fields, i) => ({
        name: `idx_${name}_${i}`,
        fields: fields.map(f => ({ field: f })),
      }))
    }

    // Handle fields
    for (const [fieldName, field] of iceSchema.fields) {
      if (field.relation) {
        const rel = field.relation
        const inverse = rel.inverse || fieldName
        typeDef[fieldName] = `${rel.operator} ${rel.targetType}.${inverse}`
      } else {
        const required = field.required ? '!' : ''
        let def = `${field.type}${required}`
        if (field.default !== undefined) {
          def += ` = ${JSON.stringify(field.default)}`
        }
        typeDef[fieldName] = def
      }
    }

    // Handle FTS indexes
    if (iceSchema.$fts) {
      for (const field of iceSchema.$fts) {
        if (typeof typeDef[field] === 'string') {
          typeDef[field] = { type: typeDef[field] as string, index: 'fts' }
        }
      }
    }

    // Handle vector indexes
    if (iceSchema.$vector) {
      for (const vec of iceSchema.$vector) {
        typeDef[vec.field] = {
          type: `vector(${vec.dimensions})`,
          index: 'vector',
          dimensions: vec.dimensions,
        }
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
  | { type: 'graphdl'; schema: GraphDLSchema }
  | { type: 'icetype'; schema: IceTypeParsedSchema }
  | { type: 'json'; path: string }
  | { type: 'typescript'; path: string }

/**
 * Load schema from various sources
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
      throw new Error(`Unknown schema source type: ${(source as any).type}`)
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
