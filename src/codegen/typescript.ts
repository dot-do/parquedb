/**
 * TypeScript Code Generator
 *
 * Generates TypeScript type definitions from schema snapshots.
 * Enables compile-time type safety for database operations.
 */

import type { SchemaSnapshot, CollectionSchemaSnapshot, SchemaFieldSnapshot } from '../sync/schema-snapshot'

/**
 * Options for TypeScript generation
 */
export interface TypeScriptGenerationOptions {
  /** Wrap types in a namespace */
  namespace?: string | undefined
  /** Export db instance as default */
  exportDefault?: boolean | undefined
  /** Include schema metadata in output */
  includeMetadata?: boolean | undefined
  /** Include import statements */
  includeImports?: boolean | undefined
  /** Import path for ParqueDB (default: 'parquedb') */
  importPath?: string | undefined
}

/**
 * Generate TypeScript types from schema snapshot
 *
 * @param schema Schema snapshot to generate types from
 * @param opts Generation options
 * @returns TypeScript code as string
 */
export function generateTypeScript(
  schema: SchemaSnapshot,
  opts: TypeScriptGenerationOptions = {}
): string {
  const {
    namespace,
    exportDefault: _exportDefault = false,
    includeMetadata = true,
    includeImports = true,
    importPath = 'parquedb'
  } = opts

  const sections: string[] = []

  // Header comment
  sections.push(generateHeader(schema))

  // Imports
  if (includeImports) {
    sections.push(generateImports(importPath))
  }

  // Metadata
  if (includeMetadata) {
    sections.push(generateMetadata(schema))
  }

  // Entity interfaces
  const collectionNames = Object.keys(schema.collections)
  if (collectionNames.length > 0) {
    sections.push('// =============================================================================')
    sections.push('// Entity Types')
    sections.push('// =============================================================================')
    sections.push('')

    for (const name of collectionNames) {
      const collection = schema.collections[name]!
      sections.push(generateCollectionType(collection))
      sections.push('')
    }
  }

  // Collection interfaces
  if (collectionNames.length > 0) {
    sections.push('// =============================================================================')
    sections.push('// Collection Types')
    sections.push('// =============================================================================')
    sections.push('')

    for (const name of collectionNames) {
      sections.push(generateCollectionInterface(name))
      sections.push('')
    }
  }

  // Database interface
  if (collectionNames.length > 0) {
    sections.push('// =============================================================================')
    sections.push('// Database Interface')
    sections.push('// =============================================================================')
    sections.push('')
    sections.push(generateDatabaseInterface(collectionNames))
  }

  let code = sections.join('\n')

  // Wrap in namespace if requested
  if (namespace) {
    code = wrapInNamespace(namespace, code)
  }

  return code
}

/**
 * Generate file header comment
 */
function generateHeader(schema: SchemaSnapshot): string {
  const date = new Date(schema.capturedAt).toISOString()
  const commitInfo = schema.commitHash ? `\n * Commit: ${schema.commitHash}` : ''

  return `/**
 * Generated ParqueDB Type Definitions
 *
 * Auto-generated from schema snapshot
 * Captured: ${date}${commitInfo}
 * Schema hash: ${schema.hash}
 *
 * DO NOT EDIT MANUALLY
 * Regenerate with: parquedb types generate
 */`
}

/**
 * Generate import statements
 */
function generateImports(importPath: string): string {
  return `
import type {
  Entity,
  EntityRef,
  Filter,
  UpdateOperators,
  FindOptions,
  GetOptions,
  PaginatedResult
} from '${importPath}'`
}

/**
 * Generate metadata constant
 */
function generateMetadata(schema: SchemaSnapshot): string {
  return `

// =============================================================================
// Schema Metadata
// =============================================================================

export const SCHEMA_METADATA = {
  hash: '${schema.hash}',
  commit: ${schema.commitHash ? `'${schema.commitHash}'` : 'undefined'},
  capturedAt: ${schema.capturedAt},
  timestamp: new Date(${schema.capturedAt}).toISOString()
} as const`
}

/**
 * Generate TypeScript interface for a collection's entity type
 */
function generateCollectionType(collection: CollectionSchemaSnapshot): string {
  const fields = collection.fields.map(field => {
    const optional = !field.required ? '?' : ''
    const tsType = mapFieldToTypeScript(field)
    const comment = generateFieldComment(field)

    return `${comment}  ${field.name}${optional}: ${tsType}`
  })

  const entityInterface = `/**
 * ${collection.name} entity type
 */
export interface ${collection.name}Entity extends Entity {
  $type: '${collection.name}'
${fields.join('\n')}
}`

  // Generate input type (all fields optional for partial updates)
  const inputFields = collection.fields.map(field => {
    const tsType = mapFieldToTypeScript(field)
    const comment = generateFieldComment(field)

    return `${comment}  ${field.name}?: ${tsType}`
  })

  const inputInterface = `/**
 * ${collection.name} input type (for create/update)
 */
export interface ${collection.name}Input {
${inputFields.join('\n')}
}`

  return `${entityInterface}\n\n${inputInterface}`
}

/**
 * Generate collection interface with typed methods
 */
function generateCollectionInterface(collectionName: string): string {
  return `/**
 * Typed ${collectionName} collection interface
 */
export interface ${collectionName}Collection {
  /**
   * Create a new ${collectionName} entity
   */
  create(input: ${collectionName}Input): Promise<${collectionName}Entity>

  /**
   * Get ${collectionName} entity by ID
   */
  get(id: string, options?: GetOptions): Promise<${collectionName}Entity | null>

  /**
   * Find ${collectionName} entities matching filter
   */
  find(filter?: Filter<${collectionName}Entity>, options?: FindOptions): Promise<${collectionName}Entity[]>

  /**
   * Find first ${collectionName} entity matching filter
   */
  findOne(filter?: Filter<${collectionName}Entity>, options?: FindOptions): Promise<${collectionName}Entity | null>

  /**
   * Find ${collectionName} entities with pagination
   */
  findPaginated(filter?: Filter<${collectionName}Entity>, options?: FindOptions): Promise<PaginatedResult<${collectionName}Entity>>

  /**
   * Update ${collectionName} entity by ID
   */
  update(id: string, update: UpdateOperators<${collectionName}Entity> | Partial<${collectionName}Input>): Promise<${collectionName}Entity>

  /**
   * Update multiple ${collectionName} entities
   */
  updateMany(filter: Filter<${collectionName}Entity>, update: UpdateOperators<${collectionName}Entity>): Promise<number>

  /**
   * Delete ${collectionName} entity by ID
   */
  delete(id: string): Promise<boolean>

  /**
   * Delete multiple ${collectionName} entities
   */
  deleteMany(filter: Filter<${collectionName}Entity>): Promise<number>

  /**
   * Count ${collectionName} entities matching filter
   */
  count(filter?: Filter<${collectionName}Entity>): Promise<number>

  /**
   * Check if ${collectionName} entity exists
   */
  exists(filter: Filter<${collectionName}Entity>): Promise<boolean>
}`
}

/**
 * Generate database interface with all collections
 */
function generateDatabaseInterface(collectionNames: string[]): string {
  const collections = collectionNames.map(name => `  ${name}: ${name}Collection`).join('\n')

  return `/**
 * Typed database interface with all collections
 */
export interface Database {
${collections}
}`
}

/**
 * Map schema field to TypeScript type
 */
function mapFieldToTypeScript(field: SchemaFieldSnapshot): string {
  let baseType: string

  // Handle relationships
  if (field.relationship) {
    baseType = `EntityRef<${field.relationship.target}Entity>`
  } else {
    // Extract base type from field.type
    baseType = mapType(field.type)
  }

  // Handle arrays
  if (field.array) {
    return `${baseType}[]`
  }

  return baseType
}

/**
 * Map ParqueDB type string to TypeScript type
 *
 * Examples:
 * - 'string', 'string!' → 'string'
 * - 'int', 'int?' → 'number'
 * - 'boolean' → 'boolean'
 * - 'date' → 'Date'
 * - '-> User' → handled by relationship logic
 */
export function mapType(parquedbType: string): string {
  // Remove modifiers
  const cleaned = parquedbType
    .replace(/[!?#@]/g, '')
    .replace(/\[\]/g, '')
    .replace(/\s*->\s*.+/, '')
    .replace(/\s*<-\s*.+/, '')
    .trim()
    .toLowerCase()

  // Map to TypeScript types
  const typeMap: Record<string, string> = {
    string: 'string',
    text: 'string',
    int: 'number',
    integer: 'number',
    float: 'number',
    double: 'number',
    number: 'number',
    decimal: 'number',
    boolean: 'boolean',
    bool: 'boolean',
    date: 'Date',
    datetime: 'Date',
    timestamp: 'Date',
    time: 'Date',
    json: 'unknown',
    jsonb: 'unknown',
    variant: 'unknown',
    any: 'unknown',
    object: 'Record<string, unknown>',
    array: 'unknown[]'
  }

  return typeMap[cleaned] ?? 'unknown'
}

/**
 * Generate JSDoc comment for a field
 */
function generateFieldComment(field: SchemaFieldSnapshot): string {
  const parts: string[] = []

  if (field.relationship) {
    parts.push(`Relationship to ${field.relationship.target}`)
  }

  if (field.unique) {
    parts.push('unique')
  }

  if (field.indexed) {
    parts.push('indexed')
  }

  if (parts.length === 0) {
    return ''
  }

  return `  /** ${parts.join(', ')} */\n`
}

/**
 * Wrap code in a namespace
 */
function wrapInNamespace(namespace: string, code: string): string {
  // Indent all lines
  const indented = code.split('\n').map(line => line ? `  ${line}` : '').join('\n')

  return `export namespace ${namespace} {
${indented}
}`
}

/**
 * Generate a standalone .d.ts file
 *
 * @param schema Schema snapshot
 * @param opts Generation options
 * @returns TypeScript declaration file content
 */
export function generateDeclarationFile(
  schema: SchemaSnapshot,
  opts: TypeScriptGenerationOptions = {}
): string {
  return generateTypeScript(schema, {
    ...opts,
    includeImports: true
  })
}
