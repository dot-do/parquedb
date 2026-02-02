/**
 * Migration utility types for ParqueDB
 *
 * Types for importing data from various sources (JSON, CSV, MongoDB BSON)
 */

/**
 * Options for migration operations
 */
export interface MigrationOptions {
  /**
   * Number of documents to process in each batch
   * @default 1000
   */
  batchSize?: number

  /**
   * Progress callback called after each batch
   * @param count - Total number of documents processed so far
   */
  onProgress?: (count: number) => void

  /**
   * Transform function applied to each document before import
   * Use this to modify document structure, add fields, or filter documents
   * Return null/undefined to skip a document
   */
  transform?: (doc: unknown) => unknown

  /**
   * Entity type ($type) to assign to imported documents
   * If not provided, will try to infer from document or use namespace
   */
  entityType?: string

  /**
   * Skip validation during import for better performance
   * @default false
   */
  skipValidation?: boolean

  /**
   * Actor to attribute the imports to
   * @default 'system/migration'
   */
  actor?: string

  /**
   * Enable streaming mode for large files
   * When enabled, files are processed line by line instead of loading entirely into memory
   * @default false
   */
  streaming?: boolean
}

/**
 * Options for JSON import
 */
export interface JsonImportOptions extends MigrationOptions {
  /**
   * For JSON arrays at root level, this is ignored.
   * For JSON objects with nested arrays, specify the path to the array
   * e.g., "data.items" to import from { data: { items: [...] } }
   */
  arrayPath?: string
}

/**
 * Options for CSV import
 */
export interface CsvImportOptions extends MigrationOptions {
  /**
   * CSV delimiter character
   * @default ','
   */
  delimiter?: string

  /**
   * Whether the first row contains column headers
   * @default true
   */
  headers?: boolean | string[]

  /**
   * Skip empty lines
   * @default true
   */
  skipEmptyLines?: boolean

  /**
   * Type inference for columns
   * If true, attempts to parse numbers, booleans, and dates
   * @default true
   */
  inferTypes?: boolean

  /**
   * Custom column type mappings
   * e.g., { age: 'number', active: 'boolean', createdAt: 'date' }
   */
  columnTypes?: Record<string, 'string' | 'number' | 'boolean' | 'date' | 'json'>

  /**
   * Column to use as the entity name
   * If not provided, will generate a name or use first column
   */
  nameColumn?: string
}

/**
 * Options for MongoDB BSON import
 */
export interface BsonImportOptions extends MigrationOptions {
  /**
   * Field to use as the entity $id (will be prefixed with namespace)
   * If not provided, a new ID will be generated
   */
  idField?: string

  /**
   * Field to use as the entity name
   * If not provided, will try common fields like 'name', 'title', '_id'
   */
  nameField?: string

  /**
   * Whether to preserve MongoDB's _id field in the document data
   * @default false
   */
  preserveMongoId?: boolean

  /**
   * Whether to convert MongoDB ObjectIds to strings
   * @default true
   */
  convertObjectIds?: boolean

  /**
   * Whether to convert MongoDB dates to JS Date objects
   * @default true
   */
  convertDates?: boolean
}

/**
 * Result of an import operation
 */
export interface MigrationResult {
  /** Total number of documents successfully imported */
  imported: number

  /** Number of documents skipped (e.g., filtered by transform) */
  skipped: number

  /** Number of documents that failed to import */
  failed: number

  /** Errors encountered during import */
  errors: MigrationError[]

  /** Duration of the import in milliseconds */
  duration: number
}

/**
 * Error encountered during migration
 */
export interface MigrationError {
  /** Index/line number of the document that failed */
  index: number

  /** Error message */
  message: string

  /** The document that failed (if available) */
  document?: unknown
}

/**
 * JSONL (JSON Lines) reader interface
 */
export interface JsonLinesReader {
  [Symbol.asyncIterator](): AsyncIterator<unknown>
}

/**
 * Streaming result for async iteration
 * Used by streamFromJsonl and streamFromCsv
 */
export interface StreamingDocument<T = Record<string, unknown>> {
  /** The parsed document (null when error is present) */
  document: T | null
  /** Line number in source file (1-indexed) */
  lineNumber: number
  /** Any parse error (document will be null) */
  error?: string
}

/**
 * Streaming options for async iterators
 */
export interface StreamingOptions {
  /**
   * Transform function applied to each document
   * Return null/undefined to skip a document
   */
  transform?: (doc: unknown) => unknown

  /**
   * Skip documents that fail to parse (instead of yielding error)
   * @default false
   */
  skipErrors?: boolean
}

/**
 * CSV streaming options
 */
export interface CsvStreamingOptions extends StreamingOptions {
  /**
   * CSV delimiter character
   * @default ','
   */
  delimiter?: string

  /**
   * Whether the first row contains column headers
   * @default true
   */
  headers?: boolean | string[]

  /**
   * Skip empty lines
   * @default true
   */
  skipEmptyLines?: boolean

  /**
   * Type inference for columns
   * If true, attempts to parse numbers, booleans, and dates
   * @default true
   */
  inferTypes?: boolean

  /**
   * Custom column type mappings
   * e.g., { age: 'number', active: 'boolean', createdAt: 'date' }
   */
  columnTypes?: Record<string, 'string' | 'number' | 'boolean' | 'date' | 'json'>
}
