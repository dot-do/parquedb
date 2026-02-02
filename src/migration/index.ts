/**
 * Migration utilities for ParqueDB
 *
 * Import data from various sources:
 * - JSON/JSONL files
 * - CSV files
 * - MongoDB Extended JSON / BSON
 *
 * Features:
 * - Streaming support for large files
 * - Async iterators for memory-efficient processing
 * - Progress callbacks during import
 *
 * @packageDocumentation
 */

// Types
export type {
  MigrationOptions,
  JsonImportOptions,
  CsvImportOptions,
  BsonImportOptions,
  MigrationResult,
  MigrationError,
  StreamingDocument,
  StreamingOptions,
  CsvStreamingOptions,
} from './types'

// JSON import
export {
  importFromJson,
  importFromJsonl,
  streamFromJsonl,
  streamFromJson,
} from './json'

// CSV import
export {
  importFromCsv,
  streamFromCsv,
} from './csv'

// MongoDB import
export {
  importFromMongodb,
  importFromBson,
  streamFromMongodbJsonl,
} from './mongodb'

export type { MongoStreamingOptions } from './mongodb'

// Re-export utility functions that may be useful
export {
  inferType,
  parseCsvLine,
  convertBsonValue,
} from './utils'
