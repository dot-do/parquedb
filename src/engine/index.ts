/**
 * ParqueDB MergeTree Engine
 *
 * JSONL append-only writes + Parquet compaction with merge-on-read semantics.
 */

// Core engine
export { ParqueEngine } from './engine'
export type { EngineConfig, AutoCompactOptions, UpdateOps, FindOptions } from './engine'

// Types
export type { DataLine, RelLine, EventLine, SchemaLine, Migration, Line, DataOp, RelOp, EventOp } from './types'

// Buffers
export { TableBuffer } from './buffer'
export type { ScanFilter } from './buffer'
export { RelationshipBuffer } from './rel-buffer'

// Schema
export { SchemaRegistry } from './schema'

// JSONL
export { JsonlWriter } from './jsonl-writer'
export { BunJsonlWriter } from './bun-writer'
export { replay, replayInto, replayRange, lineCount } from './jsonl-reader'
export { serializeLine, deserializeLine, isDataLine, isRelLine, isEventLine, isSchemaLine } from './jsonl'

// Merge
export { mergeResults } from './merge'

// Compaction
export { compactDataTable, shouldCompact } from './compactor'
export type { StorageAdapter, CompactOptions } from './compactor'
export { compactRelationships } from './compactor-rels'
export type { RelStorageAdapter } from './compactor-rels'
export { compactEvents } from './compactor-events'
export type { EventStorageAdapter } from './compactor-events'

// Rotation
export { rotate, cleanup, needsRecovery, getCompactingPath } from './rotation'

// Storage adapters
export { LocalStorageAdapter, MemoryStorageAdapter } from './storage-adapters'
export { hybridCompactData, hybridCompactRels, hybridCompactEvents, hybridCompactAll } from './storage-adapters'
export type { FullStorageAdapter } from './storage-adapters'

// Parquet storage adapter
export { ParquetStorageAdapter } from './parquet-adapter'

// Bun-optimized Parquet reader
export { readParquetFile, isBunRuntime } from './bun-reader'
export type { AsyncBuffer } from './bun-reader'

// ParqueDB Bridge Adapter
export { EngineDB, EngineCollection } from './parquedb-adapter'
export type { EngineDBConfig } from './parquedb-adapter'

// Worker thread compaction
export { CompactionWorker, encodeDataToParquet, encodeRelsToParquet, encodeEventsToParquet } from './compaction-worker'
export type { CompactionRequest, CompactionResult } from './compaction-worker'

// DO read path (merge-on-read)
export { DOReadPath } from './do-read-path'
