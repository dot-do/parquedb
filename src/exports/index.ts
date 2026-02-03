/**
 * ParqueDB Tiered Exports
 *
 * This module provides tiered exports for different bundle size requirements:
 *
 * - **Full** (main): Complete ParqueDB with all features
 * - **Small** (~50KB): Core CRUD, MemoryBackend, basic filters/updates
 * - **Tiny** (~15KB): Read-only parquet queries with filtering
 *
 * @example
 * ```typescript
 * // Full export (all features)
 * import { ParqueDB, IcebergBackend, IndexManager } from 'parquedb'
 *
 * // Small export (core CRUD only)
 * import { ParqueDB, Collection, MemoryBackend } from 'parquedb/small'
 *
 * // Tiny export (read-only queries)
 * import { parquetQuery, parquetStream } from 'parquedb/tiny'
 * ```
 *
 * @packageDocumentation
 * @module parquedb/exports
 */

export * from './small'
