/**
 * Snippet-Optimized ParqueDB
 *
 * Query massive datasets for FREE on Cloudflare Snippets.
 *
 * Key constraints satisfied:
 * - Script: <32KB (hyparquet ~29KB)
 * - CPU: <5ms (~1-2ms for point lookups)
 * - Memory: <32MB (~2MB per 5K rows)
 * - Fetches: ≤5 (1 meta + 1-2 row groups)
 *
 * @example
 * ```typescript
 * import { createQueryClient } from './query'
 *
 * const db = createQueryClient('https://cdn.example.com/data/by-id')
 *
 * // Point lookup: ~1ms CPU
 * const user = await db.get('user-123')
 *
 * // Range query: ~2-3ms CPU
 * const recent = await db.find('createdAt', '2024-01-01', '2024-12-31', { limit: 10 })
 * ```
 */

export {
  partitionById,
  partitionByType,
  partitionByDate,
  estimatePartitionCount,
  type PartitionConfig,
  type PartitionResult,
  type PartitionManifest,
} from './partition'

export {
  createQueryClient,
  createAsyncFile,
  getById,
  findByRange,
  type AsyncFile,
  type QueryResult,
} from './query'

/**
 * Performance characteristics (measured locally, Node.js):
 *
 * | Row Group Size | Read Time | Memory |
 * |----------------|-----------|--------|
 * | 1K rows        | ~0.5ms    | ~0.4MB |
 * | 5K rows        | ~2ms      | ~2MB   |
 * | 10K rows       | ~4ms      | ~4MB   |
 *
 * Cloudflare Workers add ~5-10ms network latency per range request,
 * but this doesn't count against CPU time.
 *
 * Optimal configuration:
 * - Row group size: 5-10K rows
 * - File size: <25MB (free static hosting)
 * - Sort data by query column for statistics-based skipping
 */
