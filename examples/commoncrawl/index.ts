/**
 * Common Crawl Host Graph Example
 *
 * Load and analyze the Common Crawl host-level web graph using ParqueDB.
 *
 * @example
 * ```typescript
 * import {
 *   HostGraphLoader,
 *   HostGraphClient,
 *   MetricsPipeline,
 *   createCrawlId,
 *   createTLD,
 *   KNOWN_CRAWLS,
 * } from './examples/commoncrawl-hostgraph'
 *
 * const crawlId = createCrawlId('cc-main-2025-oct-nov-dec')
 * const loader = new HostGraphLoader(bucket, crawlId)
 * await loader.load()
 * ```
 */

// Schema exports
export {
  // Types
  type Host,
  type Link,
  type HostMetrics,
  type TLDStats,
  type CrawlMetadata,
  type HostId,
  type TLD,
  type CrawlId,
  type ReversedHostname,
  type Timestamp,
  type HostGraphConfig,

  // Parquet schemas
  HOST_PARQUET_SCHEMA,
  LINK_FORWARD_PARQUET_SCHEMA,
  LINK_REVERSE_PARQUET_SCHEMA,
  METRICS_PARQUET_SCHEMA,
  TLD_STATS_PARQUET_SCHEMA,
  CRAWL_METADATA_PARQUET_SCHEMA,

  // Storage paths
  STORAGE_PATHS,

  // Configuration
  DEFAULT_CONFIG,

  // Type constructors
  createHostId,
  createCrawlId,
  createTLD,
  createTimestamp,
  createReversedHostname,

  // Utility functions
  parseReversedHostname,
  reverseHostname,
  extractTLD,
  computeEdgeHash,
  computeHostnameHash,
} from './schema'

// Loader exports
export {
  // Classes
  HostGraphLoader,
  IncrementalHostGraphLoader,

  // Types
  type StorageBucket,
  type LoadProgress,
  type ProgressCallback,

  // URLs and constants
  getCrawlUrls,
  KNOWN_CRAWLS,

  // Entry point
  main as runLoader,
} from './load'

// Metrics exports
export {
  // Classes
  PageRankComputer,
  HITSComputer,
  MetricsPipeline,

  // Functions
  computeDegrees,
  approximatePageRankMonteCarlo,
  computeClusteringCoefficient,
  approximateGlobalClusteringCoefficient,
  aggregateHostMetrics,
  computeTLDStats,

  // Types
  type MetricsProgress,
  type MetricsProgressCallback,
} from './metrics'

// Query exports
export {
  // Classes
  HostGraphClient,

  // Types
  type QueryResult,
  type SortSpec,
  type FilterOp,
  type HostFilter,
  type LinkFilter,
  type MetricsFilter,

  // Example function
  exampleQueries,
} from './queries'
