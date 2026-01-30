/**
 * Common Crawl Host Graph Schema Definitions
 *
 * Defines the Parquet schemas for storing and querying the Common Crawl
 * host-level web graph in ParqueDB.
 *
 * Data source: https://commoncrawl.org/web-graphs
 */

// ============================================================================
// Branded Types
// ============================================================================

declare const brand: unique symbol
type Brand<B> = { [brand]: B }

/** Host identifier (numeric index from Common Crawl) */
export type HostId = number & Brand<'HostId'>

/** Reversed hostname (e.g., "com.example.www" -> "www.example.com") */
export type ReversedHostname = string & Brand<'ReversedHostname'>

/** TLD (Top-Level Domain) */
export type TLD = string & Brand<'TLD'>

/** Crawl identifier (e.g., "cc-main-2025-oct-nov-dec") */
export type CrawlId = string & Brand<'CrawlId'>

/** Timestamp in microseconds since epoch */
export type Timestamp = bigint & Brand<'Timestamp'>

// Constructors
export const createHostId = (id: number): HostId => id as HostId
export const createReversedHostname = (name: string): ReversedHostname => name as ReversedHostname
export const createTLD = (tld: string): TLD => tld as TLD
export const createCrawlId = (id: string): CrawlId => id as CrawlId
export const createTimestamp = (ts: bigint): Timestamp => ts as Timestamp

// ============================================================================
// Host Entity Schema
// ============================================================================

/**
 * Host (domain/subdomain) entity
 *
 * Represents a single host in the web graph.
 * Hosts are identified by their reversed hostname for lexicographic sorting.
 *
 * Partitioned by: TLD (first segment of reversed hostname)
 * Sort order: (tld, reversed_hostname, crawl_id DESC)
 */
export interface Host {
  /** Numeric host ID from Common Crawl (unique per crawl) */
  host_id: HostId

  /** Reversed hostname (e.g., "com.example" for "example.com") */
  reversed_hostname: ReversedHostname

  /** Extracted TLD for partitioning (e.g., "com", "org", "uk") */
  tld: TLD

  /** Second-level domain (e.g., "example" from "example.com") */
  sld: string

  /** Full subdomain path (e.g., "www.blog" from "www.blog.example.com") */
  subdomain: string | null

  /** Hash of hostname for consistent partitioning (xxHash64) */
  hostname_hash: bigint

  /** Crawl this host was observed in */
  crawl_id: CrawlId

  /** When this record was ingested */
  ingested_at: Timestamp

  /** Whether host is still active in latest crawl */
  is_active: boolean

  /** Number of crawls this host has appeared in */
  crawl_count: number

  /** First crawl this host was seen in */
  first_seen_crawl: CrawlId | null

  /** Last crawl this host was seen in */
  last_seen_crawl: CrawlId | null
}

/**
 * Parquet schema for hosts.parquet
 */
export const HOST_PARQUET_SCHEMA = {
  host_id: { type: 'INT64', encoding: 'DELTA' },
  reversed_hostname: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  tld: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  sld: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  subdomain: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  hostname_hash: { type: 'INT64', encoding: 'PLAIN' },
  crawl_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  ingested_at: { type: 'INT64', encoding: 'PLAIN' },
  is_active: { type: 'BOOLEAN', encoding: 'PLAIN' },
  crawl_count: { type: 'INT32', encoding: 'PLAIN' },
  first_seen_crawl: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
  last_seen_crawl: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
} as const

// ============================================================================
// Link (Edge) Schema
// ============================================================================

/**
 * Link edge between hosts
 *
 * Represents a hyperlink relationship from one host to another.
 * Common Crawl provides (from_id, to_id) pairs; we enrich with counts.
 *
 * Multiple indexes for efficient traversal:
 * - Forward: (from_host_id, to_host_id) - outbound links
 * - Reverse: (to_host_id, from_host_id) - inbound links
 * - By TLD: (from_tld, to_tld, from_host_id, to_host_id)
 */
export interface Link {
  /** Source host ID */
  from_host_id: HostId

  /** Target host ID */
  to_host_id: HostId

  /** Source TLD (for partitioning) */
  from_tld: TLD

  /** Target TLD (for cross-TLD analysis) */
  to_tld: TLD

  /** Hash for consistent partitioning */
  edge_hash: bigint

  /** Crawl this link was observed in */
  crawl_id: CrawlId

  /** Number of individual links from source to target (page-level aggregated) */
  link_count: number

  /** When this edge was ingested */
  ingested_at: Timestamp

  /** Whether edge is bidirectional (both hosts link to each other) */
  is_bidirectional: boolean

  /** First crawl this link was seen */
  first_seen_crawl: CrawlId | null

  /** Consecutive crawls this link has been present */
  stability_score: number
}

/**
 * Parquet schema for links_forward.parquet (outbound traversal)
 * Sort order: (from_tld, from_host_id, to_host_id, crawl_id DESC)
 */
export const LINK_FORWARD_PARQUET_SCHEMA = {
  from_host_id: { type: 'INT64', encoding: 'DELTA' },
  to_host_id: { type: 'INT64', encoding: 'DELTA' },
  from_tld: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  to_tld: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  edge_hash: { type: 'INT64', encoding: 'PLAIN' },
  crawl_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  link_count: { type: 'INT32', encoding: 'PLAIN' },
  ingested_at: { type: 'INT64', encoding: 'PLAIN' },
  is_bidirectional: { type: 'BOOLEAN', encoding: 'PLAIN' },
  first_seen_crawl: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },
  stability_score: { type: 'INT32', encoding: 'PLAIN' },
} as const

/**
 * Parquet schema for links_reverse.parquet (inbound traversal)
 * Sort order: (to_tld, to_host_id, from_host_id, crawl_id DESC)
 */
export const LINK_REVERSE_PARQUET_SCHEMA = LINK_FORWARD_PARQUET_SCHEMA

// ============================================================================
// Graph Metrics Schema
// ============================================================================

/**
 * Computed graph metrics for each host
 *
 * Pre-computed centrality and degree metrics for efficient querying.
 * Updated incrementally as new crawls are processed.
 */
export interface HostMetrics {
  /** Host ID */
  host_id: HostId

  /** TLD for partitioning */
  tld: TLD

  /** Crawl these metrics were computed for */
  crawl_id: CrawlId

  /** Number of outbound links (out-degree) */
  out_degree: bigint

  /** Number of inbound links (in-degree) */
  in_degree: bigint

  /** Unique hosts this host links to */
  unique_out_hosts: bigint

  /** Unique hosts linking to this host */
  unique_in_hosts: bigint

  /** PageRank score (approximation) */
  pagerank: number

  /** Harmonic centrality score */
  harmonic_centrality: number

  /** Hub score (HITS algorithm) */
  hub_score: number

  /** Authority score (HITS algorithm) */
  authority_score: number

  /** Clustering coefficient */
  clustering_coefficient: number

  /** Is this a hub (out_degree >> in_degree)? */
  is_hub: boolean

  /** Is this an authority (in_degree >> out_degree)? */
  is_authority: boolean

  /** When metrics were computed */
  computed_at: Timestamp
}

/**
 * Parquet schema for metrics.parquet
 * Sort order: (tld, host_id, crawl_id DESC)
 */
export const METRICS_PARQUET_SCHEMA = {
  host_id: { type: 'INT64', encoding: 'DELTA' },
  tld: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  crawl_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  out_degree: { type: 'INT64', encoding: 'PLAIN' },
  in_degree: { type: 'INT64', encoding: 'PLAIN' },
  unique_out_hosts: { type: 'INT64', encoding: 'PLAIN' },
  unique_in_hosts: { type: 'INT64', encoding: 'PLAIN' },
  pagerank: { type: 'DOUBLE', encoding: 'PLAIN' },
  harmonic_centrality: { type: 'DOUBLE', encoding: 'PLAIN' },
  hub_score: { type: 'DOUBLE', encoding: 'PLAIN' },
  authority_score: { type: 'DOUBLE', encoding: 'PLAIN' },
  clustering_coefficient: { type: 'DOUBLE', encoding: 'PLAIN' },
  is_hub: { type: 'BOOLEAN', encoding: 'PLAIN' },
  is_authority: { type: 'BOOLEAN', encoding: 'PLAIN' },
  computed_at: { type: 'INT64', encoding: 'PLAIN' },
} as const

// ============================================================================
// TLD Aggregation Schema
// ============================================================================

/**
 * Aggregated statistics per TLD
 *
 * Pre-computed aggregations for TLD-level analysis.
 */
export interface TLDStats {
  /** Top-level domain */
  tld: TLD

  /** Crawl ID */
  crawl_id: CrawlId

  /** Total hosts in this TLD */
  host_count: bigint

  /** Total outbound links from this TLD */
  total_out_links: bigint

  /** Total inbound links to this TLD */
  total_in_links: bigint

  /** Average PageRank of hosts in this TLD */
  avg_pagerank: number

  /** Median in-degree */
  median_in_degree: number

  /** 99th percentile in-degree */
  p99_in_degree: bigint

  /** Number of links staying within this TLD */
  internal_links: bigint

  /** Number of links going to other TLDs */
  external_links: bigint

  /** Top target TLDs (JSON array of {tld, count}) */
  top_target_tlds: string

  /** When stats were computed */
  computed_at: Timestamp
}

/**
 * Parquet schema for tld_stats.parquet
 */
export const TLD_STATS_PARQUET_SCHEMA = {
  tld: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  crawl_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  host_count: { type: 'INT64', encoding: 'PLAIN' },
  total_out_links: { type: 'INT64', encoding: 'PLAIN' },
  total_in_links: { type: 'INT64', encoding: 'PLAIN' },
  avg_pagerank: { type: 'DOUBLE', encoding: 'PLAIN' },
  median_in_degree: { type: 'DOUBLE', encoding: 'PLAIN' },
  p99_in_degree: { type: 'INT64', encoding: 'PLAIN' },
  internal_links: { type: 'INT64', encoding: 'PLAIN' },
  external_links: { type: 'INT64', encoding: 'PLAIN' },
  top_target_tlds: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  computed_at: { type: 'INT64', encoding: 'PLAIN' },
} as const

// ============================================================================
// Crawl Metadata Schema
// ============================================================================

/**
 * Metadata about each processed crawl
 */
export interface CrawlMetadata {
  /** Crawl identifier */
  crawl_id: CrawlId

  /** Human-readable name */
  display_name: string

  /** Source URL pattern */
  source_url: string

  /** When the crawl data was collected by Common Crawl */
  crawl_date_start: Timestamp

  /** End date of crawl period */
  crawl_date_end: Timestamp

  /** When we ingested this crawl */
  ingested_at: Timestamp

  /** Processing status */
  status: 'pending' | 'processing' | 'complete' | 'failed'

  /** Total hosts in this crawl */
  total_hosts: bigint

  /** Total edges in this crawl */
  total_edges: bigint

  /** Processing duration in milliseconds */
  processing_duration_ms: bigint

  /** Bytes processed */
  bytes_processed: bigint

  /** Error message if failed */
  error_message: string | null
}

/**
 * Parquet schema for crawl_metadata.parquet
 */
export const CRAWL_METADATA_PARQUET_SCHEMA = {
  crawl_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  display_name: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  source_url: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  crawl_date_start: { type: 'INT64', encoding: 'PLAIN' },
  crawl_date_end: { type: 'INT64', encoding: 'PLAIN' },
  ingested_at: { type: 'INT64', encoding: 'PLAIN' },
  status: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  total_hosts: { type: 'INT64', encoding: 'PLAIN' },
  total_edges: { type: 'INT64', encoding: 'PLAIN' },
  processing_duration_ms: { type: 'INT64', encoding: 'PLAIN' },
  bytes_processed: { type: 'INT64', encoding: 'PLAIN' },
  error_message: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
} as const

// ============================================================================
// Storage Layout
// ============================================================================

/**
 * Storage path conventions for the host graph
 *
 * Structure:
 * /hostgraph/
 *   crawls/
 *     {crawl_id}/
 *       hosts/
 *         tld={tld}/
 *           data.parquet
 *       links/
 *         forward/
 *           tld={tld}/
 *             data.parquet
 *         reverse/
 *           tld={tld}/
 *             data.parquet
 *       metrics/
 *         tld={tld}/
 *           data.parquet
 *   aggregations/
 *     tld_stats.parquet
 *   metadata/
 *     crawls.parquet
 *   checkpoints/
 *     {timestamp}/
 */
export const STORAGE_PATHS = {
  /** Root path */
  root: '/hostgraph',

  /** Hosts by TLD partition */
  hosts: (crawlId: CrawlId, tld: TLD) =>
    `/hostgraph/crawls/${crawlId}/hosts/tld=${tld}/data.parquet`,

  /** Forward links (outbound) by source TLD */
  linksForward: (crawlId: CrawlId, tld: TLD) =>
    `/hostgraph/crawls/${crawlId}/links/forward/tld=${tld}/data.parquet`,

  /** Reverse links (inbound) by target TLD */
  linksReverse: (crawlId: CrawlId, tld: TLD) =>
    `/hostgraph/crawls/${crawlId}/links/reverse/tld=${tld}/data.parquet`,

  /** Metrics by TLD */
  metrics: (crawlId: CrawlId, tld: TLD) =>
    `/hostgraph/crawls/${crawlId}/metrics/tld=${tld}/data.parquet`,

  /** TLD aggregations */
  tldStats: () => `/hostgraph/aggregations/tld_stats.parquet`,

  /** Crawl metadata */
  crawlMetadata: () => `/hostgraph/metadata/crawls.parquet`,

  /** Checkpoint */
  checkpoint: (timestamp: string) =>
    `/hostgraph/checkpoints/${timestamp}/manifest.json`,

  /** High-degree node adjacency list */
  adjacencyList: (crawlId: CrawlId, hostId: HostId) =>
    `/hostgraph/crawls/${crawlId}/adjacency/${hostId}.parquet`,
} as const

// ============================================================================
// Configuration
// ============================================================================

/**
 * Host graph loader configuration
 */
export interface HostGraphConfig {
  /** Storage bucket */
  bucket: string

  /** Maximum concurrent downloads */
  maxConcurrentDownloads: number

  /** Chunk size for streaming (bytes) */
  chunkSize: number

  /** Row group size for Parquet files */
  rowGroupSize: number

  /** Enable Bloom filters for edge lookups */
  enableBloomFilters: boolean

  /** Bloom filter false positive rate */
  bloomFilterFPR: number

  /** Threshold for creating dedicated adjacency files */
  adjacencyThreshold: number

  /** TLDs to process (empty = all) */
  tldFilter: TLD[]

  /** Number of PageRank iterations */
  pagerankIterations: number

  /** PageRank damping factor */
  pagerankDamping: number
}

/**
 * Default configuration
 */
export const DEFAULT_CONFIG: HostGraphConfig = {
  bucket: 'parquedb-hostgraph',
  maxConcurrentDownloads: 4,
  chunkSize: 64 * 1024 * 1024, // 64MB chunks
  rowGroupSize: 100_000,
  enableBloomFilters: true,
  bloomFilterFPR: 0.01,
  adjacencyThreshold: 10_000, // Create adjacency file for hosts with >10k edges
  tldFilter: [],
  pagerankIterations: 20,
  pagerankDamping: 0.85,
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Parse reversed hostname into components
 *
 * @example
 * parseReversedHostname("com.example.www") => { tld: "com", sld: "example", subdomain: "www" }
 * parseReversedHostname("uk.co.bbc.www") => { tld: "uk", sld: "co.bbc", subdomain: "www" }
 */
export function parseReversedHostname(reversed: string): {
  tld: TLD
  sld: string
  subdomain: string | null
  normalized: string
} {
  const parts = reversed.split('.')

  if (parts.length === 0) {
    throw new Error(`Invalid reversed hostname: ${reversed}`)
  }

  const tld = parts[0] as TLD

  // Handle special two-part TLDs (e.g., "co.uk", "com.au")
  const twoPartTlds = new Set(['co', 'com', 'org', 'net', 'gov', 'edu', 'ac'])
  let sldStart = 1

  if (parts.length > 2 && twoPartTlds.has(parts[1])) {
    // This is a two-part TLD like "uk.co"
    sldStart = 2
  }

  const sld = parts.length > sldStart ? parts[sldStart] : parts[1] || ''
  const subdomain = parts.length > sldStart + 1
    ? parts.slice(sldStart + 1).join('.')
    : null

  // Convert back to normal hostname
  const normalized = parts.reverse().join('.')

  return { tld, sld, subdomain, normalized }
}

/**
 * Convert normal hostname to reversed format
 *
 * @example
 * reverseHostname("www.example.com") => "com.example.www"
 */
export function reverseHostname(hostname: string): ReversedHostname {
  // Strip leading "www." as Common Crawl does
  const stripped = hostname.replace(/^www\./, '')
  return stripped.split('.').reverse().join('.') as ReversedHostname
}

/**
 * Extract TLD from reversed hostname
 */
export function extractTLD(reversed: ReversedHostname): TLD {
  const dotIndex = reversed.indexOf('.')
  return (dotIndex === -1 ? reversed : reversed.slice(0, dotIndex)) as TLD
}

/**
 * Compute edge hash for consistent partitioning
 */
export function computeEdgeHash(fromId: HostId, toId: HostId): bigint {
  // Simple hash combining both IDs
  // In production, use xxHash64 for better distribution
  const combined = `${fromId}:${toId}`
  let hash = BigInt(0)
  for (let i = 0; i < combined.length; i++) {
    hash = (hash * BigInt(31) + BigInt(combined.charCodeAt(i))) & BigInt('0xFFFFFFFFFFFFFFFF')
  }
  return hash
}

/**
 * Compute hostname hash for partitioning
 */
export function computeHostnameHash(hostname: string): bigint {
  let hash = BigInt(0)
  for (let i = 0; i < hostname.length; i++) {
    hash = (hash * BigInt(31) + BigInt(hostname.charCodeAt(i))) & BigInt('0xFFFFFFFFFFFFFFFF')
  }
  return hash
}
