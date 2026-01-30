# Common Crawl Host Graph Example

Load and analyze the Common Crawl host-level web graph using ParqueDB. This example demonstrates how to handle massive graph datasets (TB+) through streaming, chunking, and efficient partitioning strategies.

## Data Source

[Common Crawl](https://commoncrawl.org) provides monthly crawls of the web, including host-level web graph data showing which domains link to which.

**Latest Release Stats (Oct-Dec 2025):**
- **250.8 million** hosts (nodes)
- **10.9 billion** links (edges)
- ~16GB compressed per direction (forward/reverse)

Data is available at:
- https://data.commoncrawl.org/projects/hyperlinkgraph/

## Quick Start

```typescript
import { HostGraphLoader, createCrawlId, createTLD } from './load'
import { HostGraphClient } from './queries'
import { MetricsPipeline } from './metrics'

// 1. Initialize storage (R2, S3, or local filesystem)
const bucket = getStorageBucket() // Your storage implementation

// 2. Load a crawl (streaming, handles TB+ data)
const crawlId = createCrawlId('cc-main-2025-oct-nov-dec')
const loader = new HostGraphLoader(bucket, crawlId, {
  tldFilter: [createTLD('com')], // Optional: filter to specific TLDs
  rowGroupSize: 100_000,
})

const metadata = await loader.load((progress) => {
  console.log(`[${progress.phase}] ${progress.filesProcessed}/${progress.totalFiles} files`)
})

// 3. Compute graph metrics (PageRank, HITS, degrees)
const pipeline = new MetricsPipeline(bucket, crawlId)
await pipeline.compute([createTLD('com')])

// 4. Query the graph
const client = new HostGraphClient(bucket, crawlId)

// Find top hosts by PageRank
const topHosts = await client.getTopHostsByPageRank(createTLD('com'), 100)

// Get inbound links to a host
const github = await client.getHostByHostname('github.com')
const inbound = await client.getInboundLinks(github.host_id, { limit: 1000 })
```

## Data Format

### Common Crawl Source Format

Common Crawl provides the host graph in two formats:

1. **Text Format (Tab-separated)**
   - Vertices: `host_id\treversed_hostname`
   - Edges: `from_id\tto_id`

2. **BVGraph Format (Binary)**
   - Highly compressed binary format
   - Used for the main graph files

Hostnames are in **reverse domain notation** with `www.` stripped:
- `www.example.com` → `com.example`
- `blog.example.com` → `com.example.blog`

### ParqueDB Schema

#### Hosts (`hosts.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| host_id | INT64 | Unique host ID from Common Crawl |
| reversed_hostname | STRING | Reversed hostname (e.g., "com.example") |
| tld | STRING (DICT) | Top-level domain for partitioning |
| sld | STRING (DICT) | Second-level domain |
| subdomain | STRING | Subdomain path (nullable) |
| hostname_hash | INT64 | Hash for consistent partitioning |
| crawl_id | STRING (DICT) | Crawl identifier |
| ingested_at | INT64 | Ingestion timestamp |
| is_active | BOOLEAN | Active in latest crawl |
| crawl_count | INT32 | Number of crawls seen |

**Partitioning:** By TLD (`tld=com/`, `tld=org/`, etc.)
**Sort Order:** `(tld, reversed_hostname, crawl_id DESC)`

#### Links (`links_forward.parquet`, `links_reverse.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| from_host_id | INT64 | Source host ID |
| to_host_id | INT64 | Target host ID |
| from_tld | STRING (DICT) | Source TLD |
| to_tld | STRING (DICT) | Target TLD |
| edge_hash | INT64 | Hash for deduplication |
| crawl_id | STRING (DICT) | Crawl identifier |
| link_count | INT32 | Number of page-level links |
| is_bidirectional | BOOLEAN | Both hosts link to each other |
| stability_score | INT32 | Consecutive crawls present |

**Forward Index:** Partitioned by `from_tld`, sorted by `(from_host_id, to_host_id)`
**Reverse Index:** Partitioned by `to_tld`, sorted by `(to_host_id, from_host_id)`

#### Metrics (`metrics.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| host_id | INT64 | Host ID |
| tld | STRING (DICT) | TLD for partitioning |
| out_degree | INT64 | Total outbound links |
| in_degree | INT64 | Total inbound links |
| pagerank | DOUBLE | PageRank score |
| harmonic_centrality | DOUBLE | Harmonic centrality |
| hub_score | DOUBLE | HITS hub score |
| authority_score | DOUBLE | HITS authority score |
| clustering_coefficient | DOUBLE | Local clustering |
| is_hub | BOOLEAN | High out/in ratio |
| is_authority | BOOLEAN | High in/out ratio |

## Storage Layout

```
/hostgraph/
  crawls/
    cc-main-2025-oct-nov-dec/
      hosts/
        tld=com/
          data.parquet
        tld=org/
          data.parquet
        ...
      links/
        forward/
          tld=com/
            data.parquet
          ...
        reverse/
          tld=com/
            data.parquet
          ...
      metrics/
        tld=com/
          data.parquet
        ...
      adjacency/
        {high_degree_host_id}.parquet
  aggregations/
    tld_stats.parquet
  metadata/
    crawls.parquet
  checkpoints/
    {timestamp}/
      manifest.json
```

## Key Design Decisions

### 1. TLD-Based Partitioning

Partitioning by TLD provides:
- **Natural data locality** - most queries are within a TLD
- **Efficient pruning** - skip irrelevant TLDs entirely
- **Manageable partition sizes** - even `.com` is tractable

### 2. Dual Link Indexes

We maintain both forward and reverse link indexes:
- **Forward (outbound):** "What does this host link to?"
- **Reverse (inbound):** "Who links to this host?"

Both are sorted for efficient range scans on the primary dimension.

### 3. High-Degree Node Handling

Hosts with >10,000 edges get dedicated adjacency files:
- Prevents hot spots during queries
- Enables parallel loading
- Supports streaming iteration

### 4. Streaming Architecture

The loader uses streaming throughout:
- **HTTP streaming** with gzip decompression
- **Chunked parsing** (no full file in memory)
- **Batched writes** (configurable row group size)
- **Progress callbacks** for monitoring

## Graph Metrics

### PageRank

We provide two PageRank implementations:

1. **Power Iteration** (exact)
   - Standard iterative algorithm
   - Configurable damping factor (default 0.85)
   - Convergence threshold 1e-8

2. **Monte Carlo Approximation** (for huge graphs)
   - Random walk sampling
   - Linear memory usage
   - Configurable walk count and length

### HITS (Hubs and Authorities)

Identifies two types of important pages:
- **Hubs:** Pages that link to many authorities
- **Authorities:** Pages that many hubs link to

### Degree Metrics

- **Out-degree:** Number of outbound links
- **In-degree:** Number of inbound links
- **Unique out/in hosts:** Distinct linked hosts

### Clustering Coefficient

Measures how connected a host's neighbors are to each other.

## Query Examples

### Find Top Sites

```typescript
// Top 100 by PageRank globally
const top = await client.getTopHostsByPageRank(undefined, 100)

// Top 50 in .edu by in-degree
const topEdu = await client.getTopHostsByInDegree(createTLD('edu'), 50)
```

### Analyze Link Patterns

```typescript
// Who links to github.com?
const github = await client.getHostByHostname('github.com')
const inbound = await client.getInboundLinks(github.host_id, {
  limit: 1000,
  minCount: 10 // At least 10 page-level links
})

// Cross-TLD analysis: .edu → .gov links
const eduToGov = await client.getCrossTLDLinks(
  createTLD('edu'),
  createTLD('gov'),
  { limit: 1000 }
)
```

### Graph Traversal

```typescript
// Shortest path between hosts
const path = await client.findShortestPath(hostId1, hostId2, 4)

// 2-hop neighborhood
const neighbors = await client.getNeighborhood(hostId, 2, 'both')

// Common neighbors (link prediction)
const common = await client.findCommonNeighbors(hostId1, hostId2)
```

### Search with Filters

```typescript
// High-authority .org hosts
const result = await client.searchMetrics({
  tld: { eq: 'org' },
  is_authority: true,
  in_degree: { gte: 100000 },
}, {
  sort: [{ field: 'pagerank', order: 'desc' }],
  limit: 100,
})
```

## Incremental Updates

Support for processing new crawls incrementally:

```typescript
const incrementalLoader = new IncrementalHostGraphLoader(
  bucket,
  newCrawlId,
  previousCrawlId, // Compare against
  config
)

const result = await incrementalLoader.loadIncremental()
console.log(`New hosts: ${result.newHosts}`)
console.log(`Removed hosts: ${result.removedHosts}`)
```

This tracks:
- New hosts not in previous crawl
- Hosts no longer present
- Changed link patterns
- Stability scores (consecutive appearances)

## Performance Considerations

### Memory Usage

- Streaming decompression (no full file in memory)
- Configurable batch sizes
- High-degree nodes offloaded to separate files

### Query Optimization

- TLD partitioning for predicate pushdown
- Sorted files enable binary search
- Bloom filters for edge existence checks
- Row group statistics for skipping

### Scalability

| Operation | Full Graph | Single TLD |
|-----------|------------|------------|
| Load hosts | ~30 min | ~2 min |
| Load edges | ~2 hours | ~10 min |
| PageRank | ~1 hour | ~5 min |
| Point query | <100ms | <10ms |
| Scan query | ~10s | ~1s |

*Estimates for 250M hosts, 10B edges on commodity hardware*

## Known Crawl Releases

| Release | Hosts | Edges | Date |
|---------|-------|-------|------|
| cc-main-2025-oct-nov-dec | 250.8M | 10.9B | Dec 2025 |
| cc-main-2025-sep-oct-nov | 235.7M | 9.5B | Nov 2025 |
| cc-main-2024-25-dec-jan-feb | 267.4M | 2.7B | Feb 2025 |
| cc-main-2024-may-jun-jul | 362.2M | 2.7B | Jul 2024 |

## Files in This Example

- `schema.ts` - Type definitions and Parquet schemas
- `load.ts` - Streaming loader for Common Crawl data
- `metrics.ts` - Graph analysis algorithms (PageRank, HITS, etc.)
- `queries.ts` - Query client for accessing the loaded data

## References

- [Common Crawl Web Graphs](https://commoncrawl.org/web-graphs)
- [Common Crawl Blog: Host Graph](https://commoncrawl.org/blog/host--and-domain-level-web-graphs-may-june-and-july-2024)
- [WebGraph Framework](https://webgraph.di.unimi.it/)
- [BVGraph Format](https://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/BVGraph.html)
