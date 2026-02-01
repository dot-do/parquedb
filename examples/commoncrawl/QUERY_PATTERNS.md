# Common Crawl Host Graph Query Patterns for ParqueDB

**Real-World Query Patterns for Web Graph Analysis**

This document describes 20 production-ready query patterns for the Common Crawl host graph dataset, organized by use case. Each pattern includes the business question, filter structure, recommended index columns, selectivity analysis, and row-group statistics pushdown benefits.

---

## Table of Contents

1. [Data Model Overview](#data-model-overview)
2. [Index Column Strategy](#index-column-strategy)
3. [Query Pattern Summary Table](#query-pattern-summary-table)
4. [SEO and Link Analysis](#seo-and-link-analysis)
5. [Security and Threat Intelligence](#security-and-threat-intelligence)
6. [Competitive Intelligence](#competitive-intelligence)
7. [Academic Web Research](#academic-web-research)
8. [Brand Monitoring](#brand-monitoring)
9. [Domain Authority and Scoring](#domain-authority-and-scoring)
10. [Web Archiving and Preservation](#web-archiving-and-preservation)

---

## Data Model Overview

The Common Crawl host graph in ParqueDB consists of four main entity types:

### Host Entity
```typescript
interface Host {
  host_id: HostId           // Numeric ID from Common Crawl
  reversed_hostname: string // "com.example.www" for lexicographic sorting
  tld: string               // Top-level domain ("com", "org", "uk")
  sld: string               // Second-level domain ("example")
  subdomain: string | null  // Subdomain path ("www.blog")
  hostname_hash: bigint     // xxHash64 for consistent partitioning
  crawl_id: string          // Crawl identifier
  is_active: boolean        // Active in latest crawl
  crawl_count: number       // Times observed across crawls
  first_seen_crawl: string  // First crawl appearance
  last_seen_crawl: string   // Last crawl appearance
}
```

### Link Entity
```typescript
interface Link {
  from_host_id: HostId      // Source host
  to_host_id: HostId        // Target host
  from_tld: string          // Source TLD
  to_tld: string            // Target TLD
  edge_hash: bigint         // Hash for partitioning
  crawl_id: string          // Crawl identifier
  link_count: number        // Aggregated page-level links
  is_bidirectional: boolean // Both hosts link to each other
  first_seen_crawl: string  // First observation
  stability_score: number   // Consecutive crawls present
}
```

### HostMetrics Entity
```typescript
interface HostMetrics {
  host_id: HostId
  tld: string
  crawl_id: string
  out_degree: bigint        // Outbound link count
  in_degree: bigint         // Inbound link count
  unique_out_hosts: bigint  // Distinct hosts linked to
  unique_in_hosts: bigint   // Distinct hosts linking in
  pagerank: number          // PageRank approximation
  harmonic_centrality: number
  hub_score: number         // HITS hub score
  authority_score: number   // HITS authority score
  clustering_coefficient: number
  is_hub: boolean
  is_authority: boolean
}
```

### TLDStats Entity
```typescript
interface TLDStats {
  tld: string
  crawl_id: string
  host_count: bigint
  total_out_links: bigint
  total_in_links: bigint
  avg_pagerank: number
  median_in_degree: number
  p99_in_degree: bigint
  internal_links: bigint
  external_links: bigint
  top_target_tlds: string   // JSON array
}
```

---

## Index Column Strategy

ParqueDB uses a dual Variant architecture with shredded index columns:

```
$id | $index_* columns | $data (Variant blob)
```

### Recommended Index Columns by Collection

**hosts collection:**
```typescript
{
  '$id': 'STRING',                    // Entity ID (ULID)
  '$index_tld': 'STRING',             // TLD for partition pruning
  '$index_reversed_hostname': 'STRING', // For prefix/range queries
  '$index_sld': 'STRING',             // Second-level domain lookups
  '$index_host_id': 'INT64',          // Numeric ID lookups
  '$index_is_active': 'BOOLEAN',      // Filter active hosts
  '$index_crawl_count': 'INT32',      // Longevity filtering
  '$index_crawl_id': 'STRING',        // Crawl-specific queries
  '$data': 'VARIANT'                  // Full entity data
}
```

**links collection:**
```typescript
{
  '$id': 'STRING',
  '$index_from_host_id': 'INT64',     // Outbound edge lookups
  '$index_to_host_id': 'INT64',       // Inbound edge lookups
  '$index_from_tld': 'STRING',        // Source TLD filtering
  '$index_to_tld': 'STRING',          // Target TLD filtering
  '$index_stability_score': 'INT32',  // Link quality filtering
  '$index_is_bidirectional': 'BOOLEAN', // Mutual link detection
  '$index_crawl_id': 'STRING',
  '$data': 'VARIANT'
}
```

**metrics collection:**
```typescript
{
  '$id': 'STRING',
  '$index_host_id': 'INT64',
  '$index_tld': 'STRING',
  '$index_pagerank': 'DOUBLE',        // Authority ranking
  '$index_in_degree': 'INT64',        // Backlink count
  '$index_out_degree': 'INT64',       // Outlink count
  '$index_authority_score': 'DOUBLE', // HITS authority
  '$index_hub_score': 'DOUBLE',       // HITS hub
  '$index_is_authority': 'BOOLEAN',
  '$index_is_hub': 'BOOLEAN',
  '$index_crawl_id': 'STRING',
  '$data': 'VARIANT'
}
```

---

## Query Pattern Summary Table

| # | Use Case | Pattern Name | Filter Columns | Selectivity | Stats Pushdown | Bloom Filter |
|---|----------|--------------|----------------|-------------|----------------|--------------|
| 1 | SEO | Backlink Profile | `$index_to_host_id` | High | Yes (INT64 range) | Yes |
| 2 | SEO | Outbound Links | `$index_from_host_id` | High | Yes | Yes |
| 3 | SEO | TLD Distribution | `$index_to_host_id`, `$index_from_tld` | Medium | Yes | Partial |
| 4 | SEO | Link Velocity | `$index_to_host_id`, `$index_crawl_id` | High | Yes | Yes |
| 5 | Security | Suspicious TLD Links | `$index_from_tld`, `$index_to_tld` | Medium | Yes | No |
| 6 | Security | Link Farm Detection | `$index_from_host_id`, `$index_out_degree` | Low | Yes | No |
| 7 | Security | New Domain Analysis | `$index_crawl_count`, `$index_in_degree` | Medium | Yes | No |
| 8 | Competitive | Competitor Overlap | `$index_to_host_id` (multi) | High | Yes | Yes |
| 9 | Competitive | Link Gap Analysis | `$index_to_host_id`, `$index_from_host_id` | High | Yes | Yes |
| 10 | Competitive | Market Share by TLD | `$index_tld`, `$index_pagerank` | Medium | Yes | No |
| 11 | Research | Web Graph Sampling | `$index_tld`, `$index_pagerank` | Low | Yes | No |
| 12 | Research | TLD Interconnectivity | `$index_from_tld`, `$index_to_tld` | Low | Yes | No |
| 13 | Research | Temporal Evolution | `$index_host_id`, `$index_crawl_id` | High | Yes | Yes |
| 14 | Brand | Brand Mention Links | `$index_reversed_hostname` | High | Yes (prefix) | Partial |
| 15 | Brand | Subdomain Discovery | `$index_sld`, `$index_tld` | High | Yes | No |
| 16 | Brand | Citation Network | `$index_to_host_id`, `$index_from_tld` | Medium | Yes | Yes |
| 17 | Authority | PageRank Ranking | `$index_tld`, `$index_pagerank` | Low | Yes | No |
| 18 | Authority | Authority Hosts | `$index_is_authority`, `$index_tld` | Medium | Yes | No |
| 19 | Archive | Domain Longevity | `$index_crawl_count`, `$index_first_seen_crawl` | Medium | Yes | No |
| 20 | Archive | Link Stability | `$index_stability_score`, `$index_from_host_id` | Medium | Yes | No |

---

## SEO and Link Analysis

### Pattern 1: Backlink Profile Analysis

**Business Question:** "What sites link to my domain? What's my backlink profile quality?"

**Use Case:** SEO tools (Ahrefs, Moz, SEMrush) need to show users all domains linking to their site, with metrics.

```typescript
// Query: Get all backlinks to a specific host
const backlinks = await db.collection('links').find({
  '$index_to_host_id': targetHostId
}, {
  limit: 1000,
  sort: { stability_score: -1 }  // Most stable links first
})

// Join with metrics for link quality
const linkingHosts = await db.collection('metrics').find({
  '$index_host_id': { $in: backlinks.map(l => l.fromHostId) }
})
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id` (INT64) |
| Selectivity | **High** - Single host ID returns small subset |
| Row-Group Pushdown | **Yes** - INT64 min/max statistics enable skipping |
| Bloom Filter | **Yes** - Exact host_id lookups benefit from bloom |
| Typical Result Size | 100 - 100,000 links per domain |

---

### Pattern 2: Outbound Link Analysis

**Business Question:** "What sites does this domain link to? Are there toxic outbound links?"

**Use Case:** Audit outbound links for SEO health and spam detection.

```typescript
// Query: Get all outbound links from a host
const outboundLinks = await db.collection('links').find({
  '$index_from_host_id': sourceHostId
})

// Check for links to known spam TLDs
const suspiciousLinks = outboundLinks.filter(link =>
  SPAM_TLDS.includes(link.toTld)
)
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_from_host_id` (INT64) |
| Selectivity | **High** - Single source host |
| Row-Group Pushdown | **Yes** - Forward index sorted by from_host_id |
| Bloom Filter | **Yes** - Point lookups |
| Typical Result Size | 10 - 10,000 links per domain |

---

### Pattern 3: Referring Domain TLD Distribution

**Business Question:** "What's the geographic/TLD distribution of sites linking to me?"

**Use Case:** Understand international reach and link portfolio diversity.

```typescript
// Query: Count backlinks grouped by source TLD
const tldDistribution = await db.collection('links').aggregate([
  { $match: { '$index_to_host_id': targetHostId } },
  { $group: { _id: '$fromTld', count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 20 }
])
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id`, `$index_from_tld` |
| Selectivity | **Medium** - Filtered by host, then grouped |
| Row-Group Pushdown | **Yes** - host_id filter first |
| Bloom Filter | **Partial** - Host ID only |
| Notes | Benefits from composite index on (to_host_id, from_tld) |

---

### Pattern 4: Link Velocity Tracking

**Business Question:** "How many new backlinks did I gain/lose between crawls?"

**Use Case:** Monitor link building campaign effectiveness and detect negative SEO attacks.

```typescript
// Query: Compare backlinks between two crawl snapshots
const currentLinks = await db.collection('links').find({
  '$index_to_host_id': targetHostId,
  '$index_crawl_id': 'cc-main-2025-oct-nov-dec'
})

const previousLinks = await db.collection('links').find({
  '$index_to_host_id': targetHostId,
  '$index_crawl_id': 'cc-main-2025-sep-oct-nov'
})

// Calculate delta
const currentSet = new Set(currentLinks.map(l => l.fromHostId))
const previousSet = new Set(previousLinks.map(l => l.fromHostId))

const gained = [...currentSet].filter(id => !previousSet.has(id))
const lost = [...previousSet].filter(id => !currentSet.has(id))
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id`, `$index_crawl_id` |
| Selectivity | **High** - Specific host + crawl |
| Row-Group Pushdown | **Yes** - Both columns filterable |
| Bloom Filter | **Yes** - Compound key bloom (host_id + crawl_id) |
| Notes | Partition by crawl_id enables efficient temporal queries |

---

## Security and Threat Intelligence

### Pattern 5: Suspicious TLD Cross-Linking

**Business Question:** "Which domains in suspicious TLDs (.xyz, .top, .tk) link to legitimate sites?"

**Use Case:** Identify potential spam networks or malicious link injection campaigns.

```typescript
// Query: Find links from suspicious TLDs to legitimate TLDs
const suspiciousLinks = await db.collection('links').find({
  '$index_from_tld': { $in: ['xyz', 'top', 'tk', 'buzz', 'work'] },
  '$index_to_tld': { $in: ['com', 'org', 'gov', 'edu'] }
}, {
  limit: 10000
})

// Group by target to find heavily-targeted legitimate sites
const targetCounts = groupBy(suspiciousLinks, 'toHostId')
const highRiskTargets = Object.entries(targetCounts)
  .filter(([_, links]) => links.length > 100)
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_from_tld`, `$index_to_tld` |
| Selectivity | **Medium** - TLD cardinality is ~1,500 |
| Row-Group Pushdown | **Yes** - String dictionary encoding helps |
| Bloom Filter | **No** - Range/multi-value queries |
| Notes | Use TLD stats for quick volume estimates first |

---

### Pattern 6: Link Farm Detection

**Business Question:** "Which hosts have abnormally high out-degree with low authority?"

**Use Case:** Detect Private Blog Networks (PBNs) and link farms.

```typescript
// Query: Find hosts with high outlinks but low PageRank
const suspiciousHosts = await db.collection('metrics').find({
  '$index_out_degree': { $gt: 1000 },
  '$index_pagerank': { $lt: 0.0001 },
  '$index_is_hub': false  // Not a legitimate hub
}, {
  limit: 5000,
  sort: { out_degree: -1 }
})

// Cross-reference: Do these hosts link to the same targets?
const targetOverlap = await analyzeCommonTargets(suspiciousHosts)
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_out_degree`, `$index_pagerank`, `$index_is_hub` |
| Selectivity | **Low** - Scans many metrics records |
| Row-Group Pushdown | **Yes** - Numeric range filters |
| Bloom Filter | **No** - Range queries |
| Notes | Consider pre-computed "suspicious_score" index column |

---

### Pattern 7: New Domain Risk Analysis

**Business Question:** "Which recently-discovered domains are receiving unusual backlink patterns?"

**Use Case:** Early detection of spam domains or compromised sites.

```typescript
// Query: Find hosts with few crawl observations but high in-degree
const newHighAuthorityHosts = await db.collection('hosts').find({
  '$index_crawl_count': { $lte: 2 },  // New domains
  '$index_is_active': true
})

// Join with metrics to find unusually popular new domains
const metrics = await db.collection('metrics').find({
  '$index_host_id': { $in: newHighAuthorityHosts.map(h => h.hostId) },
  '$index_in_degree': { $gt: 100 }  // Suspicious for new domain
})
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_crawl_count`, `$index_is_active`, `$index_in_degree` |
| Selectivity | **Medium** - New domains are minority |
| Row-Group Pushdown | **Yes** - INT32 crawl_count filterable |
| Bloom Filter | **No** - Range/compound queries |
| Notes | Time-travel queries can compare crawl-over-crawl changes |

---

## Competitive Intelligence

### Pattern 8: Competitor Backlink Overlap

**Business Question:** "Which sites link to my competitors but not to me?"

**Use Case:** Identify link building opportunities by finding competitor backlink sources.

```typescript
// Query: Get backlinks to multiple competitors
const competitorIds = [competitor1Id, competitor2Id, competitor3Id]

const competitorBacklinks = await db.collection('links').find({
  '$index_to_host_id': { $in: competitorIds }
})

// Group by source host
const sourceToCompetitors = new Map()
for (const link of competitorBacklinks) {
  if (!sourceToCompetitors.has(link.fromHostId)) {
    sourceToCompetitors.set(link.fromHostId, new Set())
  }
  sourceToCompetitors.get(link.fromHostId).add(link.toHostId)
}

// Find sources that link to 2+ competitors (high value prospects)
const prospects = [...sourceToCompetitors.entries()]
  .filter(([_, targets]) => targets.size >= 2)
  .filter(([sourceId, _]) => !myBacklinkSources.has(sourceId))
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id` |
| Selectivity | **High** - Few competitor IDs |
| Row-Group Pushdown | **Yes** - IN clause can use stats |
| Bloom Filter | **Yes** - Multiple exact lookups |
| Notes | Benefits from sorted storage by to_host_id |

---

### Pattern 9: Link Gap Analysis

**Business Question:** "What linking domains do I have that competitors don't?"

**Use Case:** Identify unique backlink advantages and defensive link building priorities.

```typescript
// Query: Get my backlinks and competitor backlinks
const myBacklinks = await db.collection('links').find({
  '$index_to_host_id': myHostId
})

const competitorBacklinks = await db.collection('links').find({
  '$index_to_host_id': competitorId
})

// Calculate exclusive backlinks
const mySourceSet = new Set(myBacklinks.map(l => l.fromHostId))
const competitorSourceSet = new Set(competitorBacklinks.map(l => l.fromHostId))

const uniqueToMe = [...mySourceSet].filter(id => !competitorSourceSet.has(id))
const uniqueToCompetitor = [...competitorSourceSet].filter(id => !mySourceSet.has(id))
const shared = [...mySourceSet].filter(id => competitorSourceSet.has(id))
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id` |
| Selectivity | **High** |
| Row-Group Pushdown | **Yes** |
| Bloom Filter | **Yes** |
| Notes | Parallel queries for each domain enable fast comparison |

---

### Pattern 10: Market Share by TLD Segment

**Business Question:** "What's my PageRank share within the .edu or .gov ecosystem?"

**Use Case:** Understand position within specific market verticals.

```typescript
// Query: Get all hosts in a TLD with PageRank
const eduHosts = await db.collection('metrics').find({
  '$index_tld': 'edu'
}, {
  projection: { hostId: 1, pagerank: 1 }
})

// Calculate market share
const totalPageRank = eduHosts.reduce((sum, h) => sum + h.pagerank, 0)
const myHost = eduHosts.find(h => h.hostId === myHostId)
const marketShare = myHost ? (myHost.pagerank / totalPageRank) * 100 : 0
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_tld`, `$index_pagerank` |
| Selectivity | **Medium** - TLD filters significantly |
| Row-Group Pushdown | **Yes** - TLD uses dictionary encoding |
| Bloom Filter | **No** - Scanning all in TLD |
| Notes | Pre-computed TLDStats avoids full scan for common TLDs |

---

## Academic Web Research

### Pattern 11: Stratified Web Graph Sampling

**Business Question:** "How do I sample the web graph proportionally by TLD and authority?"

**Use Case:** Academic research requiring representative web graph samples.

```typescript
// Query: Stratified sample across TLDs
const SAMPLE_SIZE_PER_TLD = 1000

const tldStats = await db.collection('tld_stats').find({})
const topTlds = tldStats
  .sort((a, b) => b.hostCount - a.hostCount)
  .slice(0, 20)

const samples = []
for (const tld of topTlds) {
  // Sample hosts across PageRank distribution
  const highAuthority = await db.collection('metrics').find({
    '$index_tld': tld.tld,
    '$index_pagerank': { $gt: 0.001 }
  }, { limit: SAMPLE_SIZE_PER_TLD / 3 })

  const mediumAuthority = await db.collection('metrics').find({
    '$index_tld': tld.tld,
    '$index_pagerank': { $gte: 0.00001, $lte: 0.001 }
  }, { limit: SAMPLE_SIZE_PER_TLD / 3 })

  const lowAuthority = await db.collection('metrics').find({
    '$index_tld': tld.tld,
    '$index_pagerank': { $lt: 0.00001 }
  }, { limit: SAMPLE_SIZE_PER_TLD / 3 })

  samples.push(...highAuthority, ...mediumAuthority, ...lowAuthority)
}
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_tld`, `$index_pagerank` |
| Selectivity | **Low** - Designed for broad sampling |
| Row-Group Pushdown | **Yes** - Range queries on pagerank |
| Bloom Filter | **No** - Sampling workload |
| Notes | Reservoir sampling can be applied in-query |

---

### Pattern 12: TLD Interconnectivity Matrix

**Business Question:** "How do TLDs connect to each other? What's the link flow between country-code TLDs?"

**Use Case:** Study international web connectivity patterns.

```typescript
// Query: Build TLD-to-TLD link matrix
const tldPairs = await db.collection('links').aggregate([
  { $group: {
    _id: { from: '$fromTld', to: '$toTld' },
    count: { $sum: 1 },
    totalWeight: { $sum: '$linkCount' }
  }},
  { $sort: { count: -1 } }
])

// Build adjacency matrix
const matrix = {}
for (const pair of tldPairs) {
  if (!matrix[pair._id.from]) matrix[pair._id.from] = {}
  matrix[pair._id.from][pair._id.to] = pair.count
}
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_from_tld`, `$index_to_tld` |
| Selectivity | **Low** - Full aggregation |
| Row-Group Pushdown | **Yes** - Dictionary-encoded TLDs compress well |
| Bloom Filter | **No** - Full scan aggregation |
| Notes | Pre-computed in TLDStats.top_target_tlds for common queries |

---

### Pattern 13: Temporal Graph Evolution

**Business Question:** "How has this host's link neighborhood changed over time?"

**Use Case:** Study web graph dynamics and link decay.

```typescript
// Query: Track host links across multiple crawls
const hostId = targetHostId
const crawls = ['cc-main-2024-may-jun-jul', 'cc-main-2025-sep-oct-nov', 'cc-main-2025-oct-nov-dec']

const timeline = []
for (const crawlId of crawls) {
  const inbound = await db.collection('links').find({
    '$index_to_host_id': hostId,
    '$index_crawl_id': crawlId
  })

  const outbound = await db.collection('links').find({
    '$index_from_host_id': hostId,
    '$index_crawl_id': crawlId
  })

  timeline.push({
    crawl: crawlId,
    inDegree: inbound.length,
    outDegree: outbound.length,
    uniqueReferrers: new Set(inbound.map(l => l.fromHostId)).size
  })
}
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id`/`$index_from_host_id`, `$index_crawl_id` |
| Selectivity | **High** - Specific host + crawl |
| Row-Group Pushdown | **Yes** - Compound filter |
| Bloom Filter | **Yes** - Point lookups per crawl |
| Notes | Time-travel queries leverage CDC event log |

---

## Brand Monitoring

### Pattern 14: Brand Domain Discovery

**Business Question:** "What domains contain our brand name or variations?"

**Use Case:** Trademark protection, typosquatting detection, affiliate monitoring.

```typescript
// Query: Find hosts with brand in reversed hostname
// "com.mybrand" matches mybrand.com, www.mybrand.com, etc.
const brandHosts = await db.collection('hosts').find({
  '$index_reversed_hostname': { $prefix: 'com.mybrand' }
})

// Also check for typosquatting variations
const typoVariations = ['com.mybrnad', 'com.mybr4nd', 'com.my-brand']
const typosquatters = await db.collection('hosts').find({
  '$index_reversed_hostname': { $in: typoVariations.map(v => ({ $prefix: v })) }
})
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_reversed_hostname` |
| Selectivity | **High** - Prefix match is selective |
| Row-Group Pushdown | **Yes** - Sorted by reversed_hostname enables prefix scan |
| Bloom Filter | **Partial** - Prefix queries use range bounds |
| Notes | Reversed hostname enables efficient "*.brand.com" queries |

---

### Pattern 15: Subdomain Enumeration

**Business Question:** "What subdomains exist for our domains?"

**Use Case:** Asset discovery, security auditing, shadow IT detection.

```typescript
// Query: Find all hosts with specific SLD and TLD
const subdomains = await db.collection('hosts').find({
  '$index_sld': 'example',
  '$index_tld': 'com',
  '$index_is_active': true
}, {
  projection: { subdomain: 1, reversedHostname: 1, crawlCount: 1 }
})

// Group by subdomain pattern
const subdomainPatterns = groupBy(subdomains, h =>
  h.subdomain?.split('.')[0] || 'apex'
)
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_sld`, `$index_tld`, `$index_is_active` |
| Selectivity | **High** - SLD + TLD is very selective |
| Row-Group Pushdown | **Yes** - All filters pushable |
| Bloom Filter | **No** - Multi-column compound query |
| Notes | Useful for security teams and brand protection |

---

### Pattern 16: Citation Network Analysis

**Business Question:** "Which authoritative sites (by TLD) cite our brand?"

**Use Case:** PR monitoring, media coverage tracking.

```typescript
// Query: Find backlinks from prestigious TLDs
const citations = await db.collection('links').find({
  '$index_to_host_id': brandHostId,
  '$index_from_tld': { $in: ['edu', 'gov', 'org'] }
})

// Enrich with source metrics
const sourceMetrics = await db.collection('metrics').find({
  '$index_host_id': { $in: citations.map(c => c.fromHostId) }
})

// Rank by authority
const rankedCitations = citations
  .map(c => ({
    ...c,
    sourceAuthority: sourceMetrics.find(m => m.hostId === c.fromHostId)?.pagerank || 0
  }))
  .sort((a, b) => b.sourceAuthority - a.sourceAuthority)
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id`, `$index_from_tld` |
| Selectivity | **Medium** - Filtered by both dimensions |
| Row-Group Pushdown | **Yes** |
| Bloom Filter | **Yes** - Host ID lookup |
| Notes | Combine with metrics join for authority weighting |

---

## Domain Authority and Scoring

### Pattern 17: Top Domains by PageRank

**Business Question:** "What are the highest-authority domains in a TLD?"

**Use Case:** Authority scoring, benchmark comparisons, market analysis.

```typescript
// Query: Get top 100 domains by PageRank in a TLD
const topDomains = await db.collection('metrics').find({
  '$index_tld': 'com',
  '$index_crawl_id': 'cc-main-2025-oct-nov-dec'
}, {
  sort: { pagerank: -1 },
  limit: 100,
  projection: { hostId: 1, pagerank: 1, inDegree: 1 }
})

// Resolve host names
const hostDetails = await db.collection('hosts').find({
  '$index_host_id': { $in: topDomains.map(d => d.hostId) }
})
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_tld`, `$index_pagerank`, `$index_crawl_id` |
| Selectivity | **Low** - Scans TLD partition |
| Row-Group Pushdown | **Yes** - TLD filters, pagerank sorts |
| Bloom Filter | **No** - Top-K query |
| Notes | Consider secondary index sorted by (tld, pagerank DESC) |

---

### Pattern 18: Authority Site Discovery

**Business Question:** "Which sites are classified as authorities in a niche?"

**Use Case:** Influencer identification, citation source discovery.

```typescript
// Query: Find authority sites (high in-degree relative to out-degree)
const authorities = await db.collection('metrics').find({
  '$index_is_authority': true,
  '$index_tld': { $in: ['com', 'org', 'net'] },
  '$index_authority_score': { $gt: 0.1 }
}, {
  sort: { authorityScore: -1 },
  limit: 500
})

// Filter by topic (requires $data access for full entity)
const topicAuthorities = authorities.filter(h =>
  h.data?.topics?.includes('technology')
)
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_is_authority`, `$index_tld`, `$index_authority_score` |
| Selectivity | **Medium** - Boolean + score filter |
| Row-Group Pushdown | **Yes** - Boolean and numeric filters |
| Bloom Filter | **No** - Range/multi-value query |
| Notes | `is_authority` boolean provides fast first filter |

---

## Web Archiving and Preservation

### Pattern 19: Long-Lived Domain Analysis

**Business Question:** "Which domains have persisted across many crawls?"

**Use Case:** Web archiving prioritization, historical analysis.

```typescript
// Query: Find domains present in most crawls
const persistentDomains = await db.collection('hosts').find({
  '$index_crawl_count': { $gte: 10 },  // Present in 10+ crawls
  '$index_is_active': true
}, {
  sort: { crawlCount: -1 },
  limit: 10000
})

// Analyze first appearance distribution
const firstSeenDistribution = groupBy(persistentDomains, h =>
  h.firstSeenCrawl?.split('-')[0]  // Year
)
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_crawl_count`, `$index_is_active` |
| Selectivity | **Medium** - Long-lived domains are minority |
| Row-Group Pushdown | **Yes** - INT32 crawl_count range |
| Bloom Filter | **No** - Range query |
| Notes | `first_seen_crawl` enables temporal cohort analysis |

---

### Pattern 20: Link Stability Analysis

**Business Question:** "Which links have persisted across multiple crawls?"

**Use Case:** Identify stable reference links vs. ephemeral links.

```typescript
// Query: Find high-stability links to a domain
const stableBacklinks = await db.collection('links').find({
  '$index_to_host_id': targetHostId,
  '$index_stability_score': { $gte: 5 }  // Present in 5+ consecutive crawls
}, {
  sort: { stabilityScore: -1 }
})

// Calculate stability distribution
const stabilityDistribution = await db.collection('links').aggregate([
  { $match: { '$index_to_host_id': targetHostId } },
  { $group: { _id: '$stabilityScore', count: { $sum: 1 } } },
  { $sort: { _id: 1 } }
])
```

| Aspect | Value |
|--------|-------|
| Primary Index | `$index_to_host_id`, `$index_stability_score` |
| Selectivity | **Medium** - Host filter first, then score |
| Row-Group Pushdown | **Yes** - INT32 stability_score |
| Bloom Filter | **Yes** - Host ID lookup |
| Notes | Stability score enables link quality assessment |

---

## Optimization Recommendations

### Index Column Selection

For the Common Crawl host graph, prioritize these index columns:

1. **Always index:** `host_id`, `tld`, `crawl_id` - Used in nearly every query
2. **High value:** `pagerank`, `in_degree`, `out_degree` - Core metrics filtering
3. **Medium value:** `is_authority`, `is_hub`, `stability_score` - Boolean/quality filters
4. **Specialized:** `reversed_hostname`, `sld` - Brand/subdomain queries

### Row-Group Statistics

The following columns benefit most from min/max statistics pushdown:

| Column | Type | Benefit |
|--------|------|---------|
| `host_id` | INT64 | Range pruning for sorted partitions |
| `pagerank` | DOUBLE | Top-K and threshold queries |
| `in_degree`/`out_degree` | INT64 | Degree distribution queries |
| `stability_score` | INT32 | Quality filtering |
| `crawl_count` | INT32 | Temporal filtering |

### Bloom Filter Strategy

Enable bloom filters for:

- `host_id` columns (exact lookups)
- Compound keys: `(to_host_id, from_host_id)` for edge existence
- `reversed_hostname` (point lookups)

### Partition Strategy

Partition data by:

1. **TLD** - Primary partition key for locality
2. **Crawl ID** - Secondary partition for temporal queries
3. **Host ID hash** - For distributed processing

---

## Performance Expectations

| Query Pattern | Expected Latency | Data Scanned |
|---------------|------------------|--------------|
| Single host backlinks | 10-50ms | 1-5 row groups |
| TLD-filtered aggregation | 100-500ms | TLD partition |
| Cross-TLD analysis | 1-5s | Multiple partitions |
| Full graph sampling | 10-60s | Sampled row groups |
| Temporal comparison | 50-200ms | 2x single host query |

---

*Query Patterns Document - Common Crawl Host Graph for ParqueDB*
*Last Updated: 2026-01-31*
