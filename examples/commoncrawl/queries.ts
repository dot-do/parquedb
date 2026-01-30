/**
 * Common Crawl Host Graph Queries
 *
 * Query functions for analyzing the host-level web graph.
 * Designed for efficient access patterns using ParqueDB's
 * partitioning and indexing strategies.
 */

import type {
  Host,
  Link,
  HostMetrics,
  TLDStats,
  HostId,
  TLD,
  CrawlId,
  ReversedHostname,
  Timestamp,
} from './schema'

import {
  createHostId,
  createCrawlId,
  createTLD,
  createReversedHostname,
  reverseHostname,
  extractTLD,
  parseReversedHostname,
  STORAGE_PATHS,
} from './schema'

// ============================================================================
// Types
// ============================================================================

/**
 * Storage bucket interface
 */
export interface StorageBucket {
  get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  list(options?: { prefix?: string }): Promise<{ objects: { key: string }[] }>
}

/**
 * Query result with pagination
 */
export interface QueryResult<T> {
  items: T[]
  total?: number
  nextCursor?: string
  hasMore: boolean
}

/**
 * Sort specification
 */
export interface SortSpec {
  field: string
  order: 'asc' | 'desc'
}

/**
 * Filter operators
 */
export type FilterOp<T> =
  | { eq: T }
  | { neq: T }
  | { gt: T }
  | { gte: T }
  | { lt: T }
  | { lte: T }
  | { in: T[] }
  | { contains: string }
  | { startsWith: string }

/**
 * Host filter
 */
export interface HostFilter {
  tld?: FilterOp<string>
  sld?: FilterOp<string>
  hostname?: FilterOp<string>
  crawl_id?: FilterOp<string>
  is_active?: boolean
  crawl_count?: FilterOp<number>
}

/**
 * Link filter
 */
export interface LinkFilter {
  from_host_id?: FilterOp<number>
  to_host_id?: FilterOp<number>
  from_tld?: FilterOp<string>
  to_tld?: FilterOp<string>
  crawl_id?: FilterOp<string>
  link_count?: FilterOp<number>
  is_bidirectional?: boolean
}

/**
 * Metrics filter
 */
export interface MetricsFilter {
  host_id?: FilterOp<number>
  tld?: FilterOp<string>
  crawl_id?: FilterOp<string>
  out_degree?: FilterOp<number>
  in_degree?: FilterOp<number>
  pagerank?: FilterOp<number>
  is_hub?: boolean
  is_authority?: boolean
}

// ============================================================================
// Query Client
// ============================================================================

/**
 * Host Graph Query Client
 *
 * Provides efficient query access to the host graph data.
 */
export class HostGraphClient {
  private bucket: StorageBucket
  private crawlId: CrawlId

  constructor(bucket: StorageBucket, crawlId: CrawlId) {
    this.bucket = bucket
    this.crawlId = crawlId
  }

  // ==========================================================================
  // Host Queries
  // ==========================================================================

  /**
   * Get host by ID
   */
  async getHost(hostId: HostId): Promise<Host | null> {
    // Would need to scan or use index - in production use bloom filter
    const tlds = await this.listTLDs()

    for (const tld of tlds) {
      const hosts = await this.getHostsByTLD(tld)
      const host = hosts.find(h => h.host_id === hostId)
      if (host) return host
    }

    return null
  }

  /**
   * Get host by hostname
   */
  async getHostByHostname(hostname: string): Promise<Host | null> {
    const reversed = reverseHostname(hostname)
    const tld = extractTLD(reversed)

    const hosts = await this.getHostsByTLD(tld)
    return hosts.find(h => h.reversed_hostname === reversed) || null
  }

  /**
   * Get all hosts for a TLD
   */
  async getHostsByTLD(tld: TLD, limit?: number): Promise<Host[]> {
    const path = STORAGE_PATHS.hosts(this.crawlId, tld)
    const data = await this.bucket.get(path)

    if (!data) return []

    const buffer = await data.arrayBuffer()
    const hosts = JSON.parse(new TextDecoder().decode(buffer)) as Host[]

    return limit ? hosts.slice(0, limit) : hosts
  }

  /**
   * Search hosts with filters
   */
  async searchHosts(
    filter: HostFilter,
    options: {
      sort?: SortSpec[]
      limit?: number
      offset?: number
    } = {}
  ): Promise<QueryResult<Host>> {
    const { sort, limit = 100, offset = 0 } = options

    // Determine which TLDs to scan
    let tlds: TLD[]
    if (filter.tld && 'eq' in filter.tld) {
      tlds = [filter.tld.eq as TLD]
    } else if (filter.tld && 'in' in filter.tld) {
      tlds = filter.tld.in as TLD[]
    } else {
      tlds = await this.listTLDs()
    }

    // Collect matching hosts
    let allHosts: Host[] = []
    for (const tld of tlds) {
      const hosts = await this.getHostsByTLD(tld)
      const filtered = hosts.filter(h => this.matchesHostFilter(h, filter))
      allHosts.push(...filtered)
    }

    // Sort
    if (sort && sort.length > 0) {
      allHosts.sort((a, b) => {
        for (const s of sort) {
          const aVal = (a as unknown as Record<string, unknown>)[s.field]
          const bVal = (b as unknown as Record<string, unknown>)[s.field]
          const cmp = this.compare(aVal, bVal)
          if (cmp !== 0) return s.order === 'asc' ? cmp : -cmp
        }
        return 0
      })
    }

    // Paginate
    const total = allHosts.length
    const items = allHosts.slice(offset, offset + limit)
    const hasMore = offset + limit < total

    return {
      items,
      total,
      hasMore,
      nextCursor: hasMore ? String(offset + limit) : undefined,
    }
  }

  /**
   * Count hosts matching filter
   */
  async countHosts(filter: HostFilter): Promise<number> {
    const result = await this.searchHosts(filter, { limit: 1 })
    return result.total || 0
  }

  // ==========================================================================
  // Link Queries
  // ==========================================================================

  /**
   * Get outbound links from a host
   */
  async getOutboundLinks(
    hostId: HostId,
    options: { limit?: number; minCount?: number } = {}
  ): Promise<Link[]> {
    const { limit, minCount = 1 } = options

    // First, find the host's TLD
    const host = await this.getHost(hostId)
    if (!host) return []

    const path = STORAGE_PATHS.linksForward(this.crawlId, host.tld)
    const data = await this.bucket.get(path)

    if (!data) return []

    const buffer = await data.arrayBuffer()
    const links = JSON.parse(new TextDecoder().decode(buffer)) as Link[]

    let filtered = links.filter(
      l => l.from_host_id === hostId && l.link_count >= minCount
    )

    if (limit) {
      filtered = filtered.slice(0, limit)
    }

    return filtered
  }

  /**
   * Get inbound links to a host
   */
  async getInboundLinks(
    hostId: HostId,
    options: { limit?: number; minCount?: number } = {}
  ): Promise<Link[]> {
    const { limit, minCount = 1 } = options

    // First, find the host's TLD
    const host = await this.getHost(hostId)
    if (!host) return []

    const path = STORAGE_PATHS.linksReverse(this.crawlId, host.tld)
    const data = await this.bucket.get(path)

    if (!data) return []

    const buffer = await data.arrayBuffer()
    const links = JSON.parse(new TextDecoder().decode(buffer)) as Link[]

    let filtered = links.filter(
      l => l.to_host_id === hostId && l.link_count >= minCount
    )

    if (limit) {
      filtered = filtered.slice(0, limit)
    }

    return filtered
  }

  /**
   * Check if a link exists between two hosts
   */
  async hasLink(fromHostId: HostId, toHostId: HostId): Promise<boolean> {
    const links = await this.getOutboundLinks(fromHostId, { limit: 10000 })
    return links.some(l => l.to_host_id === toHostId)
  }

  /**
   * Get cross-TLD links
   */
  async getCrossTLDLinks(
    fromTld: TLD,
    toTld: TLD,
    options: { limit?: number } = {}
  ): Promise<Link[]> {
    const { limit = 1000 } = options

    const path = STORAGE_PATHS.linksForward(this.crawlId, fromTld)
    const data = await this.bucket.get(path)

    if (!data) return []

    const buffer = await data.arrayBuffer()
    const links = JSON.parse(new TextDecoder().decode(buffer)) as Link[]

    return links
      .filter(l => l.from_tld === fromTld && l.to_tld === toTld)
      .slice(0, limit)
  }

  /**
   * Search links with filters
   */
  async searchLinks(
    filter: LinkFilter,
    options: {
      sort?: SortSpec[]
      limit?: number
      offset?: number
    } = {}
  ): Promise<QueryResult<Link>> {
    const { sort, limit = 100, offset = 0 } = options

    // Determine which TLDs to scan
    let tlds: TLD[]
    if (filter.from_tld && 'eq' in filter.from_tld) {
      tlds = [filter.from_tld.eq as TLD]
    } else if (filter.from_tld && 'in' in filter.from_tld) {
      tlds = filter.from_tld.in as TLD[]
    } else {
      tlds = await this.listTLDs()
    }

    // Collect matching links
    let allLinks: Link[] = []
    for (const tld of tlds) {
      const path = STORAGE_PATHS.linksForward(this.crawlId, tld)
      const data = await this.bucket.get(path)

      if (data) {
        const buffer = await data.arrayBuffer()
        const links = JSON.parse(new TextDecoder().decode(buffer)) as Link[]
        const filtered = links.filter(l => this.matchesLinkFilter(l, filter))
        allLinks.push(...filtered)
      }
    }

    // Sort
    if (sort && sort.length > 0) {
      allLinks.sort((a, b) => {
        for (const s of sort) {
          const aVal = (a as unknown as Record<string, unknown>)[s.field]
          const bVal = (b as unknown as Record<string, unknown>)[s.field]
          const cmp = this.compare(aVal, bVal)
          if (cmp !== 0) return s.order === 'asc' ? cmp : -cmp
        }
        return 0
      })
    }

    // Paginate
    const total = allLinks.length
    const items = allLinks.slice(offset, offset + limit)
    const hasMore = offset + limit < total

    return {
      items,
      total,
      hasMore,
      nextCursor: hasMore ? String(offset + limit) : undefined,
    }
  }

  // ==========================================================================
  // Metrics Queries
  // ==========================================================================

  /**
   * Get metrics for a host
   */
  async getHostMetrics(hostId: HostId): Promise<HostMetrics | null> {
    const host = await this.getHost(hostId)
    if (!host) return null

    const path = STORAGE_PATHS.metrics(this.crawlId, host.tld)
    const data = await this.bucket.get(path)

    if (!data) return null

    const buffer = await data.arrayBuffer()
    const metrics = JSON.parse(new TextDecoder().decode(buffer)) as HostMetrics[]

    return metrics.find(m => m.host_id === hostId) || null
  }

  /**
   * Get top hosts by PageRank
   */
  async getTopHostsByPageRank(
    tld?: TLD,
    limit: number = 100
  ): Promise<HostMetrics[]> {
    const tlds = tld ? [tld] : await this.listTLDs()
    let allMetrics: HostMetrics[] = []

    for (const t of tlds) {
      const path = STORAGE_PATHS.metrics(this.crawlId, t)
      const data = await this.bucket.get(path)

      if (data) {
        const buffer = await data.arrayBuffer()
        const metrics = JSON.parse(new TextDecoder().decode(buffer)) as HostMetrics[]
        allMetrics.push(...metrics)
      }
    }

    // Sort by PageRank descending
    allMetrics.sort((a, b) => b.pagerank - a.pagerank)

    return allMetrics.slice(0, limit)
  }

  /**
   * Get top hosts by in-degree
   */
  async getTopHostsByInDegree(
    tld?: TLD,
    limit: number = 100
  ): Promise<HostMetrics[]> {
    const tlds = tld ? [tld] : await this.listTLDs()
    let allMetrics: HostMetrics[] = []

    for (const t of tlds) {
      const path = STORAGE_PATHS.metrics(this.crawlId, t)
      const data = await this.bucket.get(path)

      if (data) {
        const buffer = await data.arrayBuffer()
        const metrics = JSON.parse(new TextDecoder().decode(buffer)) as HostMetrics[]
        allMetrics.push(...metrics)
      }
    }

    // Sort by in-degree descending
    allMetrics.sort((a, b) => Number(b.in_degree - a.in_degree))

    return allMetrics.slice(0, limit)
  }

  /**
   * Get hub hosts (high out-degree)
   */
  async getHubs(tld?: TLD, limit: number = 100): Promise<HostMetrics[]> {
    const tlds = tld ? [tld] : await this.listTLDs()
    let allMetrics: HostMetrics[] = []

    for (const t of tlds) {
      const path = STORAGE_PATHS.metrics(this.crawlId, t)
      const data = await this.bucket.get(path)

      if (data) {
        const buffer = await data.arrayBuffer()
        const metrics = JSON.parse(new TextDecoder().decode(buffer)) as HostMetrics[]
        allMetrics.push(...metrics.filter(m => m.is_hub))
      }
    }

    // Sort by hub score descending
    allMetrics.sort((a, b) => b.hub_score - a.hub_score)

    return allMetrics.slice(0, limit)
  }

  /**
   * Get authority hosts (high in-degree)
   */
  async getAuthorities(tld?: TLD, limit: number = 100): Promise<HostMetrics[]> {
    const tlds = tld ? [tld] : await this.listTLDs()
    let allMetrics: HostMetrics[] = []

    for (const t of tlds) {
      const path = STORAGE_PATHS.metrics(this.crawlId, t)
      const data = await this.bucket.get(path)

      if (data) {
        const buffer = await data.arrayBuffer()
        const metrics = JSON.parse(new TextDecoder().decode(buffer)) as HostMetrics[]
        allMetrics.push(...metrics.filter(m => m.is_authority))
      }
    }

    // Sort by authority score descending
    allMetrics.sort((a, b) => b.authority_score - a.authority_score)

    return allMetrics.slice(0, limit)
  }

  /**
   * Search metrics with filters
   */
  async searchMetrics(
    filter: MetricsFilter,
    options: {
      sort?: SortSpec[]
      limit?: number
      offset?: number
    } = {}
  ): Promise<QueryResult<HostMetrics>> {
    const { sort, limit = 100, offset = 0 } = options

    // Determine which TLDs to scan
    let tlds: TLD[]
    if (filter.tld && 'eq' in filter.tld) {
      tlds = [filter.tld.eq as TLD]
    } else if (filter.tld && 'in' in filter.tld) {
      tlds = filter.tld.in as TLD[]
    } else {
      tlds = await this.listTLDs()
    }

    // Collect matching metrics
    let allMetrics: HostMetrics[] = []
    for (const tld of tlds) {
      const path = STORAGE_PATHS.metrics(this.crawlId, tld)
      const data = await this.bucket.get(path)

      if (data) {
        const buffer = await data.arrayBuffer()
        const metrics = JSON.parse(new TextDecoder().decode(buffer)) as HostMetrics[]
        const filtered = metrics.filter(m => this.matchesMetricsFilter(m, filter))
        allMetrics.push(...filtered)
      }
    }

    // Sort
    if (sort && sort.length > 0) {
      allMetrics.sort((a, b) => {
        for (const s of sort) {
          const aVal = (a as unknown as Record<string, unknown>)[s.field]
          const bVal = (b as unknown as Record<string, unknown>)[s.field]
          const cmp = this.compare(aVal, bVal)
          if (cmp !== 0) return s.order === 'asc' ? cmp : -cmp
        }
        return 0
      })
    }

    // Paginate
    const total = allMetrics.length
    const items = allMetrics.slice(offset, offset + limit)
    const hasMore = offset + limit < total

    return {
      items,
      total,
      hasMore,
      nextCursor: hasMore ? String(offset + limit) : undefined,
    }
  }

  // ==========================================================================
  // TLD Statistics
  // ==========================================================================

  /**
   * Get TLD statistics
   */
  async getTLDStats(tld: TLD): Promise<TLDStats | null> {
    const path = STORAGE_PATHS.tldStats()
    const data = await this.bucket.get(path)

    if (!data) return null

    const buffer = await data.arrayBuffer()
    const stats = JSON.parse(new TextDecoder().decode(buffer)) as TLDStats[]

    return stats.find(s => s.tld === tld && s.crawl_id === this.crawlId) || null
  }

  /**
   * Get all TLD statistics for current crawl
   */
  async getAllTLDStats(): Promise<TLDStats[]> {
    const path = STORAGE_PATHS.tldStats()
    const data = await this.bucket.get(path)

    if (!data) return []

    const buffer = await data.arrayBuffer()
    const stats = JSON.parse(new TextDecoder().decode(buffer)) as TLDStats[]

    return stats.filter(s => s.crawl_id === this.crawlId)
  }

  /**
   * Get top TLDs by host count
   */
  async getTopTLDs(limit: number = 20): Promise<TLDStats[]> {
    const stats = await this.getAllTLDStats()
    stats.sort((a, b) => Number(b.host_count - a.host_count))
    return stats.slice(0, limit)
  }

  // ==========================================================================
  // Graph Traversal
  // ==========================================================================

  /**
   * Find shortest path between two hosts (BFS)
   */
  async findShortestPath(
    fromHostId: HostId,
    toHostId: HostId,
    maxDepth: number = 6
  ): Promise<HostId[] | null> {
    if (fromHostId === toHostId) return [fromHostId]

    const visited = new Set<HostId>([fromHostId])
    const queue: Array<{ hostId: HostId; path: HostId[] }> = [
      { hostId: fromHostId, path: [fromHostId] }
    ]

    while (queue.length > 0) {
      const { hostId, path } = queue.shift()!

      if (path.length > maxDepth) continue

      const outbound = await this.getOutboundLinks(hostId, { limit: 1000 })

      for (const link of outbound) {
        if (link.to_host_id === toHostId) {
          return [...path, toHostId]
        }

        if (!visited.has(link.to_host_id)) {
          visited.add(link.to_host_id)
          queue.push({
            hostId: link.to_host_id,
            path: [...path, link.to_host_id]
          })
        }
      }
    }

    return null
  }

  /**
   * Get N-hop neighborhood of a host
   */
  async getNeighborhood(
    hostId: HostId,
    hops: number = 1,
    direction: 'out' | 'in' | 'both' = 'out'
  ): Promise<Set<HostId>> {
    const neighbors = new Set<HostId>()
    let frontier = new Set<HostId>([hostId])

    for (let i = 0; i < hops; i++) {
      const nextFrontier = new Set<HostId>()

      for (const currentHost of Array.from(frontier)) {
        if (direction === 'out' || direction === 'both') {
          const outbound = await this.getOutboundLinks(currentHost, { limit: 1000 })
          for (const link of outbound) {
            if (!neighbors.has(link.to_host_id) && link.to_host_id !== hostId) {
              neighbors.add(link.to_host_id)
              nextFrontier.add(link.to_host_id)
            }
          }
        }

        if (direction === 'in' || direction === 'both') {
          const inbound = await this.getInboundLinks(currentHost, { limit: 1000 })
          for (const link of inbound) {
            if (!neighbors.has(link.from_host_id) && link.from_host_id !== hostId) {
              neighbors.add(link.from_host_id)
              nextFrontier.add(link.from_host_id)
            }
          }
        }
      }

      frontier = nextFrontier
    }

    return neighbors
  }

  /**
   * Find common neighbors between two hosts
   */
  async findCommonNeighbors(
    hostId1: HostId,
    hostId2: HostId
  ): Promise<HostId[]> {
    const outbound1 = await this.getOutboundLinks(hostId1, { limit: 10000 })
    const outbound2 = await this.getOutboundLinks(hostId2, { limit: 10000 })

    const set1 = new Set(outbound1.map(l => l.to_host_id))
    const common: HostId[] = []

    for (const link of outbound2) {
      if (set1.has(link.to_host_id)) {
        common.push(link.to_host_id)
      }
    }

    return common
  }

  // ==========================================================================
  // Utility Methods
  // ==========================================================================

  /**
   * List all TLDs in the current crawl
   */
  async listTLDs(): Promise<TLD[]> {
    const prefix = `/hostgraph/crawls/${this.crawlId}/hosts/`
    const result = await this.bucket.list({ prefix })

    const tlds = new Set<TLD>()
    for (const obj of result.objects) {
      // Extract TLD from path like "/hostgraph/crawls/{id}/hosts/tld=com/data.parquet"
      const match = obj.key.match(/tld=([^/]+)/)
      if (match) {
        tlds.add(match[1] as TLD)
      }
    }

    return Array.from(tlds).sort()
  }

  /**
   * Match host against filter
   */
  private matchesHostFilter(host: Host, filter: HostFilter): boolean {
    if (filter.tld && !this.matchesOp(host.tld, filter.tld)) return false
    if (filter.sld && !this.matchesOp(host.sld, filter.sld)) return false
    if (filter.crawl_id && !this.matchesOp(host.crawl_id, filter.crawl_id)) return false
    if (filter.is_active !== undefined && host.is_active !== filter.is_active) return false
    if (filter.crawl_count && !this.matchesOp(host.crawl_count, filter.crawl_count)) return false
    return true
  }

  /**
   * Match link against filter
   */
  private matchesLinkFilter(link: Link, filter: LinkFilter): boolean {
    if (filter.from_host_id && !this.matchesOp(link.from_host_id, filter.from_host_id)) return false
    if (filter.to_host_id && !this.matchesOp(link.to_host_id, filter.to_host_id)) return false
    if (filter.from_tld && !this.matchesOp(link.from_tld, filter.from_tld)) return false
    if (filter.to_tld && !this.matchesOp(link.to_tld, filter.to_tld)) return false
    if (filter.crawl_id && !this.matchesOp(link.crawl_id, filter.crawl_id)) return false
    if (filter.link_count && !this.matchesOp(link.link_count, filter.link_count)) return false
    if (filter.is_bidirectional !== undefined && link.is_bidirectional !== filter.is_bidirectional) return false
    return true
  }

  /**
   * Match metrics against filter
   */
  private matchesMetricsFilter(metrics: HostMetrics, filter: MetricsFilter): boolean {
    if (filter.host_id && !this.matchesOp(metrics.host_id, filter.host_id)) return false
    if (filter.tld && !this.matchesOp(metrics.tld, filter.tld)) return false
    if (filter.crawl_id && !this.matchesOp(metrics.crawl_id, filter.crawl_id)) return false
    if (filter.out_degree && !this.matchesOp(Number(metrics.out_degree), filter.out_degree)) return false
    if (filter.in_degree && !this.matchesOp(Number(metrics.in_degree), filter.in_degree)) return false
    if (filter.pagerank && !this.matchesOp(metrics.pagerank, filter.pagerank)) return false
    if (filter.is_hub !== undefined && metrics.is_hub !== filter.is_hub) return false
    if (filter.is_authority !== undefined && metrics.is_authority !== filter.is_authority) return false
    return true
  }

  /**
   * Match value against filter operator
   */
  private matchesOp<T>(value: T, op: FilterOp<T>): boolean {
    if ('eq' in op) return value === op.eq
    if ('neq' in op) return value !== op.neq
    if ('gt' in op) return value > op.gt
    if ('gte' in op) return value >= op.gte
    if ('lt' in op) return value < op.lt
    if ('lte' in op) return value <= op.lte
    if ('in' in op) return (op.in as T[]).includes(value)
    if ('contains' in op) return String(value).includes(op.contains)
    if ('startsWith' in op) return String(value).startsWith(op.startsWith)
    return true
  }

  /**
   * Compare two values
   */
  private compare(a: unknown, b: unknown): number {
    if (typeof a === 'number' && typeof b === 'number') return a - b
    if (typeof a === 'bigint' && typeof b === 'bigint') return Number(a - b)
    if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b)
    return 0
  }
}

// ============================================================================
// Example Usage
// ============================================================================

/**
 * Example queries demonstrating the API
 */
export async function exampleQueries(client: HostGraphClient) {
  // Find a host by hostname
  const github = await client.getHostByHostname('github.com')
  console.log('GitHub host:', github)

  // Get top hosts by PageRank in .com TLD
  const topCom = await client.getTopHostsByPageRank('com' as TLD, 10)
  console.log('Top .com hosts by PageRank:', topCom)

  // Find who links to a host
  if (github) {
    const inbound = await client.getInboundLinks(github.host_id, { limit: 100 })
    console.log(`Sites linking to GitHub: ${inbound.length}`)
  }

  // Search for high-authority hosts
  const authorities = await client.searchMetrics(
    {
      is_authority: true,
      in_degree: { gte: 1000000 },
    },
    {
      sort: [{ field: 'in_degree', order: 'desc' }],
      limit: 20,
    }
  )
  console.log('High-authority hosts:', authorities.items)

  // Get TLD statistics
  const comStats = await client.getTLDStats('com' as TLD)
  console.log('.com TLD stats:', comStats)

  // Find path between two hosts
  if (github) {
    const path = await client.findShortestPath(
      github.host_id,
      1 as HostId, // Some other host ID
      4
    )
    console.log('Shortest path:', path)
  }
}
