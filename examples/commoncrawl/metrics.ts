/**
 * Common Crawl Host Graph Metrics
 *
 * Graph analysis algorithms for computing centrality, PageRank,
 * and other metrics on the host-level web graph.
 *
 * Designed for streaming computation on TB+ datasets.
 */

import type {
  Host,
  Link,
  HostMetrics,
  TLDStats,
  HostId,
  TLD,
  CrawlId,
  Timestamp,
  HostGraphConfig,
} from './schema'

import {
  createTimestamp,
  DEFAULT_CONFIG,
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
  put(key: string, value: ArrayBuffer | Uint8Array): Promise<void>
  list(options?: { prefix?: string }): Promise<{ objects: { key: string }[] }>
}

/**
 * Progress callback for metrics computation
 */
export interface MetricsProgress {
  phase: 'degree' | 'pagerank' | 'hits' | 'clustering' | 'aggregation'
  iteration?: number
  totalIterations?: number
  tld?: TLD
  hostsProcessed: bigint
  totalHosts: bigint
  elapsedMs: number
}

export type MetricsProgressCallback = (progress: MetricsProgress) => void

/**
 * Sparse adjacency representation for a single host
 */
interface HostAdjacency {
  hostId: HostId
  outNeighbors: HostId[]
  inNeighbors: HostId[]
  outWeights?: number[]
  inWeights?: number[]
}

/**
 * PageRank state for iterative computation
 */
interface PageRankState {
  scores: Map<HostId, number>
  outDegree: Map<HostId, number>
  danglingNodes: Set<HostId>
}

/**
 * HITS (Hyperlink-Induced Topic Search) state
 */
interface HITSState {
  hubScores: Map<HostId, number>
  authorityScores: Map<HostId, number>
}

// ============================================================================
// Degree Computation
// ============================================================================

/**
 * Compute in-degree and out-degree for all hosts
 *
 * Uses streaming to handle large graphs.
 */
export async function computeDegrees(
  bucket: StorageBucket,
  crawlId: CrawlId,
  tlds: TLD[],
  onProgress?: MetricsProgressCallback
): Promise<{
  outDegree: Map<HostId, bigint>
  inDegree: Map<HostId, bigint>
  uniqueOutHosts: Map<HostId, Set<HostId>>
  uniqueInHosts: Map<HostId, Set<HostId>>
}> {
  const outDegree = new Map<HostId, bigint>()
  const inDegree = new Map<HostId, bigint>()
  const uniqueOutHosts = new Map<HostId, Set<HostId>>()
  const uniqueInHosts = new Map<HostId, Set<HostId>>()

  let hostsProcessed = BigInt(0)
  const startTime = Date.now()

  for (const tld of tlds) {
    onProgress?.({
      phase: 'degree',
      tld,
      hostsProcessed,
      totalHosts: BigInt(0), // Unknown
      elapsedMs: Date.now() - startTime,
    })

    // Read forward links for this TLD
    const forwardPath = STORAGE_PATHS.linksForward(crawlId, tld)
    const forwardData = await bucket.get(forwardPath)

    if (forwardData) {
      const buffer = await forwardData.arrayBuffer()
      const links = JSON.parse(new TextDecoder().decode(buffer)) as Link[]

      for (const link of links) {
        // Out-degree
        const currentOut = outDegree.get(link.from_host_id) || BigInt(0)
        outDegree.set(link.from_host_id, currentOut + BigInt(link.link_count))

        // In-degree
        const currentIn = inDegree.get(link.to_host_id) || BigInt(0)
        inDegree.set(link.to_host_id, currentIn + BigInt(link.link_count))

        // Unique hosts
        const outSet = uniqueOutHosts.get(link.from_host_id) || new Set()
        outSet.add(link.to_host_id)
        uniqueOutHosts.set(link.from_host_id, outSet)

        const inSet = uniqueInHosts.get(link.to_host_id) || new Set()
        inSet.add(link.from_host_id)
        uniqueInHosts.set(link.to_host_id, inSet)

        hostsProcessed++
      }
    }
  }

  return { outDegree, inDegree, uniqueOutHosts, uniqueInHosts }
}

// ============================================================================
// PageRank Computation
// ============================================================================

/**
 * Compute PageRank using power iteration
 *
 * This implementation uses chunked processing for large graphs.
 * For TB+ graphs, consider using approximation algorithms like
 * Monte Carlo PageRank or partition-based approaches.
 */
export class PageRankComputer {
  private damping: number
  private iterations: number
  private convergenceThreshold: number

  constructor(config: Partial<HostGraphConfig> = {}) {
    const fullConfig = { ...DEFAULT_CONFIG, ...config }
    this.damping = fullConfig.pagerankDamping
    this.iterations = fullConfig.pagerankIterations
    this.convergenceThreshold = 1e-8
  }

  /**
   * Initialize PageRank state from degree information
   */
  initializeState(
    outDegree: Map<HostId, bigint>,
    inDegree: Map<HostId, bigint>
  ): PageRankState {
    const allHosts = new Set([...Array.from(outDegree.keys()), ...Array.from(inDegree.keys())])
    const n = allHosts.size
    const initialScore = 1 / n

    const scores = new Map<HostId, number>()
    const danglingNodes = new Set<HostId>()

    for (const hostId of Array.from(allHosts)) {
      scores.set(hostId, initialScore)
      const degree = outDegree.get(hostId) || BigInt(0)
      if (degree === BigInt(0)) {
        danglingNodes.add(hostId)
      }
    }

    return {
      scores,
      outDegree: new Map(Array.from(outDegree.entries()).map(([k, v]) => [k, Number(v)])),
      danglingNodes,
    }
  }

  /**
   * Run PageRank iteration
   *
   * For streaming, this processes edges in chunks and accumulates contributions.
   */
  async computeIteration(
    state: PageRankState,
    getOutNeighbors: (hostId: HostId) => Promise<HostId[]>,
    onProgress?: MetricsProgressCallback
  ): Promise<{ newScores: Map<HostId, number>; delta: number }> {
    const n = state.scores.size
    const newScores = new Map<HostId, number>()

    // Initialize with teleportation probability
    const teleportProb = (1 - this.damping) / n
    for (const hostId of Array.from(state.scores.keys())) {
      newScores.set(hostId, teleportProb)
    }

    // Compute dangling node contribution
    let danglingSum = 0
    for (const hostId of Array.from(state.danglingNodes)) {
      danglingSum += state.scores.get(hostId) || 0
    }
    const danglingContrib = this.damping * danglingSum / n

    // Add dangling contribution to all nodes
    for (const hostId of Array.from(newScores.keys())) {
      newScores.set(hostId, newScores.get(hostId)! + danglingContrib)
    }

    // Process edge contributions
    let hostsProcessed = BigInt(0)
    for (const [hostId, score] of Array.from(state.scores.entries())) {
      const outDegree = state.outDegree.get(hostId) || 0
      if (outDegree === 0) continue

      const outNeighbors = await getOutNeighbors(hostId)
      const contribution = this.damping * score / outDegree

      for (const neighbor of outNeighbors) {
        const current = newScores.get(neighbor) || teleportProb
        newScores.set(neighbor, current + contribution)
      }

      hostsProcessed++
      if (hostsProcessed % BigInt(100000) === BigInt(0)) {
        onProgress?.({
          phase: 'pagerank',
          hostsProcessed,
          totalHosts: BigInt(n),
          elapsedMs: Date.now(),
        })
      }
    }

    // Compute delta (L1 norm of difference)
    let delta = 0
    for (const [hostId, newScore] of Array.from(newScores.entries())) {
      const oldScore = state.scores.get(hostId) || 0
      delta += Math.abs(newScore - oldScore)
    }

    return { newScores, delta }
  }

  /**
   * Run full PageRank computation
   */
  async compute(
    state: PageRankState,
    getOutNeighbors: (hostId: HostId) => Promise<HostId[]>,
    onProgress?: MetricsProgressCallback
  ): Promise<Map<HostId, number>> {
    const startTime = Date.now()

    for (let i = 0; i < this.iterations; i++) {
      onProgress?.({
        phase: 'pagerank',
        iteration: i + 1,
        totalIterations: this.iterations,
        hostsProcessed: BigInt(0),
        totalHosts: BigInt(state.scores.size),
        elapsedMs: Date.now() - startTime,
      })

      const { newScores, delta } = await this.computeIteration(
        state,
        getOutNeighbors,
        onProgress
      )

      state.scores = newScores

      // Check convergence
      if (delta < this.convergenceThreshold) {
        console.log(`PageRank converged after ${i + 1} iterations (delta=${delta})`)
        break
      }
    }

    return state.scores
  }
}

/**
 * Approximate PageRank using Monte Carlo random walks
 *
 * More efficient for very large graphs where power iteration is too slow.
 */
export async function approximatePageRankMonteCarlo(
  getRandomOutNeighbor: (hostId: HostId) => Promise<HostId | null>,
  sampleHosts: HostId[],
  numWalks: number,
  walkLength: number,
  damping: number = 0.85,
  onProgress?: MetricsProgressCallback
): Promise<Map<HostId, number>> {
  const visitCounts = new Map<HostId, number>()
  const totalVisits = numWalks * walkLength
  const startTime = Date.now()

  for (let w = 0; w < numWalks; w++) {
    // Start from random sample host
    let currentHost = sampleHosts[Math.floor(Math.random() * sampleHosts.length)]

    for (let step = 0; step < walkLength; step++) {
      // Count visit
      visitCounts.set(currentHost, (visitCounts.get(currentHost) || 0) + 1)

      // Random jump with probability (1 - damping)
      if (Math.random() > damping) {
        currentHost = sampleHosts[Math.floor(Math.random() * sampleHosts.length)]
        continue
      }

      // Follow random outgoing edge
      const neighbor = await getRandomOutNeighbor(currentHost)
      if (neighbor === null) {
        // Dangling node - random jump
        currentHost = sampleHosts[Math.floor(Math.random() * sampleHosts.length)]
      } else {
        currentHost = neighbor
      }
    }

    if (w % 1000 === 0) {
      onProgress?.({
        phase: 'pagerank',
        iteration: w,
        totalIterations: numWalks,
        hostsProcessed: BigInt(w * walkLength),
        totalHosts: BigInt(totalVisits),
        elapsedMs: Date.now() - startTime,
      })
    }
  }

  // Normalize to probabilities
  const scores = new Map<HostId, number>()
  for (const [hostId, count] of Array.from(visitCounts.entries())) {
    scores.set(hostId, count / totalVisits)
  }

  return scores
}

// ============================================================================
// HITS Algorithm
// ============================================================================

/**
 * Compute Hub and Authority scores using HITS algorithm
 */
export class HITSComputer {
  private iterations: number
  private convergenceThreshold: number

  constructor(iterations: number = 20) {
    this.iterations = iterations
    this.convergenceThreshold = 1e-8
  }

  /**
   * Initialize HITS state
   */
  initializeState(hosts: Set<HostId>): HITSState {
    const hubScores = new Map<HostId, number>()
    const authorityScores = new Map<HostId, number>()
    const initialScore = 1 / Math.sqrt(hosts.size)

    for (const hostId of Array.from(hosts)) {
      hubScores.set(hostId, initialScore)
      authorityScores.set(hostId, initialScore)
    }

    return { hubScores, authorityScores }
  }

  /**
   * Run HITS iteration
   */
  async computeIteration(
    state: HITSState,
    getOutNeighbors: (hostId: HostId) => Promise<HostId[]>,
    getInNeighbors: (hostId: HostId) => Promise<HostId[]>
  ): Promise<{ newState: HITSState; delta: number }> {
    const newHubScores = new Map<HostId, number>()
    const newAuthorityScores = new Map<HostId, number>()

    // Update authority scores: a(p) = sum of h(q) for all q that link to p
    for (const hostId of Array.from(state.authorityScores.keys())) {
      const inNeighbors = await getInNeighbors(hostId)
      let authorityScore = 0
      for (const neighbor of inNeighbors) {
        authorityScore += state.hubScores.get(neighbor) || 0
      }
      newAuthorityScores.set(hostId, authorityScore)
    }

    // Update hub scores: h(p) = sum of a(q) for all q that p links to
    for (const hostId of Array.from(state.hubScores.keys())) {
      const outNeighbors = await getOutNeighbors(hostId)
      let hubScore = 0
      for (const neighbor of outNeighbors) {
        hubScore += newAuthorityScores.get(neighbor) || 0
      }
      newHubScores.set(hostId, hubScore)
    }

    // Normalize
    const hubNorm = Math.sqrt(Array.from(newHubScores.values()).reduce((a, b) => a + b * b, 0))
    const authNorm = Math.sqrt(Array.from(newAuthorityScores.values()).reduce((a, b) => a + b * b, 0))

    for (const [hostId, score] of Array.from(newHubScores.entries())) {
      newHubScores.set(hostId, hubNorm > 0 ? score / hubNorm : 0)
    }
    for (const [hostId, score] of Array.from(newAuthorityScores.entries())) {
      newAuthorityScores.set(hostId, authNorm > 0 ? score / authNorm : 0)
    }

    // Compute delta
    let delta = 0
    for (const [hostId, newScore] of Array.from(newHubScores.entries())) {
      delta += Math.abs(newScore - (state.hubScores.get(hostId) || 0))
    }
    for (const [hostId, newScore] of Array.from(newAuthorityScores.entries())) {
      delta += Math.abs(newScore - (state.authorityScores.get(hostId) || 0))
    }

    return {
      newState: { hubScores: newHubScores, authorityScores: newAuthorityScores },
      delta,
    }
  }

  /**
   * Run full HITS computation
   */
  async compute(
    hosts: Set<HostId>,
    getOutNeighbors: (hostId: HostId) => Promise<HostId[]>,
    getInNeighbors: (hostId: HostId) => Promise<HostId[]>,
    onProgress?: MetricsProgressCallback
  ): Promise<HITSState> {
    let state = this.initializeState(hosts)
    const startTime = Date.now()

    for (let i = 0; i < this.iterations; i++) {
      onProgress?.({
        phase: 'hits',
        iteration: i + 1,
        totalIterations: this.iterations,
        hostsProcessed: BigInt(hosts.size),
        totalHosts: BigInt(hosts.size),
        elapsedMs: Date.now() - startTime,
      })

      const { newState, delta } = await this.computeIteration(
        state,
        getOutNeighbors,
        getInNeighbors
      )

      state = newState

      if (delta < this.convergenceThreshold) {
        console.log(`HITS converged after ${i + 1} iterations`)
        break
      }
    }

    return state
  }
}

// ============================================================================
// Clustering Coefficient
// ============================================================================

/**
 * Compute local clustering coefficient for a node
 *
 * C(v) = 2 * |edges among neighbors| / (k * (k-1))
 * where k = degree of v
 */
export async function computeClusteringCoefficient(
  hostId: HostId,
  getNeighbors: (hostId: HostId) => Promise<HostId[]>,
  hasEdge: (from: HostId, to: HostId) => Promise<boolean>
): Promise<number> {
  const neighbors = await getNeighbors(hostId)
  const k = neighbors.length

  if (k < 2) return 0

  // Count edges among neighbors
  let edgeCount = 0
  for (let i = 0; i < neighbors.length; i++) {
    for (let j = i + 1; j < neighbors.length; j++) {
      if (await hasEdge(neighbors[i], neighbors[j])) {
        edgeCount++
      }
      if (await hasEdge(neighbors[j], neighbors[i])) {
        edgeCount++
      }
    }
  }

  // Directed graph: maximum possible edges is k * (k-1)
  return edgeCount / (k * (k - 1))
}

/**
 * Approximate global clustering coefficient using sampling
 */
export async function approximateGlobalClusteringCoefficient(
  sampleHosts: HostId[],
  getNeighbors: (hostId: HostId) => Promise<HostId[]>,
  hasEdge: (from: HostId, to: HostId) => Promise<boolean>,
  onProgress?: MetricsProgressCallback
): Promise<number> {
  let sum = 0
  let count = 0
  const startTime = Date.now()

  for (let i = 0; i < sampleHosts.length; i++) {
    const cc = await computeClusteringCoefficient(
      sampleHosts[i],
      getNeighbors,
      hasEdge
    )
    if (!isNaN(cc)) {
      sum += cc
      count++
    }

    if (i % 1000 === 0) {
      onProgress?.({
        phase: 'clustering',
        hostsProcessed: BigInt(i),
        totalHosts: BigInt(sampleHosts.length),
        elapsedMs: Date.now() - startTime,
      })
    }
  }

  return count > 0 ? sum / count : 0
}

// ============================================================================
// Metrics Aggregation
// ============================================================================

/**
 * Aggregate host metrics into HostMetrics records
 */
export function aggregateHostMetrics(
  hostId: HostId,
  tld: TLD,
  crawlId: CrawlId,
  degrees: {
    outDegree: bigint
    inDegree: bigint
    uniqueOutHosts: bigint
    uniqueInHosts: bigint
  },
  pagerank: number,
  harmonicCentrality: number,
  hubScore: number,
  authorityScore: number,
  clusteringCoefficient: number
): HostMetrics {
  const isHub = degrees.outDegree > degrees.inDegree * BigInt(10)
  const isAuthority = degrees.inDegree > degrees.outDegree * BigInt(10)

  return {
    host_id: hostId,
    tld,
    crawl_id: crawlId,
    out_degree: degrees.outDegree,
    in_degree: degrees.inDegree,
    unique_out_hosts: degrees.uniqueOutHosts,
    unique_in_hosts: degrees.uniqueInHosts,
    pagerank,
    harmonic_centrality: harmonicCentrality,
    hub_score: hubScore,
    authority_score: authorityScore,
    clustering_coefficient: clusteringCoefficient,
    is_hub: isHub,
    is_authority: isAuthority,
    computed_at: createTimestamp(BigInt(Date.now() * 1000)),
  }
}

/**
 * Compute TLD-level statistics
 */
export function computeTLDStats(
  tld: TLD,
  crawlId: CrawlId,
  metrics: HostMetrics[],
  links: Link[]
): TLDStats {
  const hostCount = BigInt(metrics.length)
  let totalOutLinks = BigInt(0)
  let totalInLinks = BigInt(0)
  let internalLinks = BigInt(0)
  let externalLinks = BigInt(0)

  for (const link of links) {
    totalOutLinks += BigInt(link.link_count)
    if (link.to_tld === tld) {
      internalLinks += BigInt(link.link_count)
    } else {
      externalLinks += BigInt(link.link_count)
    }
  }

  // Count inbound links
  for (const link of links) {
    if (link.to_tld === tld) {
      totalInLinks += BigInt(link.link_count)
    }
  }

  // Compute PageRank stats
  const pageranks = metrics.map(m => m.pagerank).sort((a, b) => a - b)
  const avgPagerank = pageranks.reduce((a, b) => a + b, 0) / pageranks.length

  // Compute in-degree stats
  const inDegrees = metrics.map(m => Number(m.in_degree)).sort((a, b) => a - b)
  const medianInDegree = inDegrees[Math.floor(inDegrees.length / 2)] || 0
  const p99InDegree = inDegrees[Math.floor(inDegrees.length * 0.99)] || 0

  // Compute top target TLDs
  const targetTldCounts = new Map<TLD, bigint>()
  for (const link of links) {
    if (link.from_tld === tld && link.to_tld !== tld) {
      const current = targetTldCounts.get(link.to_tld) || BigInt(0)
      targetTldCounts.set(link.to_tld, current + BigInt(link.link_count))
    }
  }

  const topTargetTlds = Array.from(targetTldCounts.entries())
    .sort((a, b) => Number(b[1] - a[1]))
    .slice(0, 10)
    .map(([t, count]) => ({ tld: t, count: Number(count) }))

  return {
    tld,
    crawl_id: crawlId,
    host_count: hostCount,
    total_out_links: totalOutLinks,
    total_in_links: totalInLinks,
    avg_pagerank: avgPagerank,
    median_in_degree: medianInDegree,
    p99_in_degree: BigInt(p99InDegree),
    internal_links: internalLinks,
    external_links: externalLinks,
    top_target_tlds: JSON.stringify(topTargetTlds),
    computed_at: createTimestamp(BigInt(Date.now() * 1000)),
  }
}

// ============================================================================
// Full Metrics Pipeline
// ============================================================================

/**
 * Run complete metrics computation pipeline
 */
export class MetricsPipeline {
  private bucket: StorageBucket
  private crawlId: CrawlId
  private config: HostGraphConfig

  constructor(
    bucket: StorageBucket,
    crawlId: CrawlId,
    config: Partial<HostGraphConfig> = {}
  ) {
    this.bucket = bucket
    this.crawlId = crawlId
    this.config = { ...DEFAULT_CONFIG, ...config }
  }

  /**
   * Run full metrics computation
   */
  async compute(
    tlds: TLD[],
    onProgress?: MetricsProgressCallback
  ): Promise<void> {
    const startTime = Date.now()

    // Phase 1: Compute degrees
    onProgress?.({
      phase: 'degree',
      hostsProcessed: BigInt(0),
      totalHosts: BigInt(0),
      elapsedMs: 0,
    })

    const { outDegree, inDegree, uniqueOutHosts, uniqueInHosts } =
      await computeDegrees(this.bucket, this.crawlId, tlds, onProgress)

    // Phase 2: Compute PageRank
    const pagerankComputer = new PageRankComputer(this.config)
    const pagerankState = pagerankComputer.initializeState(outDegree, inDegree)

    // Note: In production, getOutNeighbors would read from storage
    const pagerankScores = await pagerankComputer.compute(
      pagerankState,
      async (hostId) => {
        // Placeholder - would read from links files
        return []
      },
      onProgress
    )

    // Phase 3: Compute HITS
    const hitsComputer = new HITSComputer(this.config.pagerankIterations)
    const allHosts = new Set([...Array.from(outDegree.keys()), ...Array.from(inDegree.keys())])

    const hitsState = await hitsComputer.compute(
      allHosts,
      async (hostId) => [], // Would read from storage
      async (hostId) => [], // Would read from storage
      onProgress
    )

    // Phase 4: Aggregate and write metrics
    for (const tld of tlds) {
      onProgress?.({
        phase: 'aggregation',
        tld,
        hostsProcessed: BigInt(0),
        totalHosts: BigInt(allHosts.size),
        elapsedMs: Date.now() - startTime,
      })

      // Collect metrics for this TLD
      const tldMetrics: HostMetrics[] = []

      for (const hostId of Array.from(allHosts)) {
        const metrics = aggregateHostMetrics(
          hostId,
          tld,
          this.crawlId,
          {
            outDegree: outDegree.get(hostId) || BigInt(0),
            inDegree: inDegree.get(hostId) || BigInt(0),
            uniqueOutHosts: BigInt(uniqueOutHosts.get(hostId)?.size || 0),
            uniqueInHosts: BigInt(uniqueInHosts.get(hostId)?.size || 0),
          },
          pagerankScores.get(hostId) || 0,
          0, // Harmonic centrality - would need separate computation
          hitsState.hubScores.get(hostId) || 0,
          hitsState.authorityScores.get(hostId) || 0,
          0 // Clustering coefficient - expensive, compute on demand
        )

        tldMetrics.push(metrics)
      }

      // Write metrics
      const metricsPath = STORAGE_PATHS.metrics(this.crawlId, tld)
      await this.bucket.put(
        metricsPath,
        new TextEncoder().encode(JSON.stringify(tldMetrics))
      )
    }

    console.log(`Metrics computation complete in ${Date.now() - startTime}ms`)
  }
}
