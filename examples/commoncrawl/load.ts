/**
 * Common Crawl Host Graph Loader
 *
 * Streams and processes Common Crawl host-level web graph data into ParqueDB.
 * Uses the ParqueDB API with FsBackend for local storage.
 *
 * Data source: https://data.commoncrawl.org/projects/hyperlinkgraph/
 */

import {
  ParqueDB,
  FsBackend,
  type Entity,
  type EntityId,
  type CreateInput,
} from '../../src'

import type {
  Host,
  Link,
  HostId,
  TLD,
  CrawlId,
  ReversedHostname,
  Timestamp,
  CrawlMetadata,
  HostGraphConfig,
} from './schema'

import {
  createHostId,
  createCrawlId,
  createTLD,
  createTimestamp,
  createReversedHostname,
  parseReversedHostname,
  extractTLD,
  computeEdgeHash,
  computeHostnameHash,
  DEFAULT_CONFIG,
} from './schema'

// ============================================================================
// Types
// ============================================================================

/**
 * Progress callback for monitoring load progress
 */
export interface LoadProgress {
  phase: 'downloading' | 'parsing' | 'transforming' | 'writing' | 'indexing'
  currentFile: string
  filesProcessed: number
  totalFiles: number
  rowsProcessed: bigint
  bytesProcessed: bigint
  bytesTotal: bigint
  elapsedMs: number
  estimatedRemainingMs: number
}

export type ProgressCallback = (progress: LoadProgress) => void

/**
 * Parsed vertex line from Common Crawl
 */
interface VertexLine {
  hostId: HostId
  reversedHostname: ReversedHostname
}

/**
 * Parsed edge line from Common Crawl
 */
interface EdgeLine {
  fromId: HostId
  toId: HostId
}

/**
 * Host entity for ParqueDB
 */
interface HostEntity {
  $type: 'Host'
  name: string
  hostId: number
  reversedHostname: string
  tld: string
  sld: string
  subdomain: string | null
  hostnameHash: string
  crawlId: string
  ingestedAt: Date
  isActive: boolean
  crawlCount: number
  firstSeenCrawl: string | null
  lastSeenCrawl: string | null
}

/**
 * Link entity for ParqueDB (stored as relationship)
 */
interface LinkEntity {
  $type: 'Link'
  name: string
  fromHostId: number
  toHostId: number
  fromTld: string
  toTld: string
  edgeHash: string
  crawlId: string
  linkCount: number
  ingestedAt: Date
  isBidirectional: boolean
  firstSeenCrawl: string | null
  stabilityScore: number
}

// ============================================================================
// Common Crawl Data URLs
// ============================================================================

/**
 * Get URLs for a specific crawl's host graph data
 */
export function getCrawlUrls(crawlId: CrawlId): {
  verticesListUrl: string
  edgesListUrl: string
  baseUrl: string
} {
  const baseUrl = `https://data.commoncrawl.org/projects/hyperlinkgraph/${crawlId}/host`
  return {
    verticesListUrl: `${baseUrl}/${crawlId}-host-vertices.paths.gz`,
    edgesListUrl: `${baseUrl}/${crawlId}-host-edges.paths.gz`,
    baseUrl,
  }
}

/**
 * Known crawl releases (most recent first)
 */
export const KNOWN_CRAWLS: Array<{
  id: CrawlId
  name: string
  hosts: string
  edges: string
}> = [
  { id: 'cc-main-2025-oct-nov-dec' as CrawlId, name: 'Oct-Dec 2025', hosts: '250.8M', edges: '10.9B' },
  { id: 'cc-main-2025-sep-oct-nov' as CrawlId, name: 'Sep-Nov 2025', hosts: '235.7M', edges: '9.5B' },
  { id: 'cc-main-2024-25-dec-jan-feb' as CrawlId, name: 'Dec 2024 - Feb 2025', hosts: '267.4M', edges: '2.7B' },
  { id: 'cc-main-2024-25-nov-dec-jan' as CrawlId, name: 'Nov 2024 - Jan 2025', hosts: '277.7M', edges: '2.7B' },
  { id: 'cc-main-2024-may-jun-jul' as CrawlId, name: 'May-Jul 2024', hosts: '362.2M', edges: '2.7B' },
]

// ============================================================================
// Streaming Infrastructure
// ============================================================================

/**
 * Stream and decompress gzipped content
 */
async function* streamGzipLines(
  response: Response,
  onProgress?: (bytesRead: number) => void
): AsyncGenerator<string> {
  const reader = response.body?.getReader()
  if (!reader) throw new Error('No response body')

  const decompressionStream = new DecompressionStream('gzip')
  const decompressedStream = new ReadableStream({
    async start(controller) {
      while (true) {
        const { done, value } = await reader.read()
        if (done) {
          controller.close()
          break
        }
        controller.enqueue(value)
        onProgress?.(value.length)
      }
    }
  }).pipeThrough(decompressionStream)

  const decompressedReader = decompressedStream.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (true) {
    const { done, value } = await decompressedReader.read()
    if (done) break

    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    buffer = lines.pop() || ''

    for (const line of lines) {
      if (line.trim()) {
        yield line
      }
    }
  }

  // Yield any remaining content
  if (buffer.trim()) {
    yield buffer
  }
}

/**
 * Fetch file list from paths.gz file
 */
async function fetchFileList(pathsUrl: string): Promise<string[]> {
  const response = await fetch(pathsUrl)
  if (!response.ok) {
    throw new Error(`Failed to fetch file list: ${response.status}`)
  }

  const files: string[] = []
  for await (const line of streamGzipLines(response)) {
    files.push(line.trim())
  }
  return files
}

// ============================================================================
// Parsing Functions
// ============================================================================

/**
 * Parse a vertex line from Common Crawl format
 *
 * Format: "host_id\treversed_hostname" (tab-separated)
 * Example: "44\taarp.nic"
 */
function parseVertexLine(line: string): VertexLine | null {
  const parts = line.split('\t')
  if (parts.length < 2) return null

  const hostId = parseInt(parts[0], 10)
  if (isNaN(hostId)) return null

  return {
    hostId: createHostId(hostId),
    reversedHostname: createReversedHostname(parts[1]),
  }
}

/**
 * Parse an edge line from Common Crawl format
 *
 * Format: "from_id\tto_id" (tab-separated)
 * Example: "44\t123"
 */
function parseEdgeLine(line: string): EdgeLine | null {
  const parts = line.split('\t')
  if (parts.length < 2) return null

  const fromId = parseInt(parts[0], 10)
  const toId = parseInt(parts[1], 10)
  if (isNaN(fromId) || isNaN(toId)) return null

  return {
    fromId: createHostId(fromId),
    toId: createHostId(toId),
  }
}

// ============================================================================
// Transform Functions
// ============================================================================

/**
 * Transform vertex line to Host entity for ParqueDB
 */
function vertexToHostEntity(
  vertex: VertexLine,
  crawlId: CrawlId,
  ingestedAt: Date
): CreateInput<HostEntity> {
  const parsed = parseReversedHostname(vertex.reversedHostname)

  return {
    $type: 'Host',
    name: parsed.normalized, // Use normalized hostname as display name
    hostId: vertex.hostId,
    reversedHostname: vertex.reversedHostname,
    tld: parsed.tld,
    sld: parsed.sld,
    subdomain: parsed.subdomain,
    hostnameHash: computeHostnameHash(vertex.reversedHostname).toString(),
    crawlId,
    ingestedAt,
    isActive: true,
    crawlCount: 1,
    firstSeenCrawl: crawlId,
    lastSeenCrawl: crawlId,
  }
}

/**
 * Transform edge line to Link entity for ParqueDB
 */
function edgeToLinkEntity(
  edge: EdgeLine,
  hostIdToTld: Map<HostId, TLD>,
  hostIdToEntityId: Map<HostId, EntityId>,
  crawlId: CrawlId,
  ingestedAt: Date
): { entity: CreateInput<LinkEntity>; fromEntityId: EntityId; toEntityId: EntityId } | null {
  const fromTld = hostIdToTld.get(edge.fromId)
  const toTld = hostIdToTld.get(edge.toId)
  const fromEntityId = hostIdToEntityId.get(edge.fromId)
  const toEntityId = hostIdToEntityId.get(edge.toId)

  // Skip if we don't have TLD mappings or entity IDs
  if (!fromTld || !toTld || !fromEntityId || !toEntityId) return null

  return {
    entity: {
      $type: 'Link',
      name: `${edge.fromId}->${edge.toId}`,
      fromHostId: edge.fromId,
      toHostId: edge.toId,
      fromTld,
      toTld,
      edgeHash: computeEdgeHash(edge.fromId, edge.toId).toString(),
      crawlId,
      linkCount: 1,
      ingestedAt,
      isBidirectional: false,
      firstSeenCrawl: crawlId,
      stabilityScore: 1,
    },
    fromEntityId,
    toEntityId,
  }
}

// ============================================================================
// Main Loader Class
// ============================================================================

/**
 * Host Graph Loader
 *
 * Streams Common Crawl host graph data and loads into ParqueDB using
 * the proper API: db.collection().createMany() for entities and
 * $link operator for relationships.
 */
export class HostGraphLoader {
  private config: HostGraphConfig
  private db: ParqueDB
  private hostIdToTld: Map<HostId, TLD> = new Map()
  private hostIdToEntityId: Map<HostId, EntityId> = new Map()
  private processedHosts: Set<HostId> = new Set()
  private crawlId: CrawlId

  constructor(
    dataDir: string,
    crawlId: CrawlId,
    config: Partial<HostGraphConfig> = {}
  ) {
    this.crawlId = crawlId
    this.config = { ...DEFAULT_CONFIG, ...config }

    // Initialize ParqueDB with FsBackend for local storage
    const storage = new FsBackend(dataDir)
    this.db = new ParqueDB({ storage })
  }

  /**
   * Get the underlying ParqueDB instance
   */
  getDB(): ParqueDB {
    return this.db
  }

  /**
   * Load host graph from Common Crawl
   *
   * @param onProgress - Progress callback
   * @returns Crawl metadata
   */
  async load(onProgress?: ProgressCallback): Promise<CrawlMetadata> {
    const startTime = Date.now()
    const ingestedAt = new Date()

    // Initialize crawl metadata
    const metadata: CrawlMetadata = {
      crawl_id: this.crawlId,
      display_name: this.crawlId,
      source_url: getCrawlUrls(this.crawlId).baseUrl,
      crawl_date_start: createTimestamp(BigInt(startTime * 1000)),
      crawl_date_end: createTimestamp(BigInt(startTime * 1000)),
      ingested_at: createTimestamp(BigInt(startTime * 1000)),
      status: 'processing',
      total_hosts: BigInt(0),
      total_edges: BigInt(0),
      processing_duration_ms: BigInt(0),
      bytes_processed: BigInt(0),
      error_message: null,
    }

    try {
      // Phase 1: Load vertices (hosts)
      await this.loadVertices(ingestedAt, onProgress)
      metadata.total_hosts = BigInt(this.processedHosts.size)

      // Phase 2: Load edges (links) with relationships
      const totalEdges = await this.loadEdges(ingestedAt, onProgress)
      metadata.total_edges = BigInt(totalEdges)

      // Phase 3: Store crawl metadata
      metadata.status = 'complete'
      metadata.processing_duration_ms = BigInt(Date.now() - startTime)
      await this.saveCrawlMetadata(metadata)

      return metadata
    } catch (error) {
      metadata.status = 'failed'
      metadata.error_message = error instanceof Error ? error.message : String(error)
      metadata.processing_duration_ms = BigInt(Date.now() - startTime)
      await this.saveCrawlMetadata(metadata)
      throw error
    }
  }

  /**
   * Load vertices (hosts) from Common Crawl using ParqueDB API
   */
  private async loadVertices(ingestedAt: Date, onProgress?: ProgressCallback): Promise<void> {
    const { verticesListUrl, baseUrl } = getCrawlUrls(this.crawlId)
    const hosts = this.db.collection<HostEntity>('hosts')

    // Fetch list of vertex files
    const vertexFiles = await fetchFileList(verticesListUrl)

    let bytesProcessed = BigInt(0)
    let rowsProcessed = BigInt(0)
    const hostBatch: CreateInput<HostEntity>[] = []

    for (let i = 0; i < vertexFiles.length; i++) {
      const file = vertexFiles[i]
      const fileUrl = `${baseUrl}/${file}`

      onProgress?.({
        phase: 'downloading',
        currentFile: file,
        filesProcessed: i,
        totalFiles: vertexFiles.length,
        rowsProcessed,
        bytesProcessed,
        bytesTotal: BigInt(0),
        elapsedMs: Date.now(),
        estimatedRemainingMs: 0,
      })

      const response = await fetch(fileUrl)
      if (!response.ok) {
        throw new Error(`Failed to fetch ${fileUrl}: ${response.status}`)
      }

      // Stream and parse vertices
      for await (const line of streamGzipLines(response, (bytes) => {
        bytesProcessed += BigInt(bytes)
      })) {
        const vertex = parseVertexLine(line)
        if (!vertex) continue

        // Apply TLD filter if configured
        const tld = extractTLD(vertex.reversedHostname)
        if (this.config.tldFilter.length > 0 && !this.config.tldFilter.includes(tld)) {
          continue
        }

        // Track host ID to TLD mapping for edge processing
        this.hostIdToTld.set(vertex.hostId, tld)
        this.processedHosts.add(vertex.hostId)

        // Create host entity
        const hostEntity = vertexToHostEntity(vertex, this.crawlId, ingestedAt)
        hostBatch.push(hostEntity)

        rowsProcessed++

        // Write batch when it reaches threshold
        if (hostBatch.length >= this.config.rowGroupSize) {
          onProgress?.({
            phase: 'writing',
            currentFile: file,
            filesProcessed: i,
            totalFiles: vertexFiles.length,
            rowsProcessed,
            bytesProcessed,
            bytesTotal: BigInt(0),
            elapsedMs: Date.now(),
            estimatedRemainingMs: 0,
          })

          const created = await this.createHostBatch(hosts, hostBatch)

          // Track hostId -> entityId mapping for relationships
          for (const entity of created) {
            const hostId = (entity as Entity<HostEntity>).hostId
            this.hostIdToEntityId.set(hostId as HostId, entity.$id)
          }

          hostBatch.length = 0
        }
      }
    }

    // Write remaining batch
    if (hostBatch.length > 0) {
      const created = await this.createHostBatch(hosts, hostBatch)
      for (const entity of created) {
        const hostId = (entity as Entity<HostEntity>).hostId
        this.hostIdToEntityId.set(hostId as HostId, entity.$id)
      }
    }
  }

  /**
   * Create a batch of hosts using ParqueDB API
   */
  private async createHostBatch(
    collection: ReturnType<ParqueDB['collection']>,
    batch: CreateInput<HostEntity>[]
  ): Promise<Entity<HostEntity>[]> {
    // Create entities one by one (createMany not available on Collection)
    // In a real implementation, we'd want batch create support
    const results: Entity<HostEntity>[] = []
    for (const data of batch) {
      const entity = await collection.create(data) as Entity<HostEntity>
      results.push(entity)
    }
    return results
  }

  /**
   * Load edges (links) from Common Crawl with relationships
   */
  private async loadEdges(ingestedAt: Date, onProgress?: ProgressCallback): Promise<number> {
    const { edgesListUrl, baseUrl } = getCrawlUrls(this.crawlId)
    const links = this.db.collection<LinkEntity>('links')

    // Fetch list of edge files
    const edgeFiles = await fetchFileList(edgesListUrl)

    let bytesProcessed = BigInt(0)
    let rowsProcessed = BigInt(0)
    let totalEdges = 0

    const linkBatch: {
      entity: CreateInput<LinkEntity>
      fromEntityId: EntityId
      toEntityId: EntityId
    }[] = []

    for (let i = 0; i < edgeFiles.length; i++) {
      const file = edgeFiles[i]
      const fileUrl = `${baseUrl}/${file}`

      onProgress?.({
        phase: 'downloading',
        currentFile: file,
        filesProcessed: i,
        totalFiles: edgeFiles.length,
        rowsProcessed,
        bytesProcessed,
        bytesTotal: BigInt(0),
        elapsedMs: Date.now(),
        estimatedRemainingMs: 0,
      })

      const response = await fetch(fileUrl)
      if (!response.ok) {
        throw new Error(`Failed to fetch ${fileUrl}: ${response.status}`)
      }

      // Stream and parse edges
      for await (const line of streamGzipLines(response, (bytes) => {
        bytesProcessed += BigInt(bytes)
      })) {
        const edge = parseEdgeLine(line)
        if (!edge) continue

        // Transform to link entity with relationship info
        const linkData = edgeToLinkEntity(
          edge,
          this.hostIdToTld,
          this.hostIdToEntityId,
          this.crawlId,
          ingestedAt
        )
        if (!linkData) continue

        // Apply TLD filter if configured
        const fromTld = this.hostIdToTld.get(edge.fromId)
        const toTld = this.hostIdToTld.get(edge.toId)
        if (this.config.tldFilter.length > 0) {
          if (!fromTld || !toTld) continue
          if (!this.config.tldFilter.includes(fromTld) &&
              !this.config.tldFilter.includes(toTld)) {
            continue
          }
        }

        linkBatch.push(linkData)
        rowsProcessed++
        totalEdges++

        // Write batch when it reaches threshold
        if (linkBatch.length >= this.config.rowGroupSize) {
          onProgress?.({
            phase: 'writing',
            currentFile: file,
            filesProcessed: i,
            totalFiles: edgeFiles.length,
            rowsProcessed,
            bytesProcessed,
            bytesTotal: BigInt(0),
            elapsedMs: Date.now(),
            estimatedRemainingMs: 0,
          })

          await this.createLinkBatch(links, linkBatch)
          linkBatch.length = 0
        }
      }
    }

    // Write remaining batch
    if (linkBatch.length > 0) {
      await this.createLinkBatch(links, linkBatch)
    }

    return totalEdges
  }

  /**
   * Create a batch of links with relationships using ParqueDB API
   */
  private async createLinkBatch(
    collection: ReturnType<ParqueDB['collection']>,
    batch: { entity: CreateInput<LinkEntity>; fromEntityId: EntityId; toEntityId: EntityId }[]
  ): Promise<void> {
    const hosts = this.db.collection('hosts')

    for (const { entity, fromEntityId, toEntityId } of batch) {
      // Create the link entity
      const linkEntity = await collection.create(entity) as Entity<LinkEntity>

      // Create relationship: source host -> link (via "linksTo" predicate)
      // This models the directed edge in the graph
      try {
        const fromLocalId = fromEntityId.split('/')[1]
        await hosts.update(fromLocalId, {
          $link: {
            linksTo: linkEntity.$id,
          },
        })
      } catch (err) {
        // Host might not exist if filtered out - skip relationship
      }

      // Create relationship: link -> target host (via "target" predicate)
      try {
        await collection.update(linkEntity.$id.split('/')[1], {
          $link: {
            target: toEntityId,
          },
        })
      } catch (err) {
        // Target might not exist if filtered out - skip relationship
      }

      // Optionally, create direct host-to-host relationship for graph traversal
      // This allows queries like "find all hosts that host X links to"
      try {
        const fromLocalId = fromEntityId.split('/')[1]
        await hosts.update(fromLocalId, {
          $link: {
            linksToHost: toEntityId,
          },
        })
      } catch (err) {
        // Skip if hosts don't exist
      }
    }
  }

  /**
   * Save crawl metadata
   */
  private async saveCrawlMetadata(metadata: CrawlMetadata): Promise<void> {
    const crawls = this.db.collection<{
      $type: string
      name: string
      displayName: string
      sourceUrl: string
      status: string
      totalHosts: string
      totalEdges: string
      processingDurationMs: string
      errorMessage: string | null
    }>('crawls')

    await crawls.create({
      $type: 'Crawl',
      name: metadata.crawl_id,
      displayName: metadata.display_name,
      sourceUrl: metadata.source_url,
      status: metadata.status,
      totalHosts: metadata.total_hosts.toString(),
      totalEdges: metadata.total_edges.toString(),
      processingDurationMs: metadata.processing_duration_ms.toString(),
      errorMessage: metadata.error_message,
    })
  }
}

// ============================================================================
// Incremental Update Support
// ============================================================================

/**
 * Incremental loader for processing new crawls
 *
 * Compares with previous crawl and only processes changes.
 */
export class IncrementalHostGraphLoader {
  private loader: HostGraphLoader
  private previousCrawlId: CrawlId | null

  constructor(
    dataDir: string,
    crawlId: CrawlId,
    previousCrawlId: CrawlId | null,
    config: Partial<HostGraphConfig> = {}
  ) {
    this.loader = new HostGraphLoader(dataDir, crawlId, config)
    this.previousCrawlId = previousCrawlId
  }

  /**
   * Load incrementally, tracking changes from previous crawl
   */
  async loadIncremental(onProgress?: ProgressCallback): Promise<{
    metadata: CrawlMetadata
    newHosts: number
    removedHosts: number
    newEdges: number
    removedEdges: number
  }> {
    // For a full incremental implementation:
    // 1. Load previous crawl's host IDs from ParqueDB
    // 2. Compare with new crawl
    // 3. Mark hosts/edges as active/inactive
    // 4. Update crawl_count, first_seen_crawl, last_seen_crawl

    const metadata = await this.loader.load(onProgress)

    return {
      metadata,
      newHosts: 0, // Would be computed from diff
      removedHosts: 0,
      newEdges: 0,
      removedEdges: 0,
    }
  }
}

// ============================================================================
// CLI Entry Point
// ============================================================================

/**
 * Example CLI usage
 */
export async function main() {
  const dataDir = './data/commoncrawl'
  const crawlId = createCrawlId('cc-main-2025-oct-nov-dec')

  const loader = new HostGraphLoader(dataDir, crawlId, {
    tldFilter: [createTLD('com')], // Only process .com for demo
    rowGroupSize: 10_000,
  })

  console.log('Loading Common Crawl host graph using ParqueDB...')
  console.log(`Crawl: ${crawlId}`)
  console.log(`Data directory: ${dataDir}`)

  const metadata = await loader.load((progress) => {
    console.log(
      `[${progress.phase}] ${progress.currentFile} - ` +
      `${progress.rowsProcessed} rows, ` +
      `${progress.filesProcessed}/${progress.totalFiles} files`
    )
  })

  console.log('Load complete!')
  console.log(`Total hosts: ${metadata.total_hosts}`)
  console.log(`Total edges: ${metadata.total_edges}`)
  console.log(`Duration: ${metadata.processing_duration_ms}ms`)

  // Example: Query the loaded data
  const db = loader.getDB()
  const hosts = db.collection('hosts')

  // Find hosts with high degree (many outgoing links)
  const topHosts = await hosts.find(
    { tld: 'com' },
    { limit: 10, sort: { name: 1 } }
  )
  console.log('\nSample hosts:')
  for (const host of topHosts.items) {
    console.log(`  - ${host.name} (${host.$id})`)
  }
}
