#!/usr/bin/env node
/**
 * Wikidata Query Examples
 *
 * Demonstrates querying the loaded Wikidata in ParqueDB.
 * These examples show common patterns for working with the knowledge graph.
 *
 * Usage:
 *   npx tsx examples/wikidata/queries.ts <db-path>
 */

import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import {
  ENTITY_TYPES,
  COMMON_PROPERTIES,
  getEnglishLabel,
  getEnglishDescription,
} from './schema.js'

// =============================================================================
// Mock ParqueDB Client (for demonstration)
// =============================================================================

/**
 * Simplified ParqueDB client for demonstration.
 * In production, this would be replaced with actual ParqueDB imports.
 */
class WikidataDB {
  private basePath: string
  private items: Map<string, Record<string, unknown>> = new Map()
  private properties: Map<string, Record<string, unknown>> = new Map()
  private claims: Map<string, Record<string, unknown>[]> = new Map()

  constructor(basePath: string) {
    this.basePath = basePath
    this.loadData()
  }

  private loadData(): void {
    // Load items
    const itemsDir = join(this.basePath, 'data/items')
    if (existsSync(itemsDir)) {
      this.loadDir(itemsDir, this.items, 'id')
    }

    // Load properties
    const propsDir = join(this.basePath, 'data/properties')
    if (existsSync(propsDir)) {
      this.loadDir(propsDir, this.properties, 'id')
    }

    // Load claims (grouped by subject)
    const claimsDir = join(this.basePath, 'data/claims')
    if (existsSync(claimsDir)) {
      this.loadClaimsDir(claimsDir)
    }
  }

  private loadDir(dir: string, map: Map<string, Record<string, unknown>>, keyField: string): void {
    const files = readdirSync(dir, { recursive: true })
      .filter(f => f.toString().endsWith('.ndjson'))

    for (const file of files) {
      const filepath = join(dir, file.toString())
      const content = readFileSync(filepath, 'utf8')
      for (const line of content.split('\n')) {
        if (!line.trim()) continue
        try {
          const record = JSON.parse(line)
          map.set(record[keyField], record)
        } catch {
          // Skip parse errors
        }
      }
    }
  }

  private loadClaimsDir(dir: string): void {
    const files = readdirSync(dir, { recursive: true }) as string[]
    const ndjsonFiles = files.filter(f => f.toString().endsWith('.ndjson'))

    for (const file of ndjsonFiles) {
      const filepath = join(dir, file.toString())
      const content = readFileSync(filepath, 'utf8')
      for (const line of content.split('\n')) {
        if (!line.trim()) continue
        try {
          const record = JSON.parse(line)
          const subjectId = record.subject_id
          if (!this.claims.has(subjectId)) {
            this.claims.set(subjectId, [])
          }
          this.claims.get(subjectId)!.push(record)
        } catch {
          // Skip parse errors
        }
      }
    }
  }

  // =========================================================================
  // Query Methods
  // =========================================================================

  /**
   * Get item by Q-ID
   */
  getItem(qid: string): Record<string, unknown> | null {
    return this.items.get(qid) ?? null
  }

  /**
   * Get property by P-ID
   */
  getProperty(pid: string): Record<string, unknown> | null {
    return this.properties.get(pid) ?? null
  }

  /**
   * Get claims for an item
   */
  getClaims(qid: string): Record<string, unknown>[] {
    return this.claims.get(qid) ?? []
  }

  /**
   * Get claims by property for an item
   */
  getClaimsByProperty(qid: string, pid: string): Record<string, unknown>[] {
    return this.getClaims(qid).filter(c => c.property_id === pid)
  }

  /**
   * Find items by type
   */
  findByType(typeCategory: string): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = []
    for (const item of Array.from(this.items.values())) {
      if (item.item_type === typeCategory) {
        results.push(item)
      }
    }
    return results
  }

  /**
   * Find items by label (case-insensitive search)
   */
  findByLabel(query: string, limit = 10): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = []
    const lowerQuery = query.toLowerCase()

    for (const item of Array.from(this.items.values())) {
      const label = item.label_en as string
      if (label && label.toLowerCase().includes(lowerQuery)) {
        results.push(item)
        if (results.length >= limit) break
      }
    }

    return results
  }

  /**
   * Get related items via claims
   */
  getRelated(qid: string, pid: string): Record<string, unknown>[] {
    const claims = this.getClaimsByProperty(qid, pid)
    const results: Record<string, unknown>[] = []

    for (const claim of claims) {
      const objectId = claim.object_id as string
      if (objectId) {
        const related = this.getItem(objectId)
        if (related) {
          results.push(related)
        }
      }
    }

    return results
  }

  /**
   * Get reverse relationships (what links to this item)
   */
  getBacklinks(qid: string, pid?: string): { subject: Record<string, unknown>; claim: Record<string, unknown> }[] {
    const results: { subject: Record<string, unknown>; claim: Record<string, unknown> }[] = []

    for (const [subjectId, claims] of Array.from(this.claims.entries())) {
      for (const claim of claims) {
        if (claim.object_id === qid) {
          if (!pid || claim.property_id === pid) {
            const subject = this.getItem(subjectId)
            if (subject) {
              results.push({ subject, claim })
            }
          }
        }
      }
    }

    return results
  }

  /**
   * Count items by type
   */
  countByType(): Map<string, number> {
    const counts = new Map<string, number>()
    for (const item of Array.from(this.items.values())) {
      const type = item.item_type as string ?? 'unknown'
      counts.set(type, (counts.get(type) ?? 0) + 1)
    }
    return counts
  }

  /**
   * Get statistics
   */
  getStats(): { items: number; properties: number; claims: number } {
    let claimCount = 0
    for (const claims of Array.from(this.claims.values())) {
      claimCount += claims.length
    }
    return {
      items: this.items.size,
      properties: this.properties.size,
      claims: claimCount,
    }
  }
}

// =============================================================================
// Query Examples
// =============================================================================

function formatItem(item: Record<string, unknown>): string {
  return `${item.id}: ${item.label_en ?? item.name} (${item.item_type})`
}

function formatClaim(claim: Record<string, unknown>, db: WikidataDB): string {
  const property = db.getProperty(claim.property_id as string)
  const propertyLabel = property?.label_en ?? claim.property_id

  let valueStr: string
  const objectId = claim.object_id as string
  if (objectId) {
    const object = db.getItem(objectId)
    valueStr = object ? (object.label_en as string ?? objectId) : objectId
  } else {
    const value = JSON.parse(claim.value as string)
    if (value?.value?.time) {
      valueStr = value.value.time.replace('+', '')
    } else if (value?.value) {
      valueStr = JSON.stringify(value.value)
    } else {
      valueStr = 'unknown'
    }
  }

  return `  ${propertyLabel}: ${valueStr}`
}

async function runExamples(db: WikidataDB): Promise<void> {
  const stats = db.getStats()
  console.log('Database Statistics')
  console.log('='.repeat(60))
  console.log(`Items:      ${stats.items.toLocaleString()}`)
  console.log(`Properties: ${stats.properties.toLocaleString()}`)
  console.log(`Claims:     ${stats.claims.toLocaleString()}`)
  console.log('')

  // Example 1: Count by type
  console.log('Items by Type')
  console.log('-'.repeat(40))
  const typeCounts = db.countByType()
  for (const [type, count] of Array.from(typeCounts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 10)) {
    console.log(`  ${type.padEnd(20)} ${count.toLocaleString()}`)
  }
  console.log('')

  // Example 2: Find by label
  console.log('Search: "Einstein"')
  console.log('-'.repeat(40))
  const searchResults = db.findByLabel('Einstein', 5)
  for (const item of searchResults) {
    console.log(`  ${formatItem(item)}`)
  }
  console.log('')

  // Example 3: Get specific item (Douglas Adams - Q42)
  console.log('Item: Q42 (Douglas Adams)')
  console.log('-'.repeat(40))
  const douglas = db.getItem('Q42')
  if (douglas) {
    console.log(`  Label:       ${douglas.label_en}`)
    console.log(`  Description: ${douglas.description_en}`)
    console.log(`  Type:        ${douglas.item_type}`)
    console.log('')
    console.log('  Claims:')
    const claims = db.getClaims('Q42').slice(0, 10)
    for (const claim of claims) {
      console.log(formatClaim(claim, db))
    }
  } else {
    console.log('  (not found in dataset)')
  }
  console.log('')

  // Example 4: Get birthplace
  console.log('Query: Birthplace of Q42')
  console.log('-'.repeat(40))
  const birthplaceClaims = db.getClaimsByProperty('Q42', COMMON_PROPERTIES.placeOfBirth)
  if (birthplaceClaims.length > 0) {
    const birthplaces = db.getRelated('Q42', COMMON_PROPERTIES.placeOfBirth)
    for (const place of birthplaces) {
      console.log(`  ${formatItem(place)}`)
    }
  } else {
    console.log('  (not found in dataset)')
  }
  console.log('')

  // Example 5: Get notable works
  console.log('Query: Notable works of Q42')
  console.log('-'.repeat(40))
  const works = db.getRelated('Q42', COMMON_PROPERTIES.notableWork)
  if (works.length > 0) {
    for (const work of works) {
      console.log(`  ${formatItem(work)}`)
    }
  } else {
    console.log('  (not found in dataset)')
  }
  console.log('')

  // Example 6: Backlinks - What mentions Douglas Adams?
  console.log('Backlinks: Items referencing Q42')
  console.log('-'.repeat(40))
  const backlinks = db.getBacklinks('Q42').slice(0, 5)
  if (backlinks.length > 0) {
    for (const { subject, claim } of backlinks) {
      const prop = db.getProperty(claim.property_id as string)
      const propLabel = prop?.label_en ?? claim.property_id
      console.log(`  ${subject.label_en ?? subject.id} (via ${propLabel})`)
    }
  } else {
    console.log('  (no backlinks found)')
  }
  console.log('')

  // Example 7: Find humans (type Q5)
  console.log('Query: All humans (limited to 5)')
  console.log('-'.repeat(40))
  const humans = db.findByType('human').slice(0, 5)
  if (humans.length > 0) {
    for (const human of humans) {
      console.log(`  ${formatItem(human)}`)
    }
  } else {
    console.log('  (no humans in dataset)')
  }
  console.log('')

  // Example 8: Find cities
  console.log('Query: All locations (limited to 5)')
  console.log('-'.repeat(40))
  const locations = db.findByType('location').slice(0, 5)
  if (locations.length > 0) {
    for (const loc of locations) {
      console.log(`  ${formatItem(loc)}`)
    }
  } else {
    console.log('  (no locations in dataset)')
  }
  console.log('')

  // Example 9: Property lookup
  console.log('Property: P31 (instance of)')
  console.log('-'.repeat(40))
  const p31 = db.getProperty('P31')
  if (p31) {
    console.log(`  Label:       ${p31.label_en}`)
    console.log(`  Description: ${p31.description_en}`)
    console.log(`  Datatype:    ${p31.datatype}`)
  } else {
    console.log('  (not found in dataset)')
  }
  console.log('')
}

// =============================================================================
// Graph Traversal Examples
// =============================================================================

/**
 * Example: Find all entities within N hops of a starting entity
 */
function traverseGraph(
  db: WikidataDB,
  startQid: string,
  maxDepth: number,
  properties?: string[]
): Map<string, { depth: number; path: string[] }> {
  const visited = new Map<string, { depth: number; path: string[] }>()
  const queue: { qid: string; depth: number; path: string[] }[] = []

  queue.push({ qid: startQid, depth: 0, path: [startQid] })
  visited.set(startQid, { depth: 0, path: [startQid] })

  while (queue.length > 0) {
    const { qid, depth, path } = queue.shift()!

    if (depth >= maxDepth) continue

    const claims = db.getClaims(qid)
    for (const claim of claims) {
      // Filter by properties if specified
      if (properties && !properties.includes(claim.property_id as string)) {
        continue
      }

      const objectId = claim.object_id as string
      if (objectId && !visited.has(objectId)) {
        const newPath = [...path, objectId]
        visited.set(objectId, { depth: depth + 1, path: newPath })
        queue.push({ qid: objectId, depth: depth + 1, path: newPath })
      }
    }
  }

  return visited
}

/**
 * Example: Find shortest path between two entities
 */
function findShortestPath(
  db: WikidataDB,
  startQid: string,
  endQid: string,
  maxDepth: number
): string[] | null {
  const visited = new Map<string, string[]>()
  const queue: { qid: string; path: string[] }[] = []

  queue.push({ qid: startQid, path: [startQid] })
  visited.set(startQid, [startQid])

  while (queue.length > 0) {
    const { qid, path } = queue.shift()!

    if (qid === endQid) {
      return path
    }

    if (path.length > maxDepth) continue

    const claims = db.getClaims(qid)
    for (const claim of claims) {
      const objectId = claim.object_id as string
      if (objectId && !visited.has(objectId)) {
        const newPath = [...path, objectId]
        visited.set(objectId, newPath)

        if (objectId === endQid) {
          return newPath
        }

        queue.push({ qid: objectId, path: newPath })
      }
    }
  }

  return null
}

async function runGraphExamples(db: WikidataDB): Promise<void> {
  console.log('Graph Traversal Examples')
  console.log('='.repeat(60))
  console.log('')

  // Example: Traverse from Douglas Adams (Q42)
  console.log('Traverse: 2 hops from Q42 (Douglas Adams)')
  console.log('-'.repeat(40))
  const reachable = traverseGraph(db, 'Q42', 2)
  console.log(`  Found ${reachable.size} reachable entities`)
  console.log('')

  // Show some results
  const byDepth = new Map<number, number>()
  for (const { depth } of Array.from(reachable.values())) {
    byDepth.set(depth, (byDepth.get(depth) ?? 0) + 1)
  }
  for (const [depth, count] of Array.from(byDepth.entries())) {
    console.log(`  Depth ${depth}: ${count} entities`)
  }
  console.log('')

  // Example: Find path (would need two known entities in the dataset)
  console.log('Shortest path example:')
  console.log('-'.repeat(40))
  console.log('  (Requires two entities in the dataset to demonstrate)')
  console.log('')
}

// =============================================================================
// Aggregation Examples
// =============================================================================

async function runAggregationExamples(db: WikidataDB): Promise<void> {
  console.log('Aggregation Examples')
  console.log('='.repeat(60))
  console.log('')

  // Count claims by property
  const claimsByProperty = new Map<string, number>()
  for (const item of db.findByType('human').slice(0, 1000)) {
    const claims = db.getClaims(item.id as string)
    for (const claim of claims) {
      const pid = claim.property_id as string
      claimsByProperty.set(pid, (claimsByProperty.get(pid) ?? 0) + 1)
    }
  }

  console.log('Most common properties for humans:')
  console.log('-'.repeat(40))
  const sorted = Array.from(claimsByProperty.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)

  for (const [pid, count] of sorted) {
    const prop = db.getProperty(pid)
    const label = prop?.label_en ?? pid
    console.log(`  ${String(label).padEnd(30)} ${count}`)
  }
  console.log('')
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const dbPath = process.argv[2]

  if (!dbPath) {
    console.error('Usage: npx tsx queries.ts <db-path>')
    console.error('')
    console.error('Examples:')
    console.error('  npx tsx queries.ts ./wikidata-db')
    console.error('  npx tsx queries.ts ./humans-db')
    process.exit(1)
  }

  if (!existsSync(dbPath)) {
    console.error(`Error: Database not found: ${dbPath}`)
    process.exit(1)
  }

  console.log('Wikidata Query Examples')
  console.log('='.repeat(60))
  console.log(`Database: ${dbPath}`)
  console.log('='.repeat(60))
  console.log('')

  const db = new WikidataDB(dbPath)

  await runExamples(db)
  await runGraphExamples(db)
  await runAggregationExamples(db)

  console.log('Done!')
}

main().catch(console.error)

// Export for use as a library
export {
  WikidataDB,
  traverseGraph,
  findShortestPath,
}
