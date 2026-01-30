/**
 * O*NET to UNSPSC Integration for ParqueDB
 *
 * This module demonstrates cross-dataset relationships between:
 * - O*NET occupational database (skills, tools, technology requirements)
 * - UNSPSC taxonomy (product and service classifications)
 *
 * The O*NET Tools and Technology file contains UNSPSC commodity codes,
 * enabling powerful queries that link occupations to product categories.
 *
 * Use Cases:
 * - Procurement: "What products do Software Developers need?"
 * - Workforce: "What occupations use IT equipment (UNSPSC segment 43)?"
 * - Career: "Find occupations with similar tool requirements"
 * - Market: "Which UNSPSC categories are in highest demand?"
 *
 * @see ./schema.ts for entity definitions
 * @see ../unspsc/schema.ts for UNSPSC taxonomy
 */

import type { Technology, OccupationTechnology, Occupation } from './schema'
import type { Commodity, Class, Family, Segment } from '../unspsc/schema'
import { parseCode } from '../unspsc/schema'

// =============================================================================
// Types
// =============================================================================

/**
 * Tool with its full UNSPSC hierarchy
 */
export interface ToolWithHierarchy {
  tool: Technology
  unspsc: {
    commodity?: Commodity
    class?: Class
    family?: Family
    segment?: Segment
  }
}

/**
 * Occupation with technology profile
 */
export interface OccupationTechProfile {
  occupation: Occupation
  technologies: Technology[]
  unspscCategories: {
    segment: Segment
    count: number
    percentage: number
  }[]
  hotTechnologies: Technology[]
}

/**
 * UNSPSC category with occupation demand
 */
export interface CategoryDemand {
  segment: Segment
  family?: Family
  class?: Class
  occupationCount: number
  topOccupations: Occupation[]
  topTools: Technology[]
}

/**
 * Similarity result for occupation comparison
 */
export interface OccupationSimilarity {
  occupation: Occupation
  sharedTools: Technology[]
  sharedCategories: string[] // UNSPSC segment codes
  similarityScore: number // 0-1
}

// =============================================================================
// Mock Database Interface
// =============================================================================

/**
 * Collection interface for ParqueDB queries
 */
interface Collection<T> {
  get(id: string, options?: { populate?: string[] }): Promise<T | null>
  find(filter: Record<string, unknown>, options?: FindOptions): Promise<T[]>
  count(filter?: Record<string, unknown>): Promise<number>
}

interface FindOptions {
  sort?: Record<string, 1 | -1>
  limit?: number
  skip?: number
  populate?: string[]
}

/**
 * Integrated database interface spanning O*NET and UNSPSC
 */
export interface IntegratedDatabase {
  // O*NET collections
  Occupations: Collection<Occupation>
  Technologies: Collection<Technology>
  OccupationTechnologies: Collection<OccupationTechnology>

  // UNSPSC collections
  Segments: Collection<Segment>
  Families: Collection<Family>
  Classes: Collection<Class>
  Commodities: Collection<Commodity>
}

// =============================================================================
// Query Functions
// =============================================================================

/**
 * Find O*NET tools/technologies by UNSPSC code
 *
 * Supports searching by:
 * - Full 8-digit commodity code (exact match)
 * - 6-digit class code (all commodities in class)
 * - 4-digit family code (all commodities in family)
 * - 2-digit segment code (all commodities in segment)
 *
 * @example
 * // Find all tools in UNSPSC segment 43 (IT equipment)
 * const tools = await findToolsByUnspsc(db, '43')
 *
 * // Find tools matching specific commodity
 * const excel = await findToolsByUnspsc(db, '43231513') // Spreadsheet software
 *
 * @param db - Integrated database instance
 * @param unspscCode - UNSPSC code (2, 4, 6, or 8 digits)
 * @param options - Query options (limit, includeHierarchy)
 * @returns Array of matching technologies with optional UNSPSC hierarchy
 */
export async function findToolsByUnspsc(
  db: IntegratedDatabase,
  unspscCode: string,
  options: {
    limit?: number
    includeHierarchy?: boolean
  } = {}
): Promise<ToolWithHierarchy[]> {
  const { limit = 100, includeHierarchy = false } = options
  const normalizedCode = unspscCode.replace(/[^0-9]/g, '')

  // Build filter based on code length
  let filter: Record<string, unknown>

  if (normalizedCode.length === 8) {
    // Exact commodity match
    filter = { unspscCode: normalizedCode }
  } else {
    // Prefix match (segment, family, or class)
    filter = { unspscCode: { $startsWith: normalizedCode } }
  }

  const technologies = await db.Technologies.find(filter, {
    limit,
    sort: { name: 1 },
  })

  if (!includeHierarchy) {
    return technologies.map(tool => ({ tool, unspsc: {} }))
  }

  // Fetch UNSPSC hierarchy for each tool
  const results: ToolWithHierarchy[] = []

  for (const tool of technologies) {
    if (!tool.commodityCode) {
      results.push({ tool, unspsc: {} })
      continue
    }

    const parsed = parseCode(tool.commodityCode)
    const unspsc: ToolWithHierarchy['unspsc'] = {}

    // Fetch each level of the hierarchy in parallel
    const [segment, family, cls, commodity] = await Promise.all([
      db.Segments.get(parsed.segment),
      parsed.family ? db.Families.get(parsed.family) : null,
      parsed.class ? db.Classes.get(parsed.class) : null,
      parsed.commodity ? db.Commodities.get(parsed.commodity) : null,
    ])

    if (segment) unspsc.segment = segment
    if (family) unspsc.family = family
    if (cls) unspsc.class = cls
    if (commodity) unspsc.commodity = commodity

    results.push({ tool, unspsc })
  }

  return results
}

/**
 * Find occupations that use a specific tool or technology
 *
 * Searches by tool name with fuzzy matching support.
 *
 * @example
 * // Find occupations using Microsoft Excel
 * const occupations = await findOccupationsByTool(db, 'Microsoft Excel')
 *
 * // Find occupations using any Python-related tool
 * const pythonOccupations = await findOccupationsByTool(db, 'Python', { fuzzy: true })
 *
 * @param db - Integrated database instance
 * @param toolName - Tool name or search term
 * @param options - Query options
 * @returns Array of occupations using the tool
 */
export async function findOccupationsByTool(
  db: IntegratedDatabase,
  toolName: string,
  options: {
    limit?: number
    fuzzy?: boolean
    hotTechnologyOnly?: boolean
  } = {}
): Promise<Array<{
  occupation: Occupation
  technology: Technology
  isHotTechnology: boolean
}>> {
  const { limit = 50, fuzzy = false, hotTechnologyOnly = false } = options

  // Find matching technologies
  let techFilter: Record<string, unknown>

  if (fuzzy) {
    techFilter = {
      $or: [
        { name: { $contains: toolName } },
        { example: { $contains: toolName } },
      ],
    }
  } else {
    techFilter = { name: toolName }
  }

  const technologies = await db.Technologies.find(techFilter, { limit: 100 })

  if (technologies.length === 0) {
    return []
  }

  // Find occupation-technology links
  const techIds = technologies.map(t => t.$id)
  let linkFilter: Record<string, unknown> = {
    'technology.$id': { $in: techIds },
  }

  if (hotTechnologyOnly) {
    linkFilter.isHotTechnology = true
  }

  const links = await db.OccupationTechnologies.find(linkFilter, {
    limit,
    populate: ['occupation', 'technology'],
    sort: { 'occupation.title': 1 },
  })

  // Map to result format
  const results: Array<{
    occupation: Occupation
    technology: Technology
    isHotTechnology: boolean
  }> = []

  for (const link of links) {
    const occupation = await db.Occupations.get((link.occupation as any).$id)
    const technology = await db.Technologies.get((link.technology as any).$id)

    if (occupation && technology) {
      results.push({
        occupation,
        technology,
        isHotTechnology: link.isHotTechnology ?? false,
      })
    }
  }

  return results
}

/**
 * Get complete UNSPSC hierarchy for a technology/tool
 *
 * Returns the full path from commodity up to segment.
 *
 * @example
 * const hierarchy = await getToolHierarchy(db, 'onet/technology/excel-123')
 * // Returns: { commodity: {...}, class: {...}, family: {...}, segment: {...} }
 *
 * @param db - Integrated database instance
 * @param toolId - Technology entity ID
 * @returns Full UNSPSC hierarchy or null if not found
 */
export async function getToolHierarchy(
  db: IntegratedDatabase,
  toolId: string
): Promise<ToolWithHierarchy | null> {
  const tool = await db.Technologies.get(toolId)

  if (!tool) {
    return null
  }

  if (!tool.unspscCode) {
    return { tool, unspsc: {} }
  }

  const parsed = parseCode(tool.unspscCode)
  const unspsc: ToolWithHierarchy['unspsc'] = {}

  // Fetch hierarchy in parallel
  const [segment, family, cls, commodity] = await Promise.all([
    db.Segments.get(parsed.segment),
    parsed.family ? db.Families.get(parsed.family) : null,
    parsed.class ? db.Classes.get(parsed.class) : null,
    parsed.commodity ? db.Commodities.get(parsed.commodity) : null,
  ])

  if (segment) unspsc.segment = segment
  if (family) unspsc.family = family
  if (cls) unspsc.class = cls
  if (commodity) unspsc.commodity = commodity

  return { tool, unspsc }
}

/**
 * Find occupations with similar tool/technology requirements
 *
 * Compares the UNSPSC categories of tools used by occupations to find
 * occupations with overlapping technology profiles.
 *
 * @example
 * // Find occupations similar to Software Developer (15-1252.00)
 * const similar = await findSimilarOccupationsByTools(db, 'onet/occupations/15-1252-00')
 *
 * @param db - Integrated database instance
 * @param occupationId - Source occupation entity ID
 * @param options - Query options
 * @returns Array of similar occupations with similarity scores
 */
export async function findSimilarOccupationsByTools(
  db: IntegratedDatabase,
  occupationId: string,
  options: {
    limit?: number
    minSimilarity?: number
  } = {}
): Promise<OccupationSimilarity[]> {
  const { limit = 20, minSimilarity = 0.3 } = options

  // Get source occupation's technologies
  const sourceLinks = await db.OccupationTechnologies.find(
    { 'occupation.$id': occupationId },
    { populate: ['technology'] }
  )

  if (sourceLinks.length === 0) {
    return []
  }

  // Extract tool IDs and UNSPSC segments
  const sourceToolIds = new Set<string>()
  const sourceSegments = new Set<string>()

  for (const link of sourceLinks) {
    const tech = await db.Technologies.get((link.technology as any).$id)
    if (tech) {
      sourceToolIds.add(tech.$id)
      if (tech.unspscCode) {
        sourceSegments.add(tech.unspscCode.slice(0, 2)) // Segment code
      }
    }
  }

  // Find other occupations using any of the same tools
  const otherLinks = await db.OccupationTechnologies.find(
    {
      'technology.$id': { $in: Array.from(sourceToolIds) },
      'occupation.$id': { $ne: occupationId },
    },
    { limit: 500 }
  )

  // Group by occupation and calculate similarity
  const occupationScores = new Map<string, {
    occupation: Occupation | null
    sharedTools: Set<string>
    sharedSegments: Set<string>
    totalTools: number
  }>()

  for (const link of otherLinks) {
    const occId = (link.occupation as any).$id
    const techId = (link.technology as any).$id

    if (!occupationScores.has(occId)) {
      const occupation = await db.Occupations.get(occId)
      occupationScores.set(occId, {
        occupation,
        sharedTools: new Set(),
        sharedSegments: new Set(),
        totalTools: 0,
      })
    }

    const score = occupationScores.get(occId)!

    // Check if this is a shared tool
    if (sourceToolIds.has(techId)) {
      score.sharedTools.add(techId)

      const tech = await db.Technologies.get(techId)
      if (tech?.unspscCode) {
        const segment = tech.unspscCode.slice(0, 2)
        if (sourceSegments.has(segment)) {
          score.sharedSegments.add(segment)
        }
      }
    }
    score.totalTools++
  }

  // Calculate similarity scores and filter
  const results: OccupationSimilarity[] = []

  for (const [, data] of Array.from(occupationScores.entries())) {
    if (!data.occupation) continue

    // Jaccard similarity based on shared tools
    const intersection = data.sharedTools.size
    const union = sourceToolIds.size + data.totalTools - intersection
    const similarityScore = union > 0 ? intersection / union : 0

    if (similarityScore >= minSimilarity) {
      // Resolve shared tools to full entities
      const sharedTools: Technology[] = []
      for (const toolId of Array.from(data.sharedTools)) {
        const tool = await db.Technologies.get(toolId)
        if (tool) sharedTools.push(tool)
      }

      results.push({
        occupation: data.occupation,
        sharedTools,
        sharedCategories: Array.from(data.sharedSegments),
        similarityScore,
      })
    }
  }

  // Sort by similarity and limit
  return results
    .sort((a, b) => b.similarityScore - a.similarityScore)
    .slice(0, limit)
}

/**
 * Get technology profile for an occupation
 *
 * Returns all technologies used by an occupation, grouped by UNSPSC category.
 *
 * @example
 * const profile = await getOccupationTechProfile(db, 'onet/occupations/15-1252-00')
 * // Shows Software Developer's tools organized by UNSPSC segment
 *
 * @param db - Integrated database instance
 * @param occupationId - Occupation entity ID
 * @returns Technology profile with UNSPSC categorization
 */
export async function getOccupationTechProfile(
  db: IntegratedDatabase,
  occupationId: string
): Promise<OccupationTechProfile | null> {
  const occupation = await db.Occupations.get(occupationId)
  if (!occupation) return null

  // Get all technology links
  const links = await db.OccupationTechnologies.find(
    { 'occupation.$id': occupationId },
    { limit: 500 }
  )

  const technologies: Technology[] = []
  const hotTechnologies: Technology[] = []
  const segmentCounts = new Map<string, { segment: Segment; count: number }>()

  for (const link of links) {
    const tech = await db.Technologies.get((link.technology as any).$id)
    if (!tech) continue

    technologies.push(tech)

    if (link.isHotTechnology || tech.hotTechnology) {
      hotTechnologies.push(tech)
    }

    // Count by UNSPSC segment
    if (tech.unspscCode) {
      const segmentCode = tech.unspscCode.slice(0, 2)

      if (!segmentCounts.has(segmentCode)) {
        const segment = await db.Segments.get(segmentCode)
        if (segment) {
          segmentCounts.set(segmentCode, { segment, count: 0 })
        }
      }

      const entry = segmentCounts.get(segmentCode)
      if (entry) {
        entry.count++
      }
    }
  }

  // Calculate percentages
  const total = technologies.length
  const unspscCategories = Array.from(segmentCounts.values())
    .map(({ segment, count }) => ({
      segment,
      count,
      percentage: total > 0 ? (count / total) * 100 : 0,
    }))
    .sort((a, b) => b.count - a.count)

  return {
    occupation,
    technologies,
    unspscCategories,
    hotTechnologies,
  }
}

/**
 * Find occupations by UNSPSC category
 *
 * Discovers which occupations use tools in a specific UNSPSC category.
 *
 * @example
 * // Find occupations using IT equipment (segment 43)
 * const demand = await findOccupationsByUnspscCategory(db, '43')
 *
 * @param db - Integrated database instance
 * @param unspscCode - UNSPSC code (2, 4, 6, or 8 digits)
 * @param options - Query options
 * @returns Category demand information with top occupations
 */
export async function findOccupationsByUnspscCategory(
  db: IntegratedDatabase,
  unspscCode: string,
  options: {
    limit?: number
  } = {}
): Promise<CategoryDemand | null> {
  const { limit = 20 } = options
  const normalizedCode = unspscCode.replace(/[^0-9]/g, '')

  // Get the UNSPSC entity
  const parsed = parseCode(normalizedCode)

  let segment: Segment | null = null
  let family: Family | null = null
  let cls: Class | null = null

  segment = await db.Segments.get(parsed.segment)
  if (!segment) return null

  if (parsed.family) {
    family = await db.Families.get(parsed.family)
  }
  if (parsed.class) {
    cls = await db.Classes.get(parsed.class)
  }

  // Find tools in this category
  const tools = await findToolsByUnspsc(db, normalizedCode, { limit: 500 })

  if (tools.length === 0) {
    return {
      segment,
      family: family ?? undefined,
      class: cls ?? undefined,
      occupationCount: 0,
      topOccupations: [],
      topTools: [],
    }
  }

  // Find occupations using these tools
  const toolIds = tools.map(t => t.tool.$id)
  const links = await db.OccupationTechnologies.find(
    { 'technology.$id': { $in: toolIds } },
    { limit: 1000 }
  )

  // Count occurrences by occupation
  const occupationCounts = new Map<string, number>()

  for (const link of links) {
    const occId = (link.occupation as any).$id
    occupationCounts.set(occId, (occupationCounts.get(occId) || 0) + 1)
  }

  // Get top occupations
  const topOccupationIds = Array.from(occupationCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([id]) => id)

  const topOccupations: Occupation[] = []
  for (const id of topOccupationIds) {
    const occ = await db.Occupations.get(id)
    if (occ) topOccupations.push(occ)
  }

  // Get top tools (most used)
  const toolUsage = new Map<string, number>()
  for (const link of links) {
    const techId = (link.technology as any).$id
    toolUsage.set(techId, (toolUsage.get(techId) || 0) + 1)
  }

  const topToolIds = Array.from(toolUsage.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([id]) => id)

  const topTools: Technology[] = []
  for (const id of topToolIds) {
    const tool = await db.Technologies.get(id)
    if (tool) topTools.push(tool)
  }

  return {
    segment,
    family: family ?? undefined,
    class: cls ?? undefined,
    occupationCount: occupationCounts.size,
    topOccupations,
    topTools,
  }
}

/**
 * Get UNSPSC categories used across all occupations
 *
 * Returns aggregate statistics about UNSPSC usage in O*NET.
 *
 * @param db - Integrated database instance
 * @returns Map of segment codes to usage statistics
 */
export async function getUnspscUsageStats(
  db: IntegratedDatabase
): Promise<Map<string, {
  segment: Segment
  toolCount: number
  occupationCount: number
  topTools: string[]
}>> {
  const stats = new Map<string, {
    segment: Segment
    toolCount: number
    occupationCount: Set<string>
    topTools: Map<string, number>
  }>()

  // Get all technologies with UNSPSC codes
  const technologies = await db.Technologies.find(
    { unspscCode: { $exists: true, $ne: null } },
    { limit: 10000 }
  )

  for (const tech of technologies) {
    if (!tech.unspscCode) continue

    const segmentCode = tech.unspscCode.slice(0, 2)

    if (!stats.has(segmentCode)) {
      const segment = await db.Segments.get(segmentCode)
      if (segment) {
        stats.set(segmentCode, {
          segment,
          toolCount: 0,
          occupationCount: new Set(),
          topTools: new Map(),
        })
      }
    }

    const entry = stats.get(segmentCode)
    if (entry) {
      entry.toolCount++
      entry.topTools.set(tech.name, (entry.topTools.get(tech.name) || 0) + 1)
    }
  }

  // Count occupations per segment
  for (const [segmentCode, entry] of Array.from(stats.entries())) {
    const tools = await findToolsByUnspsc(db, segmentCode, { limit: 1000 })
    const toolIds = tools.map(t => t.tool.$id)

    const links = await db.OccupationTechnologies.find(
      { 'technology.$id': { $in: toolIds } },
      { limit: 10000 }
    )

    for (const link of links) {
      entry.occupationCount.add((link.occupation as any).$id)
    }
  }

  // Convert to final format
  const result = new Map<string, {
    segment: Segment
    toolCount: number
    occupationCount: number
    topTools: string[]
  }>()

  for (const [code, entry] of Array.from(stats.entries())) {
    result.set(code, {
      segment: entry.segment,
      toolCount: entry.toolCount,
      occupationCount: entry.occupationCount.size,
      topTools: Array.from(entry.topTools.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([name]) => name),
    })
  }

  return result
}

// =============================================================================
// Example Queries - Demonstrating Cross-Dataset Power
// =============================================================================

/**
 * Example: What occupations use Microsoft Excel?
 *
 * This demonstrates tool-to-occupation lookup across the O*NET database.
 */
export async function exampleToolLookup(db: IntegratedDatabase): Promise<void> {
  console.log('\n' + '='.repeat(70))
  console.log('Example: What occupations use Microsoft Excel?')
  console.log('='.repeat(70) + '\n')

  const results = await findOccupationsByTool(db, 'Microsoft Excel', {
    limit: 10,
    fuzzy: true,
  })

  console.log(`Found ${results.length} occupations using Excel:\n`)

  for (const { occupation, isHotTechnology } of results) {
    const hot = isHotTechnology ? ' [HOT]' : ''
    console.log(`  - ${occupation.title} (${occupation.socCode})${hot}`)
  }

  console.log('\n' + '-'.repeat(70))
  console.log('Query: findOccupationsByTool(db, "Microsoft Excel", { fuzzy: true })')
  console.log('-'.repeat(70) + '\n')
}

/**
 * Example: What UNSPSC categories does a Software Developer need?
 *
 * Shows the occupation -> tools -> UNSPSC relationship.
 */
export async function exampleOccupationToUnspsc(db: IntegratedDatabase): Promise<void> {
  console.log('\n' + '='.repeat(70))
  console.log('Example: What UNSPSC categories does a Software Developer need?')
  console.log('='.repeat(70) + '\n')

  // Software Developers (15-1252.00)
  const occupationId = 'onet/occupations/15-1252-00'

  const profile = await getOccupationTechProfile(db, occupationId)

  if (!profile) {
    console.log('Occupation not found')
    return
  }

  console.log(`Occupation: ${profile.occupation.title}\n`)
  console.log(`Total technologies: ${profile.technologies.length}`)
  console.log(`Hot technologies: ${profile.hotTechnologies.length}\n`)

  console.log('UNSPSC Categories used:\n')

  for (const cat of profile.unspscCategories.slice(0, 10)) {
    console.log(`  ${cat.segment.code} - ${cat.segment.title}`)
    console.log(`      ${cat.count} tools (${cat.percentage.toFixed(1)}%)\n`)
  }

  console.log('Hot Technologies:\n')
  for (const tech of profile.hotTechnologies.slice(0, 5)) {
    console.log(`  - ${tech.name}`)
  }

  console.log('\n' + '-'.repeat(70))
  console.log('Query: getOccupationTechProfile(db, occupationId)')
  console.log('-'.repeat(70) + '\n')
}

/**
 * Example: Find occupations that use tools in UNSPSC category 43 (IT equipment)
 *
 * Demonstrates UNSPSC -> occupations reverse lookup.
 */
export async function exampleUnspscToOccupations(db: IntegratedDatabase): Promise<void> {
  console.log('\n' + '='.repeat(70))
  console.log('Example: Find occupations using IT equipment (UNSPSC segment 43)')
  console.log('='.repeat(70) + '\n')

  const demand = await findOccupationsByUnspscCategory(db, '43', { limit: 15 })

  if (!demand) {
    console.log('UNSPSC segment not found')
    return
  }

  console.log(`Segment: ${demand.segment.code} - ${demand.segment.title}\n`)
  console.log(`Occupations using IT equipment: ${demand.occupationCount}\n`)

  console.log('Top occupations:\n')
  for (const occ of demand.topOccupations) {
    console.log(`  - ${occ.title} (${occ.socCode})`)
  }

  console.log('\nMost common IT tools:\n')
  for (const tool of demand.topTools) {
    const unspsc = tool.unspscCode ? ` [${tool.unspscCode}]` : ''
    console.log(`  - ${tool.name}${unspsc}`)
  }

  console.log('\n' + '-'.repeat(70))
  console.log('Query: findOccupationsByUnspscCategory(db, "43")')
  console.log('-'.repeat(70) + '\n')
}

/**
 * Example: Find occupations similar to Software Developer by tool usage
 *
 * Demonstrates occupation similarity based on shared tools and UNSPSC categories.
 */
export async function exampleSimilarOccupations(db: IntegratedDatabase): Promise<void> {
  console.log('\n' + '='.repeat(70))
  console.log('Example: Find occupations similar to Software Developer')
  console.log('='.repeat(70) + '\n')

  const occupationId = 'onet/occupations/15-1252-00'

  const similar = await findSimilarOccupationsByTools(db, occupationId, {
    limit: 10,
    minSimilarity: 0.2,
  })

  console.log('Similar occupations by tool usage:\n')

  for (const result of similar) {
    console.log(`  ${result.occupation.title} (${result.occupation.socCode})`)
    console.log(`    Similarity: ${(result.similarityScore * 100).toFixed(1)}%`)
    console.log(`    Shared tools: ${result.sharedTools.slice(0, 3).map(t => t.name).join(', ')}`)
    console.log(`    Shared UNSPSC segments: ${result.sharedCategories.join(', ')}\n`)
  }

  console.log('-'.repeat(70))
  console.log('Query: findSimilarOccupationsByTools(db, occupationId)')
  console.log('-'.repeat(70) + '\n')
}

/**
 * Example: Get UNSPSC hierarchy for a specific tool
 *
 * Shows the full taxonomy path for a technology.
 */
export async function exampleToolHierarchy(db: IntegratedDatabase): Promise<void> {
  console.log('\n' + '='.repeat(70))
  console.log('Example: Get UNSPSC hierarchy for a tool')
  console.log('='.repeat(70) + '\n')

  // Find Python tool
  const tools = await db.Technologies.find(
    { name: { $contains: 'Python' } },
    { limit: 1 }
  )

  if (tools.length === 0) {
    console.log('Tool not found')
    return
  }

  const hierarchy = await getToolHierarchy(db, tools[0].$id)

  if (!hierarchy) {
    console.log('Hierarchy not found')
    return
  }

  console.log(`Tool: ${hierarchy.tool.name}`)
  console.log(`UNSPSC Code: ${hierarchy.tool.unspscCode || 'N/A'}\n`)

  if (hierarchy.unspsc.segment) {
    console.log('UNSPSC Hierarchy:\n')
    console.log(`  Segment:   ${hierarchy.unspsc.segment.code} - ${hierarchy.unspsc.segment.title}`)

    if (hierarchy.unspsc.family) {
      console.log(`  Family:    ${hierarchy.unspsc.family.code} - ${hierarchy.unspsc.family.title}`)
    }
    if (hierarchy.unspsc.class) {
      console.log(`  Class:     ${hierarchy.unspsc.class.code} - ${hierarchy.unspsc.class.title}`)
    }
    if (hierarchy.unspsc.commodity) {
      console.log(`  Commodity: ${hierarchy.unspsc.commodity.code} - ${hierarchy.unspsc.commodity.title}`)
    }
  } else {
    console.log('No UNSPSC classification available')
  }

  console.log('\n' + '-'.repeat(70))
  console.log('Query: getToolHierarchy(db, toolId)')
  console.log('-'.repeat(70) + '\n')
}

/**
 * Run all example queries
 */
export async function runAllExamples(db: IntegratedDatabase): Promise<void> {
  console.log('\n')
  console.log('#'.repeat(70))
  console.log('#  O*NET + UNSPSC Integration Examples')
  console.log('#  Demonstrating Cross-Dataset Queries in ParqueDB')
  console.log('#'.repeat(70))

  await exampleToolLookup(db)
  await exampleOccupationToUnspsc(db)
  await exampleUnspscToOccupations(db)
  await exampleSimilarOccupations(db)
  await exampleToolHierarchy(db)

  console.log('\n')
  console.log('#'.repeat(70))
  console.log('#  Integration Summary')
  console.log('#'.repeat(70))
  console.log('\n')
  console.log('The O*NET to UNSPSC integration enables powerful queries:')
  console.log('')
  console.log('  1. TOOL LOOKUP')
  console.log('     "What occupations use Microsoft Excel?"')
  console.log('     findOccupationsByTool(db, "Microsoft Excel")')
  console.log('')
  console.log('  2. OCCUPATION TECH PROFILE')
  console.log('     "What UNSPSC categories does a Software Developer need?"')
  console.log('     getOccupationTechProfile(db, occupationId)')
  console.log('')
  console.log('  3. CATEGORY-BASED DISCOVERY')
  console.log('     "Find occupations using IT equipment (UNSPSC 43)"')
  console.log('     findOccupationsByUnspscCategory(db, "43")')
  console.log('')
  console.log('  4. SIMILARITY MATCHING')
  console.log('     "Find occupations similar to Software Developer"')
  console.log('     findSimilarOccupationsByTools(db, occupationId)')
  console.log('')
  console.log('  5. TOOL HIERARCHY')
  console.log('     "Show UNSPSC classification for Python"')
  console.log('     getToolHierarchy(db, toolId)')
  console.log('')
  console.log('These queries demonstrate ParqueDB\'s ability to traverse')
  console.log('relationships across different datasets using a unified API.')
  console.log('\n')
}

// =============================================================================
// Mock Database for Demo
// =============================================================================

/**
 * Create a mock database for demonstration purposes
 *
 * In production, this would be replaced with actual ParqueDB initialization.
 */
export function createMockIntegratedDatabase(): IntegratedDatabase {
  // Sample data for demonstration
  const sampleOccupations: Occupation[] = [
    {
      $id: 'onet/occupations/15-1252-00',
      $type: 'Occupation',
      name: 'Software Developers',
      socCode: '15-1252.00',
      title: 'Software Developers',
      description: 'Research, design, and develop computer and network software or specialized utility programs.',
      jobZone: 4,
    },
    {
      $id: 'onet/occupations/15-1253-00',
      $type: 'Occupation',
      name: 'Software Quality Assurance Analysts and Testers',
      socCode: '15-1253.00',
      title: 'Software Quality Assurance Analysts and Testers',
      description: 'Develop and execute software tests to identify software problems and their causes.',
      jobZone: 4,
    },
    {
      $id: 'onet/occupations/15-1211-00',
      $type: 'Occupation',
      name: 'Computer Systems Analysts',
      socCode: '15-1211.00',
      title: 'Computer Systems Analysts',
      description: 'Analyze science, engineering, business, and other data processing problems.',
      jobZone: 4,
    },
    {
      $id: 'onet/occupations/13-2011-00',
      $type: 'Occupation',
      name: 'Accountants and Auditors',
      socCode: '13-2011.00',
      title: 'Accountants and Auditors',
      description: 'Examine, analyze, and interpret accounting records.',
      jobZone: 4,
    },
    {
      $id: 'onet/occupations/11-3031-00',
      $type: 'Occupation',
      name: 'Financial Managers',
      socCode: '11-3031.00',
      title: 'Financial Managers',
      description: 'Plan, direct, or coordinate accounting, investing, banking, insurance, securities, and other financial activities.',
      jobZone: 4,
    },
  ]

  const sampleTechnologies: Technology[] = [
    {
      $id: 'onet/technology/python',
      $type: 'Technology',
      name: 'Python',
      unspscCode: '43232110',
      example: 'Python programming language',
      category: 'Development environment software',
      hotTechnology: true,
    },
    {
      $id: 'onet/technology/excel',
      $type: 'Technology',
      name: 'Microsoft Excel',
      unspscCode: '43231513',
      example: 'Microsoft Excel spreadsheet software',
      category: 'Spreadsheet software',
      hotTechnology: true,
    },
    {
      $id: 'onet/technology/java',
      $type: 'Technology',
      name: 'Java',
      unspscCode: '43232110',
      example: 'Java programming language',
      category: 'Development environment software',
      hotTechnology: true,
    },
    {
      $id: 'onet/technology/sql',
      $type: 'Technology',
      name: 'SQL',
      unspscCode: '43232301',
      example: 'Structured Query Language',
      category: 'Database software',
      hotTechnology: true,
    },
    {
      $id: 'onet/technology/jira',
      $type: 'Technology',
      name: 'Atlassian Jira',
      unspscCode: '43232106',
      example: 'Project management software',
      category: 'Project management software',
      hotTechnology: false,
    },
  ]

  const sampleSegments: Segment[] = [
    {
      $type: 'Segment',
      code: '43',
      title: 'Information Technology Broadcasting and Telecommunications',
      isActive: true,
    },
    {
      $type: 'Segment',
      code: '84',
      title: 'Financial and Insurance Services',
      isActive: true,
    },
  ]

  const sampleFamilies: Family[] = [
    {
      $type: 'Family',
      code: '4323',
      title: 'Software',
      segmentCode: '43',
      isActive: true,
    },
  ]

  const sampleClasses: Class[] = [
    {
      $type: 'Class',
      code: '432315',
      title: 'Office suite software',
      familyCode: '4323',
      segmentCode: '43',
      isActive: true,
    },
    {
      $type: 'Class',
      code: '432321',
      title: 'Development environment software',
      familyCode: '4323',
      segmentCode: '43',
      isActive: true,
    },
  ]

  const sampleCommodities: Commodity[] = [
    {
      $type: 'Commodity',
      code: '43231513',
      title: 'Spreadsheet software',
      classCode: '432315',
      familyCode: '4323',
      segmentCode: '43',
      isActive: true,
    },
    {
      $type: 'Commodity',
      code: '43232110',
      title: 'Integrated development environment software',
      classCode: '432321',
      familyCode: '4323',
      segmentCode: '43',
      isActive: true,
    },
  ]

  // Helper to get nested property value (e.g., 'occupation.$id')
  function getNestedValue(obj: any, path: string): any {
    return path.split('.').reduce((o, p) => (o && o[p] !== undefined) ? o[p] : undefined, obj)
  }

  // Mock collection factory
  function createMockCollection<T extends { $id?: string; code?: string }>(
    data: T[],
    idField: keyof T = '$id' as keyof T
  ): Collection<T> {
    return {
      async get(id: string): Promise<T | null> {
        return data.find(item => {
          if (idField === '$id') return (item as any).$id === id
          return (item as any).code === id
        }) || null
      },
      async find(filter: Record<string, unknown>, options?: FindOptions): Promise<T[]> {
        let results = [...data]

        // Handle $or filter
        if ('$or' in filter) {
          const orConditions = filter.$or as Record<string, unknown>[]
          results = results.filter(item =>
            orConditions.some(condition => {
              for (const [key, value] of Object.entries(condition)) {
                const itemValue = getNestedValue(item, key)
                if (typeof value === 'object' && value !== null) {
                  const filterObj = value as Record<string, unknown>
                  if ('$contains' in filterObj) {
                    if (!String(itemValue || '').toLowerCase().includes(
                      String(filterObj.$contains).toLowerCase()
                    )) return false
                  }
                } else if (itemValue !== value) {
                  return false
                }
              }
              return true
            })
          )
          // Remove $or from filter so it's not processed again
          filter = { ...filter }
          delete (filter as any).$or
        }

        // Simple filter implementation with nested key support
        for (const [key, value] of Object.entries(filter)) {
          if (typeof value === 'object' && value !== null) {
            const filterObj = value as Record<string, unknown>

            if ('$startsWith' in filterObj) {
              results = results.filter(item =>
                String(getNestedValue(item, key) || '').startsWith(String(filterObj.$startsWith))
              )
            } else if ('$contains' in filterObj) {
              results = results.filter(item =>
                String(getNestedValue(item, key) || '').toLowerCase().includes(
                  String(filterObj.$contains).toLowerCase()
                )
              )
            } else if ('$in' in filterObj) {
              const inValues = filterObj.$in as string[]
              results = results.filter(item => inValues.includes(getNestedValue(item, key)))
            } else if ('$ne' in filterObj) {
              results = results.filter(item => getNestedValue(item, key) !== filterObj.$ne)
            } else if ('$exists' in filterObj) {
              const shouldExist = filterObj.$exists as boolean
              results = results.filter(item => {
                const val = getNestedValue(item, key)
                return shouldExist ? (val !== undefined && val !== null) : (val === undefined || val === null)
              })
            }
          } else {
            results = results.filter(item => getNestedValue(item, key) === value)
          }
        }

        // Apply limit
        if (options?.limit) {
          results = results.slice(0, options.limit)
        }

        return results
      },
      async count(filter?: Record<string, unknown>): Promise<number> {
        if (!filter) return data.length
        const results = await this.find(filter)
        return results.length
      },
    }
  }

  // Create mock occupation-technology links
  const sampleLinks: OccupationTechnology[] = [
    // Software Developer uses Python, Java, SQL, Jira, Excel
    { $id: 'link-1', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1252-00', technology: 'onet/technology/python', isHotTechnology: true },
    { $id: 'link-2', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1252-00', technology: 'onet/technology/java', isHotTechnology: true },
    { $id: 'link-3', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1252-00', technology: 'onet/technology/sql', isHotTechnology: true },
    { $id: 'link-4', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1252-00', technology: 'onet/technology/jira', isHotTechnology: false },
    { $id: 'link-5', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1252-00', technology: 'onet/technology/excel', isHotTechnology: false },
    // QA uses Python, SQL, Jira, Excel
    { $id: 'link-6', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1253-00', technology: 'onet/technology/python', isHotTechnology: true },
    { $id: 'link-7', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1253-00', technology: 'onet/technology/sql', isHotTechnology: true },
    { $id: 'link-8', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1253-00', technology: 'onet/technology/jira', isHotTechnology: false },
    { $id: 'link-9', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1253-00', technology: 'onet/technology/excel', isHotTechnology: false },
    // Systems Analyst uses SQL, Excel, Jira
    { $id: 'link-10', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1211-00', technology: 'onet/technology/sql', isHotTechnology: true },
    { $id: 'link-11', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1211-00', technology: 'onet/technology/excel', isHotTechnology: true },
    { $id: 'link-12', $type: 'OccupationTechnology', occupation: 'onet/occupations/15-1211-00', technology: 'onet/technology/jira', isHotTechnology: false },
    // Accountant uses Excel, SQL
    { $id: 'link-13', $type: 'OccupationTechnology', occupation: 'onet/occupations/13-2011-00', technology: 'onet/technology/excel', isHotTechnology: true },
    { $id: 'link-14', $type: 'OccupationTechnology', occupation: 'onet/occupations/13-2011-00', technology: 'onet/technology/sql', isHotTechnology: false },
    // Financial Manager uses Excel
    { $id: 'link-15', $type: 'OccupationTechnology', occupation: 'onet/occupations/11-3031-00', technology: 'onet/technology/excel', isHotTechnology: true },
  ]

  return {
    Occupations: createMockCollection(sampleOccupations),
    Technologies: createMockCollection(sampleTechnologies),
    OccupationTechnologies: createMockCollection(sampleLinks),
    Segments: createMockCollection(sampleSegments, 'code' as any),
    Families: createMockCollection(sampleFamilies, 'code' as any),
    Classes: createMockCollection(sampleClasses, 'code' as any),
    Commodities: createMockCollection(sampleCommodities, 'code' as any),
  }
}

// =============================================================================
// CLI Entry Point
// =============================================================================

if (process.argv[1]?.endsWith('unspsc-integration.ts') || process.argv[1]?.endsWith('unspsc-integration.js')) {
  const db = createMockIntegratedDatabase()
  runAllExamples(db)
    .then(() => {
      console.log('Examples completed!')
      process.exit(0)
    })
    .catch((error) => {
      console.error('Error:', error)
      process.exit(1)
    })
}
