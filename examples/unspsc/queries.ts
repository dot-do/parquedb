/**
 * UNSPSC Query Examples for ParqueDB
 *
 * Demonstrates hierarchy traversal, search, and relationship queries
 * for the UNSPSC taxonomy.
 */

import { parseCode, getParentCode, formatCode, entityId } from './schema'
import type { Segment, Family, Class, Commodity, UNSPSCEntity } from './schema'

// =============================================================================
// Types
// =============================================================================

/**
 * Full hierarchical path from Segment to Commodity
 */
export interface HierarchyPath {
  segment: Segment
  family?: Family
  class?: Class
  commodity?: Commodity
}

/**
 * Search result with relevance score
 */
export interface SearchResult {
  entity: UNSPSCEntity
  score: number
  matchedField: 'title' | 'description' | 'code'
  highlight?: string
}

/**
 * Tree node for hierarchy visualization
 */
export interface TreeNode {
  code: string
  title: string
  type: 'Segment' | 'Family' | 'Class' | 'Commodity'
  children?: TreeNode[]
  childCount?: number
}

/**
 * Breadcrumb for navigation
 */
export interface Breadcrumb {
  code: string
  title: string
  type: 'Segment' | 'Family' | 'Class' | 'Commodity'
  path: string
}

// =============================================================================
// Mock Database Interface (ParqueDB API)
// =============================================================================

/**
 * Simulated ParqueDB collection interface
 */
interface Collection<T> {
  get(id: string): Promise<T | null>
  find(filter: Record<string, unknown>, options?: FindOptions): Promise<T[]>
  count(filter?: Record<string, unknown>): Promise<number>
}

interface FindOptions {
  sort?: Record<string, 1 | -1>
  limit?: number
  skip?: number
  project?: Record<string, 1 | 0>
}

/**
 * ParqueDB instance interface for UNSPSC
 */
export interface UNSPSCDatabase {
  Segments: Collection<Segment>
  Families: Collection<Family>
  Classes: Collection<Class>
  Commodities: Collection<Commodity>
}

// =============================================================================
// Hierarchy Traversal Queries
// =============================================================================

/**
 * Get complete hierarchy path for any UNSPSC code
 *
 * @example
 * const path = await getHierarchyPath(db, '43101501')
 * // Returns:
 * // {
 * //   segment: { code: '43', title: 'Information Technology...' },
 * //   family: { code: '4310', title: 'Computer Equipment...' },
 * //   class: { code: '431015', title: 'Computers' },
 * //   commodity: { code: '43101501', title: 'Notebook computers' }
 * // }
 */
export async function getHierarchyPath(
  db: UNSPSCDatabase,
  code: string
): Promise<HierarchyPath | null> {
  const parsed = parseCode(code)

  // Always fetch segment
  const segment = await db.Segments.get(parsed.segment)
  if (!segment) return null

  const result: HierarchyPath = { segment }

  // Fetch family if applicable
  if (parsed.family) {
    const family = await db.Families.get(parsed.family)
    if (family) result.family = family
  }

  // Fetch class if applicable
  if (parsed.class) {
    const cls = await db.Classes.get(parsed.class)
    if (cls) result.class = cls
  }

  // Fetch commodity if applicable
  if (parsed.commodity) {
    const commodity = await db.Commodities.get(parsed.commodity)
    if (commodity) result.commodity = commodity
  }

  return result
}

/**
 * Get all children of a given UNSPSC code
 *
 * @example
 * const children = await getChildren(db, '4310')  // Get all classes in this family
 */
export async function getChildren(
  db: UNSPSCDatabase,
  code: string
): Promise<UNSPSCEntity[]> {
  const parsed = parseCode(code)

  switch (parsed.level) {
    case 'Segment':
      return db.Families.find({ segmentCode: code }, { sort: { code: 1 } })

    case 'Family':
      return db.Classes.find({ familyCode: code }, { sort: { code: 1 } })

    case 'Class':
      return db.Commodities.find({ classCode: code }, { sort: { code: 1 } })

    case 'Commodity':
      return []  // Commodities have no children
  }
}

/**
 * Get parent entity of a given UNSPSC code
 *
 * @example
 * const parent = await getParent(db, '43101501')  // Returns the Class
 */
export async function getParent(
  db: UNSPSCDatabase,
  code: string
): Promise<UNSPSCEntity | null> {
  const parentCode = getParentCode(code)
  if (!parentCode) return null

  const parsed = parseCode(code)

  switch (parsed.level) {
    case 'Family':
      return db.Segments.get(parentCode)

    case 'Class':
      return db.Families.get(parentCode)

    case 'Commodity':
      return db.Classes.get(parentCode)

    default:
      return null
  }
}

/**
 * Get all ancestors (full path to root) for a UNSPSC code
 *
 * @example
 * const ancestors = await getAncestors(db, '43101501')
 * // Returns [Class, Family, Segment] from direct parent to root
 */
export async function getAncestors(
  db: UNSPSCDatabase,
  code: string
): Promise<UNSPSCEntity[]> {
  const ancestors: UNSPSCEntity[] = []
  let currentCode: string | null = code

  while (currentCode) {
    const parentCode = getParentCode(currentCode)
    if (!parentCode) break

    const parent = await getParent(db, currentCode)
    if (parent) {
      ancestors.push(parent)
    }

    currentCode = parentCode
  }

  return ancestors
}

/**
 * Get breadcrumb navigation for a UNSPSC code
 *
 * @example
 * const breadcrumbs = await getBreadcrumbs(db, '43101501')
 * // Returns navigation path from root to current item
 */
export async function getBreadcrumbs(
  db: UNSPSCDatabase,
  code: string
): Promise<Breadcrumb[]> {
  const path = await getHierarchyPath(db, code)
  if (!path) return []

  const breadcrumbs: Breadcrumb[] = []

  breadcrumbs.push({
    code: path.segment.code,
    title: path.segment.title,
    type: 'Segment',
    path: `/unspsc/${path.segment.code}`,
  })

  if (path.family) {
    breadcrumbs.push({
      code: path.family.code,
      title: path.family.title,
      type: 'Family',
      path: `/unspsc/${path.family.code}`,
    })
  }

  if (path.class) {
    breadcrumbs.push({
      code: path.class.code,
      title: path.class.title,
      type: 'Class',
      path: `/unspsc/${path.class.code}`,
    })
  }

  if (path.commodity) {
    breadcrumbs.push({
      code: path.commodity.code,
      title: path.commodity.title,
      type: 'Commodity',
      path: `/unspsc/${path.commodity.code}`,
    })
  }

  return breadcrumbs
}

/**
 * Get all descendants of a UNSPSC code (recursive)
 *
 * @example
 * const allItems = await getDescendants(db, '43', { maxDepth: 2 })
 */
export async function getDescendants(
  db: UNSPSCDatabase,
  code: string,
  options: { maxDepth?: number; includeInactive?: boolean } = {}
): Promise<UNSPSCEntity[]> {
  const { maxDepth = 4, includeInactive = false } = options
  const parsed = parseCode(code)
  const results: UNSPSCEntity[] = []

  const baseFilter = includeInactive ? {} : { isActive: true }

  // Determine what levels to fetch based on starting level and maxDepth
  const levels: Array<{
    collection: keyof UNSPSCDatabase
    filter: Record<string, unknown>
    depth: number
  }> = []

  switch (parsed.level) {
    case 'Segment':
      if (maxDepth >= 1) {
        levels.push({ collection: 'Families', filter: { segmentCode: code }, depth: 1 })
      }
      if (maxDepth >= 2) {
        levels.push({ collection: 'Classes', filter: { segmentCode: code }, depth: 2 })
      }
      if (maxDepth >= 3) {
        levels.push({ collection: 'Commodities', filter: { segmentCode: code }, depth: 3 })
      }
      break

    case 'Family':
      if (maxDepth >= 1) {
        levels.push({ collection: 'Classes', filter: { familyCode: code }, depth: 1 })
      }
      if (maxDepth >= 2) {
        levels.push({ collection: 'Commodities', filter: { familyCode: code }, depth: 2 })
      }
      break

    case 'Class':
      if (maxDepth >= 1) {
        levels.push({ collection: 'Commodities', filter: { classCode: code }, depth: 1 })
      }
      break
  }

  // Fetch all levels in parallel
  const fetchPromises = levels.map(async ({ collection, filter }) => {
    return db[collection].find({ ...filter, ...baseFilter }, { sort: { code: 1 } })
  })

  const levelResults = await Promise.all(fetchPromises)

  for (const entities of levelResults) {
    results.push(...(entities as unknown as UNSPSCEntity[]))
  }

  return results
}

// =============================================================================
// Search Queries
// =============================================================================

/**
 * Full-text search across all UNSPSC entities
 *
 * @example
 * const results = await searchUNSPSC(db, 'computer laptop')
 */
export async function searchUNSPSC(
  db: UNSPSCDatabase,
  query: string,
  options: {
    limit?: number
    types?: Array<'Segment' | 'Family' | 'Class' | 'Commodity'>
  } = {}
): Promise<SearchResult[]> {
  const { limit = 50, types = ['Segment', 'Family', 'Class', 'Commodity'] } = options

  const results: SearchResult[] = []
  const searchTerms = query.toLowerCase().split(/\s+/).filter(Boolean)

  // Helper to score a match
  const scoreMatch = (entity: UNSPSCEntity, field: 'title' | 'description' | 'code'): number => {
    const value = (entity[field] || '').toString().toLowerCase()

    // Exact match
    if (value === query.toLowerCase()) return 1.0

    // All terms present
    const allTermsPresent = searchTerms.every(term => value.includes(term))
    if (allTermsPresent) return 0.8

    // Starts with query
    if (value.startsWith(query.toLowerCase())) return 0.7

    // Contains query
    if (value.includes(query.toLowerCase())) return 0.5

    // Partial term matches
    const matchingTerms = searchTerms.filter(term => value.includes(term))
    if (matchingTerms.length > 0) {
      return 0.3 * (matchingTerms.length / searchTerms.length)
    }

    return 0
  }

  // Search each collection
  const searchCollection = async <T extends UNSPSCEntity>(
    collection: Collection<T>,
    type: 'Segment' | 'Family' | 'Class' | 'Commodity'
  ) => {
    if (!types.includes(type)) return

    // In production, use full-text search:
    // const entities = await collection.find({ $text: { $search: query } })

    // For demo, fetch all and filter (not efficient for production!)
    const entities = await collection.find({})

    for (const entity of entities) {
      // Score by title
      let titleScore = scoreMatch(entity, 'title')

      // Score by code
      let codeScore = scoreMatch(entity, 'code')

      // Score by description if present
      let descScore = entity.description ? scoreMatch(entity, 'description') : 0

      // Determine best match
      let matchedField: 'title' | 'description' | 'code'
      let score: number

      if (codeScore >= titleScore && codeScore >= descScore) {
        matchedField = 'code'
        score = codeScore
      } else if (titleScore >= descScore) {
        matchedField = 'title'
        score = titleScore
      } else {
        matchedField = 'description'
        score = descScore
      }

      if (score > 0) {
        // Boost commodities (more specific) slightly
        if (type === 'Commodity') score *= 1.1
        if (type === 'Class') score *= 1.05

        results.push({
          entity,
          score,
          matchedField,
        })
      }
    }
  }

  await Promise.all([
    searchCollection(db.Segments, 'Segment'),
    searchCollection(db.Families, 'Family'),
    searchCollection(db.Classes, 'Class'),
    searchCollection(db.Commodities, 'Commodity'),
  ])

  // Sort by score and limit
  return results
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
}

/**
 * Find UNSPSC codes by code prefix
 *
 * @example
 * const items = await findByCodePrefix(db, '4310')  // All items starting with 4310
 */
export async function findByCodePrefix(
  db: UNSPSCDatabase,
  prefix: string,
  options: { limit?: number } = {}
): Promise<UNSPSCEntity[]> {
  const { limit = 100 } = options
  const results: UNSPSCEntity[] = []

  // Create regex filter
  const codeFilter = { code: { $regex: `^${prefix}` } }

  // Fetch from each collection based on prefix length
  const prefixLen = prefix.length

  if (prefixLen <= 2) {
    const segments = await db.Segments.find(codeFilter, { limit, sort: { code: 1 } })
    results.push(...segments)
  }

  if (prefixLen <= 4 && results.length < limit) {
    const families = await db.Families.find(codeFilter, {
      limit: limit - results.length,
      sort: { code: 1 },
    })
    results.push(...families)
  }

  if (prefixLen <= 6 && results.length < limit) {
    const classes = await db.Classes.find(codeFilter, {
      limit: limit - results.length,
      sort: { code: 1 },
    })
    results.push(...classes)
  }

  if (results.length < limit) {
    const commodities = await db.Commodities.find(codeFilter, {
      limit: limit - results.length,
      sort: { code: 1 },
    })
    results.push(...commodities)
  }

  return results
}

// =============================================================================
// Tree Building Queries
// =============================================================================

/**
 * Build a tree structure for a segment or family
 *
 * @example
 * const tree = await buildTree(db, '43', { depth: 2 })
 */
export async function buildTree(
  db: UNSPSCDatabase,
  code: string,
  options: { depth?: number; countChildren?: boolean } = {}
): Promise<TreeNode | null> {
  const { depth = 1, countChildren = true } = options
  const parsed = parseCode(code)

  // Get root entity
  let root: UNSPSCEntity | null = null
  switch (parsed.level) {
    case 'Segment':
      root = await db.Segments.get(code)
      break
    case 'Family':
      root = await db.Families.get(code)
      break
    case 'Class':
      root = await db.Classes.get(code)
      break
    case 'Commodity':
      root = await db.Commodities.get(code)
      break
  }

  if (!root) return null

  const buildNode = async (
    entity: UNSPSCEntity,
    currentDepth: number
  ): Promise<TreeNode> => {
    const node: TreeNode = {
      code: entity.code,
      title: entity.title,
      type: entity.$type,
    }

    if (currentDepth < depth && entity.$type !== 'Commodity') {
      const children = await getChildren(db, entity.code)

      if (children.length > 0) {
        node.children = await Promise.all(
          children.map(child => buildNode(child, currentDepth + 1))
        )
      }
    } else if (countChildren && entity.$type !== 'Commodity') {
      // Just count children without fetching full tree
      node.childCount = await countDescendants(db, entity.code, 1)
    }

    return node
  }

  return buildNode(root, 0)
}

/**
 * Count descendants at a given depth
 */
async function countDescendants(
  db: UNSPSCDatabase,
  code: string,
  depth: number
): Promise<number> {
  const parsed = parseCode(code)

  switch (parsed.level) {
    case 'Segment':
      if (depth === 1) return db.Families.count({ segmentCode: code })
      if (depth === 2) return db.Classes.count({ segmentCode: code })
      if (depth >= 3) return db.Commodities.count({ segmentCode: code })
      break

    case 'Family':
      if (depth === 1) return db.Classes.count({ familyCode: code })
      if (depth >= 2) return db.Commodities.count({ familyCode: code })
      break

    case 'Class':
      if (depth >= 1) return db.Commodities.count({ classCode: code })
      break
  }

  return 0
}

/**
 * Get all segments with their family counts
 */
export async function getSegmentsWithCounts(
  db: UNSPSCDatabase
): Promise<Array<Segment & { familyCount: number; commodityCount: number }>> {
  const segments = await db.Segments.find({}, { sort: { code: 1 } })

  return Promise.all(
    segments.map(async segment => ({
      ...segment,
      familyCount: await db.Families.count({ segmentCode: segment.code }),
      commodityCount: await db.Commodities.count({ segmentCode: segment.code }),
    }))
  )
}

// =============================================================================
// Relationship Queries
// =============================================================================

/**
 * Find related commodities (siblings, cousins)
 *
 * @example
 * const related = await findRelated(db, '43101501', { relationship: 'siblings' })
 */
export async function findRelated(
  db: UNSPSCDatabase,
  code: string,
  options: {
    relationship: 'siblings' | 'cousins' | 'same-segment'
    limit?: number
  }
): Promise<Commodity[]> {
  const { relationship, limit = 50 } = options
  const parsed = parseCode(code)

  if (parsed.level !== 'Commodity') {
    throw new Error('findRelated only works with Commodity codes')
  }

  let filter: Record<string, unknown>

  switch (relationship) {
    case 'siblings':
      // Same class
      filter = { classCode: parsed.class, code: { $ne: code } }
      break

    case 'cousins':
      // Same family, different class
      filter = { familyCode: parsed.family, classCode: { $ne: parsed.class } }
      break

    case 'same-segment':
      // Same segment
      filter = { segmentCode: parsed.segment, code: { $ne: code } }
      break
  }

  return db.Commodities.find(filter, { limit, sort: { code: 1 } })
}

// =============================================================================
// Statistics Queries
// =============================================================================

/**
 * Get UNSPSC taxonomy statistics
 */
export async function getStatistics(
  db: UNSPSCDatabase
): Promise<{
  totalSegments: number
  totalFamilies: number
  totalClasses: number
  totalCommodities: number
  averageFamiliesPerSegment: number
  averageClassesPerFamily: number
  averageCommoditiesPerClass: number
}> {
  const [segments, families, classes, commodities] = await Promise.all([
    db.Segments.count(),
    db.Families.count(),
    db.Classes.count(),
    db.Commodities.count(),
  ])

  return {
    totalSegments: segments,
    totalFamilies: families,
    totalClasses: classes,
    totalCommodities: commodities,
    averageFamiliesPerSegment: segments > 0 ? families / segments : 0,
    averageClassesPerFamily: families > 0 ? classes / families : 0,
    averageCommoditiesPerClass: classes > 0 ? commodities / classes : 0,
  }
}

// =============================================================================
// Example Usage
// =============================================================================

/**
 * Example demonstrating query usage
 */
export async function exampleQueries(db: UNSPSCDatabase): Promise<void> {
  console.log('UNSPSC Query Examples\n')

  // 1. Get hierarchy for a commodity code
  console.log('1. Hierarchy for code 43101501:')
  const path = await getHierarchyPath(db, '43101501')
  if (path) {
    console.log(`   Segment: ${path.segment.code} - ${path.segment.title}`)
    console.log(`   Family: ${path.family?.code} - ${path.family?.title}`)
    console.log(`   Class: ${path.class?.code} - ${path.class?.title}`)
    console.log(`   Commodity: ${path.commodity?.code} - ${path.commodity?.title}`)
  }

  // 2. Get children of a family
  console.log('\n2. Classes in Family 4310:')
  const children = await getChildren(db, '4310')
  for (const child of children.slice(0, 5)) {
    console.log(`   ${child.code} - ${child.title}`)
  }

  // 3. Search for "computer"
  console.log('\n3. Search results for "computer":')
  const results = await searchUNSPSC(db, 'computer', { limit: 5 })
  for (const result of results) {
    console.log(`   [${result.score.toFixed(2)}] ${result.entity.code} - ${result.entity.title}`)
  }

  // 4. Get breadcrumbs for navigation
  console.log('\n4. Breadcrumbs for 43101501:')
  const breadcrumbs = await getBreadcrumbs(db, '43101501')
  console.log('   ' + breadcrumbs.map(b => b.title).join(' > '))

  // 5. Find related commodities
  console.log('\n5. Sibling commodities of 43101501:')
  const siblings = await findRelated(db, '43101501', { relationship: 'siblings', limit: 5 })
  for (const sibling of siblings) {
    console.log(`   ${sibling.code} - ${sibling.title}`)
  }

  // 6. Get statistics
  console.log('\n6. Taxonomy Statistics:')
  const stats = await getStatistics(db)
  console.log(`   Total Segments: ${stats.totalSegments}`)
  console.log(`   Total Families: ${stats.totalFamilies}`)
  console.log(`   Total Classes: ${stats.totalClasses}`)
  console.log(`   Total Commodities: ${stats.totalCommodities}`)
}
