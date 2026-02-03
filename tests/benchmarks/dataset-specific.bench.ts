/**
 * Dataset-Specific Benchmarks for ParqueDB
 *
 * Benchmarks using REAL data loaded from disk:
 * - O*NET: Skill matching, career path, similar occupations
 * - IMDB: Movie search, filmography, 6-degrees queries
 * - Wiktionary: Dictionary lookup, translation chains
 * - UNSPSC: Hierarchy traversal, category search
 * - Wikidata: Entity lookup, claim queries
 * - Common Crawl: Link traversal, PageRank simulation
 *
 * IMPORTANT: These benchmarks require data to be loaded first.
 * Run the appropriate load script before benchmarking:
 *
 *   npx tsx examples/onet/load.ts
 *   npx tsx examples/imdb/load.ts
 *   etc.
 *
 * No network calls are made during benchmark runs - all data is read
 * from local Parquet files.
 */

import { describe, bench, beforeAll, afterAll } from 'vitest'
import {
  loadTestData,
  datasetExists,
  getDataStats,
  checkDataAvailability,
  formatBytes,
  randomElement,
  randomInt,
  type DatasetName,
  type LoadedTestData,
} from './setup'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParquetReader } from '../../src/parquet/reader'

// =============================================================================
// Type Definitions for Datasets
// =============================================================================

// O*NET Types
interface ONetOccupation {
  $id: string
  $type: string
  name: string
  socCode: string
  title: string
  description: string
  jobZone?: number | undefined
  jobZoneTitle?: string | undefined
}

interface ONetSkill {
  $id: string
  $type: string
  name: string
  elementId: string
  description?: string | undefined
  category?: string | undefined
}

interface ONetOccupationSkill {
  $id: string
  $type: string
  name: string
  occupation: Record<string, string>
  skill?: Record<string, string> | undefined
  importance?: number | undefined
  level?: number | undefined
}

interface ONetTask {
  $id: string
  $type: string
  name: string
  taskId: string
  statement: string
  occupation: Record<string, string>
  isCore?: boolean | undefined
}

interface ONetTechnology {
  $id: string
  $type: string
  name: string
  commodityCode?: string | undefined
  commodityTitle?: string | undefined
}

// IMDB Types
interface IMDBTitle {
  $id?: string | undefined
  tconst: string
  titleType: string
  primaryTitle: string
  originalTitle?: string | undefined
  isAdult?: boolean | undefined
  startYear?: number | undefined
  endYear?: number | undefined
  runtimeMinutes?: number | undefined
  genres?: string[] | undefined
  averageRating?: number | undefined
  numVotes?: number | undefined
}

interface IMDBPerson {
  $id?: string | undefined
  nconst: string
  primaryName: string
  birthYear?: number | undefined
  deathYear?: number | undefined
  primaryProfession?: string[] | undefined
  knownForTitles?: string[] | undefined
}

interface IMDBCast {
  $id?: string | undefined
  tconst: string
  nconst: string
  ordering?: number | undefined
  category?: string | undefined
  job?: string | undefined
  characters?: string[] | undefined
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Filter records in-memory (simulating query execution)
 */
function filterRecords<T>(
  records: T[],
  predicate: (record: T) => boolean,
  options?: { limit?: number; offset?: number }
): T[] {
  let filtered = records.filter(predicate)

  if (options?.offset) {
    filtered = filtered.slice(options.offset)
  }

  if (options?.limit) {
    filtered = filtered.slice(0, options.limit)
  }

  return filtered
}

/**
 * Sort records by a field
 */
function sortRecords<T>(
  records: T[],
  field: keyof T,
  direction: 'asc' | 'desc' = 'desc'
): T[] {
  return [...records].sort((a, b) => {
    const aVal = a[field]
    const bVal = b[field]
    if (aVal === bVal) return 0
    if (aVal == null) return 1
    if (bVal == null) return -1
    const cmp = aVal < bVal ? -1 : 1
    return direction === 'asc' ? cmp : -cmp
  })
}

// =============================================================================
// O*NET Dataset Benchmarks
// =============================================================================

describe('O*NET Dataset Benchmarks (Real Data)', async () => {
  let data: LoadedTestData | null = null
  let occupations: ONetOccupation[] = []
  let skills: ONetSkill[] = []
  let occupationSkills: ONetOccupationSkill[] = []
  let tasks: ONetTask[] = []
  let technologies: ONetTechnology[] = []
  let socCodes: string[] = []
  let skillIds: string[] = []
  let dataAvailable = false

  beforeAll(async () => {
    const exists = await datasetExists('onet')
    if (!exists) {
      console.warn('\n[SKIP] O*NET dataset not loaded.')
      console.warn('Run `npx tsx examples/onet/load.ts` first to load test data.\n')
      return
    }

    try {
      data = await loadTestData('onet')
      dataAvailable = true

      console.log(`\n[O*NET] Loading data from ${data.stats.path}`)
      console.log(`[O*NET] ${data.stats.fileCount} files (${data.stats.totalSizeFormatted})`)
      console.log(`[O*NET] Collections: ${data.listCollections().join(', ')}\n`)

      // Load data into memory for benchmarking
      occupations = await data.readCollection<ONetOccupation>('occupations')
      socCodes = occupations.map(o => o.socCode)
      console.log(`[O*NET] Loaded ${occupations.length} occupations`)

      try {
        skills = await data.readCollection<ONetSkill>('skills')
        skillIds = skills.map(s => s.elementId)
        console.log(`[O*NET] Loaded ${skills.length} skills`)
      } catch (e) {
        console.log(`[O*NET] Skills collection not available`)
      }

      try {
        occupationSkills = await data.readCollection<ONetOccupationSkill>('occupation-skills')
        console.log(`[O*NET] Loaded ${occupationSkills.length} occupation-skill ratings`)
      } catch (e) {
        console.log(`[O*NET] Occupation-skills collection not available`)
      }

      try {
        tasks = await data.readCollection<ONetTask>('tasks', { limit: 10000 })
        console.log(`[O*NET] Loaded ${tasks.length} tasks`)
      } catch (e) {
        console.log(`[O*NET] Tasks collection not available`)
      }

      try {
        technologies = await data.readCollection<ONetTechnology>('technology')
        console.log(`[O*NET] Loaded ${technologies.length} technologies`)
      } catch (e) {
        console.log(`[O*NET] Technology collection not available`)
      }

    } catch (err) {
      console.error('[O*NET] Error loading data:', err)
      dataAvailable = false
    }
  })

  // Occupation Search Benchmarks
  bench('[O*NET] search occupation by title (prefix)', async () => {
    if (!dataAvailable || occupations.length === 0) return

    const prefix = 'Software'
    filterRecords(
      occupations,
      o => o.title?.toLowerCase().startsWith(prefix.toLowerCase()),
      { limit: 20 }
    )
  })

  bench('[O*NET] search occupation by title (contains)', async () => {
    if (!dataAvailable || occupations.length === 0) return

    const term = 'Engineer'
    filterRecords(
      occupations,
      o => o.title?.toLowerCase().includes(term.toLowerCase()),
      { limit: 20 }
    )
  })

  bench('[O*NET] filter occupations by job zone', async () => {
    if (!dataAvailable || occupations.length === 0) return

    filterRecords(
      occupations,
      o => o.jobZone === 4,
      { limit: 50 }
    )
  })

  bench('[O*NET] search occupations by SOC code prefix', async () => {
    if (!dataAvailable || occupations.length === 0) return

    // Computer occupations start with 15-
    filterRecords(
      occupations,
      o => o.socCode?.startsWith('15-'),
      { limit: 50 }
    )
  })

  // Skill Matching Benchmarks
  bench('[O*NET] find occupation skills by SOC code', async () => {
    if (!dataAvailable || occupationSkills.length === 0 || socCodes.length === 0) return

    const targetSoc = randomElement(socCodes)
    filterRecords(
      occupationSkills,
      os => {
        const occId = Object.values(os.occupation || {})[0]
        return occId?.includes(targetSoc.replace(/\./g, '-'))
      },
      { limit: 50 }
    )
  })

  bench('[O*NET] find skills with importance >= 4', async () => {
    if (!dataAvailable || occupationSkills.length === 0) return

    filterRecords(
      occupationSkills,
      os => (os.importance ?? 0) >= 4,
      { limit: 100 }
    )
  })

  bench('[O*NET] find occupations requiring specific skill', async () => {
    if (!dataAvailable || occupationSkills.length === 0 || skillIds.length === 0) return

    const targetSkill = randomElement(skillIds)
    filterRecords(
      occupationSkills,
      os => {
        const skillId = Object.values(os.skill || {})[0]
        return skillId?.includes(targetSkill.replace(/\./g, '-'))
      },
      { limit: 50 }
    )
  })

  bench('[O*NET] sort skills by importance', async () => {
    if (!dataAvailable || occupationSkills.length === 0) return

    sortRecords(occupationSkills, 'importance', 'desc').slice(0, 100)
  })

  // Task Queries
  bench('[O*NET] find tasks for occupation', async () => {
    if (!dataAvailable || tasks.length === 0 || socCodes.length === 0) return

    const targetSoc = randomElement(socCodes)
    filterRecords(
      tasks,
      t => {
        const occId = Object.values(t.occupation || {})[0]
        return occId?.includes(targetSoc.replace(/\./g, '-'))
      },
      { limit: 50 }
    )
  })

  bench('[O*NET] find core tasks only', async () => {
    if (!dataAvailable || tasks.length === 0) return

    filterRecords(
      tasks,
      t => t.isCore === true,
      { limit: 100 }
    )
  })

  bench('[O*NET] search tasks by keyword', async () => {
    if (!dataAvailable || tasks.length === 0) return

    const keyword = 'analyze'
    filterRecords(
      tasks,
      t => t.statement?.toLowerCase().includes(keyword),
      { limit: 50 }
    )
  })

  // Technology Queries
  bench('[O*NET] search technologies by name', async () => {
    if (!dataAvailable || technologies.length === 0) return

    const term = 'Python'
    filterRecords(
      technologies,
      t => t.name?.toLowerCase().includes(term.toLowerCase()),
      { limit: 20 }
    )
  })

  bench('[O*NET] filter technologies by UNSPSC segment', async () => {
    if (!dataAvailable || technologies.length === 0) return

    filterRecords(
      technologies,
      t => t.commodityCode?.startsWith('43'), // IT segment
      { limit: 50 }
    )
  })

  // Career Path / Similar Occupations
  bench('[O*NET] find similar occupations (same job zone)', async () => {
    if (!dataAvailable || occupations.length === 0) return

    const target = randomElement(occupations)
    if (!target.jobZone) return

    filterRecords(
      occupations,
      o => o.jobZone === target.jobZone && o.socCode !== target.socCode,
      { limit: 20 }
    )
  })

  bench('[O*NET] find occupations with overlapping skills', async () => {
    if (!dataAvailable || occupationSkills.length === 0 || socCodes.length === 0) return

    const targetSoc = randomElement(socCodes)
    const targetId = targetSoc.replace(/\./g, '-')

    // Get skills for target occupation
    const targetSkills = filterRecords(
      occupationSkills,
      os => {
        const occId = Object.values(os.occupation || {})[0]
        return occId?.includes(targetId)
      }
    )

    // Find top skills
    const topSkillIds = sortRecords(targetSkills, 'importance', 'desc')
      .slice(0, 5)
      .map(os => Object.values(os.skill || {})[0])
      .filter(Boolean)

    // Find occupations with same skills (simplified)
    if (topSkillIds.length > 0) {
      filterRecords(
        occupationSkills,
        os => {
          const skillId = Object.values(os.skill || {})[0]
          return topSkillIds.some(id => skillId?.includes(id as string))
        },
        { limit: 50 }
      )
    }
  })

  // Aggregation-style queries
  bench('[O*NET] count occupations by job zone', async () => {
    if (!dataAvailable || occupations.length === 0) return

    const counts = new Map<number, number>()
    for (const occ of occupations) {
      if (occ.jobZone != null) {
        counts.set(occ.jobZone, (counts.get(occ.jobZone) || 0) + 1)
      }
    }
    // Result: counts map
  })

  bench('[O*NET] average skill importance by category', async () => {
    if (!dataAvailable || skills.length === 0 || occupationSkills.length === 0) return

    // Build skill category map
    const skillCategoryMap = new Map<string, string>()
    for (const skill of skills) {
      if (skill.category && skill.elementId) {
        skillCategoryMap.set(skill.elementId, skill.category)
      }
    }

    // Calculate averages
    const sums = new Map<string, { sum: number; count: number }>()
    for (const os of occupationSkills) {
      if (os.importance == null) continue
      const skillIdParts = Object.values(os.skill || {})[0]?.split('/').pop() || ''
      const elementId = skillIdParts.replace(/-/g, '.')
      const category = skillCategoryMap.get(elementId)
      if (category) {
        const data = sums.get(category) || { sum: 0, count: 0 }
        data.sum += os.importance
        data.count++
        sums.set(category, data)
      }
    }

    // Result: averages by category
    Array.from(sums.entries()).map(([cat, data]) => ({
      category: cat,
      avgImportance: data.sum / data.count,
    }))
  })
})

// =============================================================================
// IMDB Dataset Benchmarks
// =============================================================================

describe('IMDB Dataset Benchmarks (Real Data)', async () => {
  let data: LoadedTestData | null = null
  let titles: IMDBTitle[] = []
  let persons: IMDBPerson[] = []
  let cast: IMDBCast[] = []
  let titleCodes: string[] = []
  let personCodes: string[] = []
  let dataAvailable = false

  beforeAll(async () => {
    const exists = await datasetExists('imdb')
    if (!exists) {
      console.warn('\n[SKIP] IMDB dataset not loaded.')
      console.warn('Run `npx tsx examples/imdb/load.ts` first to load test data.\n')
      return
    }

    try {
      data = await loadTestData('imdb')
      dataAvailable = true

      console.log(`\n[IMDB] Loading data from ${data.stats.path}`)
      console.log(`[IMDB] ${data.stats.fileCount} files (${data.stats.totalSizeFormatted})`)
      console.log(`[IMDB] Collections: ${data.listCollections().join(', ')}\n`)

      // Load data into memory (limit for benchmarks)
      try {
        titles = await data.readCollection<IMDBTitle>('titles', { limit: 50000 })
        titleCodes = titles.map(t => t.tconst).filter(Boolean)
        console.log(`[IMDB] Loaded ${titles.length} titles`)
      } catch (e) {
        console.log(`[IMDB] Titles collection not available`)
      }

      try {
        persons = await data.readCollection<IMDBPerson>('persons', { limit: 20000 })
        personCodes = persons.map(p => p.nconst).filter(Boolean)
        console.log(`[IMDB] Loaded ${persons.length} persons`)
      } catch (e) {
        console.log(`[IMDB] Persons collection not available`)
      }

      try {
        cast = await data.readCollection<IMDBCast>('cast', { limit: 100000 })
        console.log(`[IMDB] Loaded ${cast.length} cast entries`)
      } catch (e) {
        console.log(`[IMDB] Cast collection not available`)
      }

    } catch (err) {
      console.error('[IMDB] Error loading data:', err)
      dataAvailable = false
    }
  })

  // Title Search
  bench('[IMDB] search titles by name (prefix)', async () => {
    if (!dataAvailable || titles.length === 0) return

    const prefix = 'The '
    filterRecords(
      titles,
      t => t.primaryTitle?.startsWith(prefix),
      { limit: 20 }
    )
  })

  bench('[IMDB] search titles by name (contains)', async () => {
    if (!dataAvailable || titles.length === 0) return

    const term = 'Star'
    filterRecords(
      titles,
      t => t.primaryTitle?.toLowerCase().includes(term.toLowerCase()),
      { limit: 20 }
    )
  })

  bench('[IMDB] filter by title type (movies)', async () => {
    if (!dataAvailable || titles.length === 0) return

    filterRecords(
      titles,
      t => t.titleType === 'movie',
      { limit: 100 }
    )
  })

  bench('[IMDB] filter by year range', async () => {
    if (!dataAvailable || titles.length === 0) return

    filterRecords(
      titles,
      t => (t.startYear ?? 0) >= 2000 && (t.startYear ?? 0) <= 2020,
      { limit: 100 }
    )
  })

  bench('[IMDB] filter by genre', async () => {
    if (!dataAvailable || titles.length === 0) return

    filterRecords(
      titles,
      t => t.genres?.includes('Action'),
      { limit: 50 }
    )
  })

  bench('[IMDB] top rated movies (rating + votes filter)', async () => {
    if (!dataAvailable || titles.length === 0) return

    const filtered = filterRecords(
      titles,
      t => t.titleType === 'movie' &&
           (t.averageRating ?? 0) >= 8.0 &&
           (t.numVotes ?? 0) >= 10000
    )
    sortRecords(filtered, 'averageRating', 'desc').slice(0, 20)
  })

  bench('[IMDB] complex filter (genre + year + rating)', async () => {
    if (!dataAvailable || titles.length === 0) return

    const filtered = filterRecords(
      titles,
      t => t.genres?.includes('Drama') &&
           (t.startYear ?? 0) >= 2010 &&
           (t.averageRating ?? 0) >= 7.0
    )
    sortRecords(filtered, 'numVotes', 'desc').slice(0, 20)
  })

  // Person Queries
  bench('[IMDB] search persons by name', async () => {
    if (!dataAvailable || persons.length === 0) return

    const term = 'John'
    filterRecords(
      persons,
      p => p.primaryName?.includes(term),
      { limit: 50 }
    )
  })

  bench('[IMDB] filter persons by profession', async () => {
    if (!dataAvailable || persons.length === 0) return

    filterRecords(
      persons,
      p => p.primaryProfession?.includes('director'),
      { limit: 50 }
    )
  })

  // Filmography / Cast Queries
  bench('[IMDB] get cast for title', async () => {
    if (!dataAvailable || cast.length === 0 || titleCodes.length === 0) return

    const targetTitle = randomElement(titleCodes)
    const titleCast = filterRecords(
      cast,
      c => c.tconst === targetTitle
    )
    sortRecords(titleCast, 'ordering', 'asc')
  })

  bench('[IMDB] get filmography for person', async () => {
    if (!dataAvailable || cast.length === 0 || personCodes.length === 0) return

    const targetPerson = randomElement(personCodes)
    filterRecords(
      cast,
      c => c.nconst === targetPerson,
      { limit: 50 }
    )
  })

  bench('[IMDB] find actors in multiple titles', async () => {
    if (!dataAvailable || cast.length === 0 || titleCodes.length === 0) return

    // Get 3 random titles
    const targetTitles = [
      randomElement(titleCodes),
      randomElement(titleCodes),
      randomElement(titleCodes),
    ]

    filterRecords(
      cast,
      c => targetTitles.includes(c.tconst),
      { limit: 100 }
    )
  })

  // 2-hop / Collaboration queries
  bench('[IMDB] find collaborations (2-hop: actor -> titles -> co-actors)', async () => {
    if (!dataAvailable || cast.length === 0 || personCodes.length === 0) return

    const targetPerson = randomElement(personCodes)

    // Get actor's titles
    const actorTitles = filterRecords(
      cast,
      c => c.nconst === targetPerson,
      { limit: 10 }
    )
    const titleIds = actorTitles.map(c => c.tconst)

    // Get co-actors from those titles
    if (titleIds.length > 0) {
      filterRecords(
        cast,
        c => titleIds.includes(c.tconst) && c.nconst !== targetPerson,
        { limit: 50 }
      )
    }
  })

  // Aggregation
  bench('[IMDB] count titles by type', async () => {
    if (!dataAvailable || titles.length === 0) return

    const counts = new Map<string, number>()
    for (const title of titles) {
      if (title.titleType) {
        counts.set(title.titleType, (counts.get(title.titleType) || 0) + 1)
      }
    }
    // Result: counts map
  })

  bench('[IMDB] average rating by genre', async () => {
    if (!dataAvailable || titles.length === 0) return

    const sums = new Map<string, { sum: number; count: number }>()
    for (const title of titles) {
      if (!title.genres || title.averageRating == null) continue
      for (const genre of title.genres) {
        const data = sums.get(genre) || { sum: 0, count: 0 }
        data.sum += title.averageRating
        data.count++
        sums.set(genre, data)
      }
    }

    // Result: averages by genre
    Array.from(sums.entries())
      .map(([genre, data]) => ({
        genre,
        avgRating: data.sum / data.count,
        count: data.count,
      }))
      .sort((a, b) => b.avgRating - a.avgRating)
  })

  bench('[IMDB] count titles by decade', async () => {
    if (!dataAvailable || titles.length === 0) return

    const counts = new Map<number, number>()
    for (const title of titles) {
      if (title.startYear) {
        const decade = Math.floor(title.startYear / 10) * 10
        counts.set(decade, (counts.get(decade) || 0) + 1)
      }
    }
    // Result: counts by decade
    Array.from(counts.entries()).sort((a, b) => a[0] - b[0])
  })
})

// =============================================================================
// Summary and Data Availability Check
// =============================================================================

describe('Dataset Benchmarks Summary', () => {
  bench('check all datasets availability', async () => {
    const datasets: DatasetName[] = ['onet', 'imdb', 'wiktionary', 'unspsc', 'wikidata', 'commoncrawl']

    for (const dataset of datasets) {
      const stats = await getDataStats(dataset)
      if (!stats.available) {
        // Dataset not available - this is expected
      }
    }
  })
})
