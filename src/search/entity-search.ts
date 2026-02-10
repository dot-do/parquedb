/**
 * Entity Search Engine — cross-type full-text search with relevance scoring.
 *
 * Provides entity scoring and cross-type search without any dependency
 * on Durable Object internals. Pure utility functions.
 */

export interface ScoreResult {
  score: number
  snippet: string
}

export interface SearchParams {
  entities: Iterable<Record<string, unknown>>
  query: string
  types?: string[]
  limit?: number
  offset?: number
}

export interface EntitySearchResult {
  data: Array<Record<string, unknown>>
  meta: { total: number; limit: number; offset: number }
}

/**
 * Score an entity against a search query.
 *
 * Scoring rules:
 *   - Exact match (entire field equals query): 10
 *   - Substring match (field contains full query): 5
 *   - Term match (field contains individual terms): up to 3 (proportional)
 *   - name/title fields receive a 1.5x boost
 *
 * Fields prefixed with `_` and non-string values are skipped.
 */
export function scoreEntity(entity: Record<string, unknown>, queryLower: string, queryTerms: string[]): ScoreResult {
  let bestScore = 0
  let bestSnippet = ''

  for (const [key, value] of Object.entries(entity)) {
    if (key.startsWith('_')) continue
    if (typeof value !== 'string') continue

    const valueLower = value.toLowerCase()
    let fieldScore = 0

    // Exact full-query match in field
    if (valueLower === queryLower) {
      fieldScore = 10
    } else if (valueLower.includes(queryLower)) {
      // Full query substring match
      fieldScore = 5
    } else {
      // Check individual terms
      let termMatches = 0
      for (const term of queryTerms) {
        if (valueLower.includes(term)) {
          termMatches++
        }
      }
      if (termMatches > 0) {
        fieldScore = (termMatches / queryTerms.length) * 3
      }
    }

    // Boost name/title fields
    if (fieldScore > 0 && (key === 'name' || key === 'title')) {
      fieldScore *= 1.5
    }

    if (fieldScore > bestScore) {
      bestScore = fieldScore
      bestSnippet = `${key}: ${value}`
    }
  }

  return { score: bestScore, snippet: bestSnippet }
}

/**
 * Search an iterable of entities using full-text scoring.
 *
 * Handles query parsing, term splitting, type filtering, scoring,
 * sorting by score descending, and pagination via limit/offset.
 *
 * Entities with a truthy `$deletedAt` field are excluded.
 */
export function searchEntities({ entities, query, types, limit = 20, offset = 0 }: SearchParams): EntitySearchResult {
  const trimmed = query.trim()
  if (trimmed.length === 0) {
    return { data: [], meta: { total: 0, limit, offset } }
  }

  const queryLower = trimmed.toLowerCase()
  const queryTerms = queryLower.split(/\s+/).filter(Boolean)

  const scoredResults: Array<Record<string, unknown>> = []

  for (const rawEntity of entities) {
    // Skip soft-deleted entities
    if (rawEntity.$deletedAt) continue

    // Filter by type if specified
    if (types && types.length > 0) {
      const entityType = rawEntity.$type as string | undefined
      if (!entityType || !types.includes(entityType)) continue
    }

    const match = scoreEntity(rawEntity, queryLower, queryTerms)
    if (match.score > 0) {
      scoredResults.push({
        ...rawEntity,
        _score: match.score,
        _snippet: match.snippet,
      })
    }
  }

  // Sort by score descending
  scoredResults.sort((a, b) => (b._score as number) - (a._score as number))

  const total = scoredResults.length
  const paged = scoredResults.slice(offset, offset + limit)

  return { data: paged, meta: { total, limit, offset } }
}
