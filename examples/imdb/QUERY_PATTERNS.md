# IMDB Query Patterns for ParqueDB

This document catalogs real-world query patterns for the IMDB dataset in ParqueDB, organized by use case. Each pattern includes the business question, filter structure, recommended index columns, selectivity analysis, and row-group statistics pushdown potential.

## ParqueDB Column Architecture

ParqueDB uses a dual Variant architecture with the following column structure:

```
$id          - Entity identifier (always indexed)
$index_*     - Shredded columns for indexed fields
$data        - Variant column containing remaining fields
```

For the IMDB schema, the shredded `$index_*` columns are:
- **Title**: `$index_type`, `$index_startYear`, `$index_genres`
- **Person**: `$index_birthYear`, `$index_professions`
- **AlternateTitle**: `$index_region`, `$index_language`
- **Rating**: `$index_averageRating`

---

## Query Pattern Summary

| # | Use Case | Business Question | Index Columns Used | Selectivity | Row-Group Stats |
|---|----------|-------------------|-------------------|-------------|-----------------|
| 1 | Streaming | Movies by genre + rating | `$index_type`, `$index_genres`, `$index_averageRating` | Medium | Yes |
| 2 | Streaming | New releases by year | `$index_type`, `$index_startYear` | High | Yes |
| 3 | Streaming | Content by decade | `$index_startYear` | Medium | Yes |
| 4 | Streaming | Runtime filtering | `$index_type` + `$data.runtimeMinutes` | Medium | Partial |
| 5 | Recommendation | Similar titles by cast overlap | Relationship traversal | Low | No |
| 6 | Recommendation | Top rated in genre | `$index_type`, `$index_genres`, `$index_averageRating` | Medium | Yes |
| 7 | Search | Title text search | `$text` index | High | No |
| 8 | Search | Person by name | `$text` index | High | No |
| 9 | Search | Localized title lookup | `$index_region`, `$index_language` | High | Yes |
| 10 | Box Office | Highest voted titles | `$index_type` + `$data.numVotes` | Medium | Partial |
| 11 | Analytics | Genre distribution | `$index_genres` | Low | Partial |
| 12 | Analytics | Year-over-year trends | `$index_startYear` | Low | Yes |
| 13 | Graph | Six degrees of Kevin Bacon | Relationship traversal | Low | No |
| 14 | Graph | Director-actor pairs | Relationship traversal + aggregation | Low | No |
| 15 | Graph | TV series episodes | `$index_type` + relationship | Medium | Yes |
| 16 | News | Living legends (age query) | `$index_birthYear`, `$data.deathYear` | High | Yes |
| 17 | News | Recent deaths | `$index_birthYear`, `$data.deathYear` | High | Partial |
| 18 | News | Filmography lookup | Relationship traversal | High | No |
| 19 | Trivia | Career span analysis | Relationship + `$index_startYear` | Low | Partial |
| 20 | Trivia | Franchise timeline | Relationship traversal | Medium | Partial |

---

## Detailed Query Patterns

### 1. Streaming Service: Browse Movies by Genre and Minimum Rating

**Business Question:** "Show me Action movies rated 7.5 or higher"

**Use Case:** Netflix/Disney+ genre browsing pages with quality filters

```typescript
db.movies.find({
  type: 'movie',
  genres: { $contains: 'Action' },
  averageRating: { $gte: 7.5 }
}, {
  sort: { averageRating: -1 },
  limit: 50
})
```

**Index Columns:** `$index_type`, `$index_genres`, `$index_averageRating`

**Selectivity:** Medium (~2-5% of titles are Action movies with rating >= 7.5)

**Row-Group Stats Pushdown:** Yes - all three predicates can use min/max statistics:
- `type = 'movie'` eliminates row groups with only TV content
- `averageRating >= 7.5` eliminates row groups where max < 7.5
- `genres` array membership can check bloom filter

---

### 2. Streaming Service: New Releases by Year

**Business Question:** "Show all movies released in 2024"

**Use Case:** "New This Year" carousel on streaming home page

```typescript
db.movies.find({
  type: 'movie',
  startYear: 2024
}, {
  sort: { numVotes: -1 },
  limit: 100
})
```

**Index Columns:** `$index_type`, `$index_startYear`

**Selectivity:** High (~0.5-1% of all titles are current year releases)

**Row-Group Stats Pushdown:** Yes - `startYear = 2024` can skip row groups where min > 2024 or max < 2024

---

### 3. Streaming Service: Content by Decade

**Business Question:** "Show classic movies from the 1980s"

**Use Case:** Decade-themed collections or nostalgia features

```typescript
db.movies.find({
  type: 'movie',
  startYear: { $gte: 1980, $lt: 1990 }
}, {
  sort: { averageRating: -1 },
  limit: 100
})
```

**Index Columns:** `$index_type`, `$index_startYear`

**Selectivity:** Medium (~5-8% of movies fall within a single decade)

**Row-Group Stats Pushdown:** Yes - range predicates on `startYear` work well with min/max statistics

---

### 4. Streaming Service: Filter by Runtime

**Business Question:** "Show movies under 90 minutes for a quick watch"

**Use Case:** "Short and Sweet" or "Movie Night Quick Picks" features

```typescript
db.movies.find({
  type: 'movie',
  runtimeMinutes: { $lte: 90 },
  averageRating: { $gte: 6.5 }
}, {
  sort: { averageRating: -1 },
  limit: 50
})
```

**Index Columns:** `$index_type`, partial `$data.runtimeMinutes` (if shredded)

**Selectivity:** Medium (~15-20% of movies are under 90 minutes)

**Row-Group Stats Pushdown:** Partial - `type` uses stats, `runtimeMinutes` only if shredded or via bloom filter

---

### 5. Recommendation Engine: Similar Titles by Cast Overlap

**Business Question:** "Find movies with overlapping cast to recommend similar content"

**Use Case:** "Because you watched X" recommendations

```typescript
// Step 1: Get cast of target movie
const cast = await db.movies.get('tt1375666', {
  populate: ['cast']
})

// Step 2: Find other movies these actors appeared in
const similar = await db.people.find({
  $id: { $in: cast.cast.map(p => p.$id) }
}, {
  populate: {
    actedIn: {
      filter: { type: 'movie', $id: { $ne: 'tt1375666' } },
      limit: 10
    }
  }
})
```

**Index Columns:** Relationship indexes (forward/reverse)

**Selectivity:** Low - requires graph traversal

**Row-Group Stats Pushdown:** No - relationship traversal requires index lookup

---

### 6. Recommendation Engine: Top Rated in Genre

**Business Question:** "What are the highest-rated Sci-Fi movies of all time?"

**Use Case:** "Best of Genre" curated lists

```typescript
db.movies.find({
  type: 'movie',
  genres: { $contains: 'Sci-Fi' },
  numVotes: { $gte: 50000 }  // Ensure statistical significance
}, {
  sort: { averageRating: -1 },
  limit: 25
})
```

**Index Columns:** `$index_type`, `$index_genres`, `$index_averageRating`

**Selectivity:** Medium (~500-1000 sci-fi movies with 50k+ votes)

**Row-Group Stats Pushdown:** Yes - all predicates can leverage statistics

---

### 7. Search: Title Text Search

**Business Question:** "Find movies matching 'dark knight'"

**Use Case:** Search bar autocomplete, search results page

```typescript
db.movies.find({
  $text: { $search: 'dark knight' },
  type: 'movie'
}, {
  sort: { numVotes: -1 },  // Popularity ranking
  limit: 20
})
```

**Index Columns:** `$text` full-text index on `primaryTitle`, `originalTitle`

**Selectivity:** High - text search is highly selective

**Row-Group Stats Pushdown:** No - requires full-text index, but can filter row groups by type

---

### 8. Search: Person by Name

**Business Question:** "Find actors named 'Robert'"

**Use Case:** Cast member search, person disambiguation

```typescript
db.people.find({
  $text: { $search: 'Robert' },
  professions: { $contains: 'actor' }
}, {
  limit: 50
})
```

**Index Columns:** `$text` index on `name`, `$index_professions`

**Selectivity:** High - name search is selective

**Row-Group Stats Pushdown:** No for text, but professions can use bloom filter

---

### 9. Search: Localized Title Lookup

**Business Question:** "Find the movie known as 'El Padrino' in Spanish"

**Use Case:** International content discovery, regional search

```typescript
db.alternateTitles.find({
  localizedTitle: { $eq: 'El Padrino' },
  region: 'ES'
}, {
  populate: ['title']
})
```

**Index Columns:** `$index_region`, text index on `localizedTitle`

**Selectivity:** High - specific title + region is very selective

**Row-Group Stats Pushdown:** Yes - region partitioning allows skipping non-ES row groups entirely

---

### 10. Box Office Analytics: Most Voted Titles

**Business Question:** "What are the most popular movies by vote count?"

**Use Case:** Popularity charts, trending content

```typescript
db.movies.find({
  type: 'movie',
  numVotes: { $gte: 1000000 }
}, {
  sort: { numVotes: -1 },
  limit: 100
})
```

**Index Columns:** `$index_type`, `$data.numVotes`

**Selectivity:** Medium (~500-1000 movies have 1M+ votes)

**Row-Group Stats Pushdown:** Partial - `numVotes` threshold can skip row groups if shredded

---

### 11. Analytics: Genre Distribution

**Business Question:** "How are genres distributed across all movies?"

**Use Case:** Content library analysis, acquisition planning

```typescript
// Aggregation pattern
db.movies.aggregate([
  { $match: { type: 'movie' } },
  { $unwind: '$genres' },
  { $group: { _id: '$genres', count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```

**Index Columns:** `$index_type`, `$index_genres`

**Selectivity:** Low - full scan of movies required

**Row-Group Stats Pushdown:** Partial - type filter reduces scan scope

---

### 12. Analytics: Year-over-Year Trends

**Business Question:** "How many movies were released each year in the 2010s?"

**Use Case:** Industry trend analysis, annual reports

```typescript
db.movies.aggregate([
  {
    $match: {
      type: 'movie',
      startYear: { $gte: 2010, $lte: 2019 }
    }
  },
  { $group: { _id: '$startYear', count: { $sum: 1 } } },
  { $sort: { _id: 1 } }
])
```

**Index Columns:** `$index_type`, `$index_startYear`

**Selectivity:** Low - scans significant portion of data

**Row-Group Stats Pushdown:** Yes - year range allows skipping row groups outside 2010-2019

---

### 13. Graph Traversal: Six Degrees of Kevin Bacon

**Business Question:** "Find the shortest path between two actors through shared movies"

**Use Case:** Entertainment trivia, social connection features

```typescript
// BFS traversal pattern
async function findPath(person1Id: string, person2Id: string, maxDepth = 6) {
  const visited = new Set<string>()
  const queue = [{ id: person1Id, path: [] }]

  while (queue.length > 0) {
    const { id, path } = queue.shift()!
    if (id === person2Id) return path
    if (path.length >= maxDepth) continue
    if (visited.has(id)) continue
    visited.add(id)

    // Get all movies this person acted in
    const person = await db.people.get(id, {
      populate: { actedIn: { limit: 100 } }
    })

    for (const movie of person.actedIn || []) {
      // Get all other actors in this movie
      const cast = await db.movies.get(movie.$id, {
        populate: { cast: { limit: 50 } }
      })

      for (const actor of cast.cast || []) {
        if (!visited.has(actor.$id)) {
          queue.push({
            id: actor.$id,
            path: [...path, { via: movie, person: actor }]
          })
        }
      }
    }
  }
  return null // No path found
}
```

**Index Columns:** Relationship indexes (forward/reverse)

**Selectivity:** Low - requires extensive graph traversal

**Row-Group Stats Pushdown:** No - pure graph traversal pattern

---

### 14. Graph Traversal: Director-Actor Collaboration Pairs

**Business Question:** "Which directors have worked with the same actors most frequently?"

**Use Case:** Industry insights, collaboration analysis

```typescript
// Find frequent collaborators
db.personCollaborations.find({
  collaborationType: 'frequently_works_with',
  sharedTitleCount: { $gte: 5 }
}, {
  sort: { sharedTitleCount: -1 },
  limit: 50,
  populate: ['person1', 'person2']
})
```

**Index Columns:** `$data.collaborationType`, `$data.sharedTitleCount`

**Selectivity:** Low - requires pre-computed collaboration data

**Row-Group Stats Pushdown:** No - requires aggregation or pre-computation

---

### 15. TV Content: Series Episodes

**Business Question:** "Get all episodes of Breaking Bad Season 5"

**Use Case:** Episode browser, season navigation

```typescript
// First find the series
const series = await db.movies.findOne({
  primaryTitle: 'Breaking Bad',
  type: 'tvSeries'
})

// Then get episodes
db.movies.find({
  type: 'tvEpisode',
  parentSeries: series.$id,
  seasonNumber: 5
}, {
  sort: { episodeNumber: 1 }
})
```

**Index Columns:** `$index_type`, relationship indexes

**Selectivity:** Medium - series have 10-500 episodes typically

**Row-Group Stats Pushdown:** Yes - type filter is highly selective for episodes

---

### 16. Entertainment News: Living Legends Query

**Business Question:** "Find actors born before 1950 who are still alive"

**Use Case:** Anniversary features, tribute articles

```typescript
db.people.find({
  birthYear: { $lte: 1950 },
  deathYear: { $exists: false },
  professions: { $contains: 'actor' }
}, {
  sort: { birthYear: 1 },
  limit: 100
})
```

**Index Columns:** `$index_birthYear`, `$index_professions`

**Selectivity:** High - very few people born before 1950 still alive

**Row-Group Stats Pushdown:** Yes - `birthYear <= 1950` skips many row groups

---

### 17. Entertainment News: Recent Deaths

**Business Question:** "Find notable people who passed away in 2024"

**Use Case:** Obituaries, "In Memoriam" features

```typescript
db.people.find({
  deathYear: 2024,
  professions: { $in: ['actor', 'director', 'producer'] }
}, {
  sort: { name: 1 }
})
```

**Index Columns:** `$index_professions`, `$data.deathYear`

**Selectivity:** High - very few deaths in current year

**Row-Group Stats Pushdown:** Partial - professions can filter, deathYear if shredded

---

### 18. Entertainment News: Complete Filmography

**Business Question:** "Get Tom Hanks' complete filmography with ratings"

**Use Case:** Actor profile pages, career retrospectives

```typescript
const person = await db.people.findOne({
  name: 'Tom Hanks',
  professions: { $contains: 'actor' }
})

db.movies.find({
  cast: { $contains: person.$id }
}, {
  sort: { startYear: -1 },
  populate: ['rating']
})

// Or using relationship traversal
const filmography = await db.people.get(person.$id, {
  populate: {
    actedIn: {
      sort: { startYear: -1 },
      populate: ['rating']
    }
  }
})
```

**Index Columns:** Relationship indexes, `$index_startYear`

**Selectivity:** High - single person lookup

**Row-Group Stats Pushdown:** No - relationship lookup, but subsequent filters can use stats

---

### 19. Trivia: Career Span Analysis

**Business Question:** "Who has the longest acting career?"

**Use Case:** Record features, career milestones

```typescript
// Requires aggregation across relationships
db.careerMilestones.aggregate([
  { $match: { role: 'acted_in' } },
  { $group: {
      _id: '$personId',
      firstYear: { $min: '$year' },
      lastYear: { $max: '$year' }
    }
  },
  { $addFields: { careerSpan: { $subtract: ['$lastYear', '$firstYear'] } } },
  { $sort: { careerSpan: -1 } },
  { $limit: 25 }
])
```

**Index Columns:** `$data.role`, `$data.year`

**Selectivity:** Low - requires full scan of milestones

**Row-Group Stats Pushdown:** Partial - year statistics help with range queries

---

### 20. Trivia: Franchise Timeline

**Business Question:** "Show all Star Wars movies in release order"

**Use Case:** Franchise hubs, watch order guides

```typescript
// Using title links for franchise connections
const starWars = await db.movies.findOne({
  primaryTitle: 'Star Wars',
  startYear: 1977,
  type: 'movie'
})

// Traverse sequel/prequel relationships
const franchise = await db.titleLinks.find({
  $or: [
    { fromTitleId: starWars.$id },
    { toTitleId: starWars.$id }
  ],
  linkType: { $in: ['sequel_to', 'prequel_to', 'spinoff_of'] }
}, {
  populate: ['fromTitle', 'toTitle']
})

// Or search by title pattern
db.movies.find({
  $text: { $search: 'Star Wars' },
  type: 'movie'
}, {
  sort: { startYear: 1 }
})
```

**Index Columns:** `$text` index, `$index_type`, `$index_startYear`, relationship indexes

**Selectivity:** Medium - franchise typically has 5-20 films

**Row-Group Stats Pushdown:** Partial - type and year filters use stats

---

## Performance Optimization Guidelines

### When Row-Group Statistics Help Most

1. **Year range queries** - `startYear` has excellent locality, row groups are often sorted by insertion order which correlates with release date
2. **Type filtering** - Partitioning by type eliminates entire files
3. **Rating thresholds** - `averageRating >= X` can skip low-rating row groups entirely
4. **Region/language filters** - Partitioned alternate titles enable file-level pruning

### When to Use Secondary Indexes

1. **Text search** - Always requires full-text index, cannot use statistics
2. **Exact ID lookup** - Hash index on `$id` for O(1) access
3. **Low-cardinality enums** - Bloom filters for `type`, `genre`, `profession`

### When Graph Traversal is Required

1. **Relationship queries** - "Who acted with whom" requires edge traversal
2. **Path finding** - Bacon number requires BFS/DFS
3. **Collaboration analysis** - Pre-compute and materialize for performance

### Recommended Index Configuration for IMDB

```typescript
const indexes = {
  // Primary indexes (automatic)
  '$id': 'hash',

  // Shredded columns (automatic row-group stats)
  'Title.$index_type': 'enum',
  'Title.$index_startYear': 'range',
  'Title.$index_genres': 'bloom',
  'Person.$index_birthYear': 'range',
  'Person.$index_professions': 'bloom',
  'Rating.$index_averageRating': 'range',
  'AlternateTitle.$index_region': 'enum',

  // Full-text indexes
  'Title.primaryTitle': 'fts',
  'Title.originalTitle': 'fts',
  'Person.name': 'fts',
  'AlternateTitle.localizedTitle': 'fts',

  // Secondary indexes for common queries
  'Title.numVotes': 'range',
  'Person.deathYear': 'range',
}
```

---

## Query Pattern Categories by Response Time

### Fast (< 50ms)
- Single entity lookup by ID
- Type + year equality filters
- Text search with type filter (if indexed)

### Medium (50-200ms)
- Genre + rating range queries
- Filmography traversal (single person)
- Episode listing for a series

### Slow (200ms - 2s)
- Full-text search without type filter
- Multi-hop graph traversal
- Aggregation queries on large result sets

### Very Slow (> 2s, consider pre-computation)
- Six degrees path finding
- Collaboration pair computation
- Cross-genre statistics
- Career span analysis across all people

---

## Conclusion

The IMDB dataset provides an excellent test case for ParqueDB's hybrid relational/document/graph capabilities. The key insights are:

1. **Shredded columns** (`$index_*`) are essential for common filtering patterns
2. **Partitioning by type** dramatically reduces I/O for most queries
3. **Row-group statistics** provide significant speedups for range queries on year and rating
4. **Graph traversal** patterns require relationship indexes and often benefit from pre-computation
5. **Full-text search** requires dedicated indexing and cannot leverage columnar statistics

By understanding which queries benefit from which optimization techniques, applications can achieve sub-100ms response times for the vast majority of user-facing queries while reserving heavier computations for background jobs or cached results.
