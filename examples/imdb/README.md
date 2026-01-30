# IMDB Example for ParqueDB

This example demonstrates loading the IMDB dataset into ParqueDB, showcasing:

- Streaming large TSV files (~3GB compressed)
- Comprehensive graph schema for entities and relationships
- Partitioned storage by entity type and attributes
- Multipart uploads for large Parquet files
- Rich query patterns for graph traversal
- Performance benchmarking

## Dataset Overview

IMDB provides several datasets at https://datasets.imdbws.com/:

| File | Description | Size (compressed) |
|------|-------------|-------------------|
| `title.basics.tsv.gz` | Core title information | ~150MB |
| `title.ratings.tsv.gz` | User ratings | ~7MB |
| `title.crew.tsv.gz` | Directors and writers | ~70MB |
| `title.principals.tsv.gz` | Principal cast/crew | ~500MB |
| `title.episode.tsv.gz` | TV episode mappings | ~50MB |
| `name.basics.tsv.gz` | Person information | ~250MB |
| `title.akas.tsv.gz` | Alternate/localized titles | ~200MB |

Total: approximately 1.2GB compressed, 3GB+ uncompressed.

## Schema

### Entity Types

**Title** (movies, TV shows, episodes):
```typescript
{
  id: string           // tconst (tt0000001)
  type: TitleType      // movie, tvSeries, tvEpisode, etc.
  primaryTitle: string
  originalTitle: string
  isAdult: boolean
  startYear: number | null
  endYear: number | null
  runtimeMinutes: number | null
  genres: string[]
}
```

**Person** (actors, directors, writers):
```typescript
{
  id: string           // nconst (nm0000001)
  name: string
  birthYear: number | null
  deathYear: number | null
  professions: string[]
}
```

**AlternateTitle** (localized titles from title.akas.tsv):
```typescript
{
  titleId: string
  ordering: number
  localizedTitle: string
  region: string | null     // ISO 3166-1 alpha-2
  language: string | null   // ISO 639-1
  types: string[]           // alternative, working, imdbDisplay, etc.
  attributes: string[]
  isOriginalTitle: boolean
}
```

**Rating** (separate entity for proper graph structure):
```typescript
{
  titleId: string
  averageRating: number
  numVotes: number
}
```

**Genre** (separate entity with title relationships):
```typescript
{
  id: string
  name: string
  titleCount: number
  averageRating: number | null
}
```

**PersonCollaboration** (computed person-to-person relationships):
```typescript
{
  person1Id: string
  person2Id: string
  collaborationType: 'collaborated_with' | 'frequently_works_with'
  sharedTitleCount: number
  firstCollabYear: number | null
  lastCollabYear: number | null
}
```

**CareerMilestone** (key career moments):
```typescript
{
  personId: string
  titleId: string
  year: number
  milestoneType: 'first_credit' | 'breakthrough' | 'peak_rating' | 'most_votes'
  role: string
  rating: number | null
  votes: number | null
}
```

### Relationships

The IMDB data forms a rich graph between People, Titles, and other entities:

```
Person --[acted_in]--> Title
Person --[directed]--> Title
Person --[wrote]-----> Title
Person --[produced]--> Title
Title  --[episode_of]-> Title (for TV episodes)
Title  --[has_alternate_title]--> AlternateTitle
Title  --[has_genre]--> Genre
Title  --[sequel_to]--> Title
Title  --[remake_of]--> Title
Title  --[spinoff_of]--> Title
Person --[collaborated_with]--> Person
Person --[frequently_works_with]--> Person
```

Relationship edges include metadata:
- `ordering`: billing order
- `job`: specific role (e.g., "screenplay")
- `characters`: character names for actors

## Storage Layout

Data is stored in R2/storage with partitioning for efficient queries:

```
imdb/
  titles/
    type=movie/
      data.0000.parquet
      data.0001.parquet
    type=tvSeries/
      data.0000.parquet
    type=tvEpisode/
      data.0000.parquet
      ...
  people/
    data.0000.parquet
    data.0001.parquet
    ...
  relationships/
    acted_in/
      data.0000.parquet
      ...
    directed/
      data.0000.parquet
      ...
    wrote/
      ...
  episodes/
    data.0000.parquet
    ...
  alternate_titles/
    region=US/
      data.0000.parquet
    region=JP/
      data.0000.parquet
    ...
  ratings/
    data.0000.parquet
    ...
  genres/
    data.parquet
  title_genres/
    data.0000.parquet
    ...
  person_collaborations/
    data.0000.parquet
    ...
  career_milestones/
    data.0000.parquet
    ...
```

## Usage

### Loading Data

```typescript
import { loadAll, loadExtended } from './examples/imdb/load'

// In a Cloudflare Worker with R2 binding:
export default {
  async fetch(request: Request, env: Env) {
    // Load core data
    const stats = await loadAll(env.BUCKET, {
      prefix: 'imdb',
      onProgress: (info) => {
        console.log(`${info.dataset}: ${info.rowsProcessed} rows`)
      }
    })

    // Load extended data (computationally expensive)
    const extendedStats = await loadExtended(env.BUCKET, {
      prefix: 'imdb',
      minCollaborations: 3  // Only store pairs with 3+ shared titles
    })

    return Response.json({ stats, extendedStats })
  }
}
```

### Basic Queries

```typescript
import {
  searchTitles,
  searchPeople,
  getTitle,
  getPerson,
  getFilmography,
  getCastAndCrew,
  getTopRated
} from './examples/imdb/queries'

// Search for movies
const movies = await searchTitles(bucket, 'inception', {
  type: 'movie',
  minRating: 8.0,
  yearFrom: 2000,
  sort: 'rating'
})

// Get a person's filmography
const filmography = await getFilmography(bucket, 'nm0000138', {
  role: 'directed',
  sort: 'year'
})

// Get cast and crew for a movie
const cast = await getCastAndCrew(bucket, 'tt1375666')

// Top rated movies by genre
const topSciFi = await getTopRated(bucket, {
  type: 'movie',
  genre: 'Sci-Fi',
  minVotes: 100000
})
```

### Alternate Title Queries

```typescript
import { findByAlternateTitle, getAlternateTitles } from './examples/imdb/queries'

// Find by Japanese title
const spiritedAway = await findByAlternateTitle(
  bucket,
  'JP',
  '千と千尋の神隠し'
)

// Find by any localized title
const results = await findByAlternateTitle(
  bucket,
  null,  // all regions
  'Spirited Away'
)

// Get all alternate titles for a movie
const alternateTitles = await getAlternateTitles(bucket, 'tt0245429')
```

### Genre Analysis

```typescript
import {
  getGenres,
  getGenreCooccurrence,
  getTitlesByGenre
} from './examples/imdb/queries'

// Get all genres with statistics
const genres = await getGenres(bucket)

// Find which genres appear together most often
const cooccurrences = await getGenreCooccurrence(bucket, {
  type: 'movie',
  minCount: 100,
  limit: 50
})
// Returns: [{ genre1: 'Action', genre2: 'Adventure', count: 5000, percentage: 45 }, ...]

// Get top titles for a genre
const actionMovies = await getTitlesByGenre(bucket, 'action', {
  type: 'movie',
  minRating: 7.0,
  sort: 'rating'
})
```

### Director-Actor Pairs

```typescript
import { findDirectorActorPairs } from './examples/imdb/queries'

// Find directors who frequently work with the same actors
const pairs = await findDirectorActorPairs(bucket, {
  minCollaborations: 5,
  limit: 50
})

// Returns pairs like:
// { directorId: 'nm0000229', directorName: 'Steven Spielberg',
//   actorId: 'nm0000158', actorName: 'Tom Hanks',
//   collaborationCount: 7, titles: [...] }
```

### Career Analysis

```typescript
import {
  getCareerTrajectory,
  findBreakthroughRoles
} from './examples/imdb/queries'

// Get a person's career trajectory
const trajectory = await getCareerTrajectory(bucket, 'nm0000138')
// Returns: {
//   personId: 'nm0000138',
//   name: 'Leonardo DiCaprio',
//   careerStart: 1989,
//   careerEnd: 2023,
//   phases: [
//     { phase: 'early', startYear: 1989, endYear: 1994, titleCount: 12, avgRating: 6.5, topRole: 'actor' },
//     { phase: 'rising', startYear: 1995, endYear: 2000, titleCount: 8, avgRating: 7.2, topRole: 'actor' },
//     ...
//   ],
//   milestones: [
//     { type: 'first_credit', year: 1989, titleId: '...', title: '...' },
//     { type: 'breakthrough', year: 1997, titleId: 'tt0120338', title: 'Titanic' },
//     ...
//   ]
// }

// Find breakthrough roles for actors
const breakthroughs = await findBreakthroughRoles(bucket, {
  yearFrom: 2010,
  yearTo: 2020,
  minVotes: 10000,
  minRating: 7.0,
  limit: 50
})
```

### Graph Traversal Patterns

#### 1-hop: Direct Relationships

```typescript
// Person -> Title (filmography)
const films = await getFilmography(bucket, personId)

// Title <- Person (cast/crew)
const cast = await getCastAndCrew(bucket, titleId)
```

#### 2-hop: Co-occurrence

```typescript
import { findCollaborations, findSimilarTitles } from './examples/imdb/queries'

// Person -> Title <- Person (collaborations)
const shared = await findCollaborations(bucket, person1, person2)

// Title <- Person -> Title (similar titles)
const similar = await findSimilarTitles(bucket, titleId)
```

#### Hierarchical: Episodes

```typescript
import { getEpisodes, getSeriesForEpisode } from './examples/imdb/queries'

// Series -> Episodes
const episodes = await getEpisodes(bucket, seriesId, { season: 1 })

// Episode -> Series
const series = await getSeriesForEpisode(bucket, episodeId)
```

## Benchmarking

The example includes comprehensive benchmark scenarios:

```typescript
import { runBenchmarks, formatBenchmarkReport } from './examples/imdb/queries'

const results = await runBenchmarks(bucket, {
  iterations: 3,
  warmupIterations: 1,
  verbose: true
})

console.log(formatBenchmarkReport(results))
```

### Benchmark Scenarios

| Category | Query | Description |
|----------|-------|-------------|
| Cold Start | searchTitles | Search titles by name (first access) |
| Cold Start | getTitle | Get single title by ID (first access) |
| 1-Hop | getFilmography | Person -> Title traversal |
| 1-Hop | getCastAndCrew | Title <- Person traversal |
| 2-Hop | findCollaborations | Person -> Title <- Person |
| 2-Hop | findSimilarTitles | Title <- Person -> Title |
| 3-Hop | getCareerTrajectory | Person -> Title -> Rating + Milestones |
| Aggregation | getGenreStats | Full scan genre statistics |
| Aggregation | getGenreCooccurrence | Genre pair counting |
| Aggregation | getYearStats | Year range statistics |
| Aggregation | getRatingDistribution | Rating histogram |

### Sample Output

```
======================================================================
IMDB Query Benchmark Report
======================================================================

Summary:
  Total Duration: 4523.45ms
  Cold Start Avg: 245.32ms
  Warm Cache Avg: 42.18ms
  1-Hop Traversal Avg: 35.67ms
  2-Hop Traversal Avg: 156.23ms
  3-Hop Traversal Avg: 312.45ms
  Aggregation Avg: 567.89ms

Detailed Results:
----------------------------------------------------------------------

searchTitles_cold:
  Search titles by name (cold start)
  Cold Start: 189.23ms
  Warm Cache: 28.45ms
  Speedup: 6.65x
  Results: 20
...
```

## GraphDL Schema Definition

```typescript
const imdbSchema = {
  Title: {
    $type: 'imdb:Title',
    $ns: 'imdb',
    $shred: ['type', 'startYear', 'genres'],

    primaryTitle: 'string!',
    originalTitle: 'string!',
    type: 'string!',
    isAdult: 'boolean = false',
    startYear: 'int?',
    endYear: 'int?',
    runtimeMinutes: 'int?',
    genres: 'string[]',

    // Relationships
    directors: '[<- Person.directed]',
    writers: '[<- Person.wrote]',
    cast: '[<- Person.actedIn]',
    parentSeries: '-> Title.episodes',
    episodes: '[<- Title.parentSeries]',
    alternateTitles: '[<- AlternateTitle.title]',
    rating: '<- Rating.title',
    genreList: '[<- Genre.titles]',
    sequels: '[<- Title.prequelOf]',
    remakes: '[<- Title.remakeOf]',
    spinoffs: '[<- Title.spinoffOf]',
  },

  Person: {
    $type: 'imdb:Person',
    $ns: 'imdb',
    $shred: ['birthYear', 'professions'],

    name: 'string!',
    birthYear: 'int?',
    deathYear: 'int?',
    professions: 'string[]',

    actedIn: '[-> Title.cast]',
    directed: '[-> Title.directors]',
    wrote: '[-> Title.writers]',
    knownFor: '[~> Title]',
    collaborators: '[~> Person]',
    frequentCollaborators: '[~> Person]',
  },

  AlternateTitle: {
    $type: 'imdb:AlternateTitle',
    $ns: 'imdb',
    $shred: ['region', 'language'],

    title: '-> Title',
    ordering: 'int!',
    localizedTitle: 'string!',
    region: 'string?',
    language: 'string?',
    types: 'string[]',
    attributes: 'string[]',
    isOriginalTitle: 'boolean = false',
  },

  Rating: {
    $type: 'imdb:Rating',
    $ns: 'imdb',

    title: '-> Title',
    averageRating: 'float!',
    numVotes: 'int!',
  },

  Genre: {
    $type: 'imdb:Genre',
    $ns: 'imdb',

    name: 'string!',
    titles: '[~> Title]',
    titleCount: 'int?',
    averageRating: 'float?',
  },

  PersonCollaboration: {
    $type: 'imdb:PersonCollaboration',
    $ns: 'imdb',

    person1: '-> Person',
    person2: '-> Person',
    collaborationType: 'string!',
    sharedTitleCount: 'int!',
    firstCollabYear: 'int?',
    lastCollabYear: 'int?',
  },

  CareerMilestone: {
    $type: 'imdb:CareerMilestone',
    $ns: 'imdb',

    person: '-> Person',
    title: '-> Title',
    year: 'int!',
    milestoneType: 'string!',
    role: 'string!',
  }
}
```

## Performance Considerations

### Streaming

The loader uses Node.js streams to process data without loading it all into memory:

```typescript
// Stream -> Gunzip -> TSV Parser -> Batch Accumulator -> Parquet Writer
await pipeline(
  stream,
  createGunzip(),
  new TSVParser<Row>(),
  async function* (source) {
    for await (const row of source) {
      await accumulator.add(parseRow(row))
    }
  }
)
```

### Batching

Rows are accumulated in batches (default 50,000) before writing to Parquet:

```typescript
const IMDB_CONFIG = {
  rowGroupSize: 100_000,  // Parquet row group size
  batchSize: 50_000,      // Rows to buffer before writing
  partSize: 10 * 1024 * 1024,  // Multipart upload part size
}
```

### Partitioning

Data is partitioned for efficient filtering:
- **Titles**: Partitioned by `type` (movie, tvSeries, etc.)
- **Alternate Titles**: Partitioned by `region` (US, JP, FR, etc.)
- **Relationships**: Partitioned by `rel_type` (acted_in, directed, etc.)

This reduces I/O by orders of magnitude for filtered queries.

### Caching

Query results benefit from cache warming:
- Cold start queries may take 100-300ms
- Subsequent queries (warm cache) typically 20-50ms
- 5-10x speedup from caching

### Extended Data Loading

The `loadExtended()` function computes derived relationships:
- **Person Collaborations**: O(n^2) per title, can take 10-30 minutes
- **Career Milestones**: Requires scanning all principals + ratings

Run these during off-peak hours or as a batch job.

## Data License

IMDB datasets are provided for non-commercial use only. See:
https://developer.imdb.com/non-commercial-datasets/

When using this data:
1. Provide attribution to IMDB
2. Use only for personal/educational purposes
3. Do not redistribute the raw data
