# ParqueDB Examples

This directory contains examples demonstrating how to use ParqueDB with real-world datasets. Each example showcases different aspects of working with large-scale data: streaming, partitioning, graph relationships, and memory-efficient processing.

## Quick Start

The fastest way to get started is to use the pre-generated example datasets:

```bash
# List available datasets
node scripts/load-examples.mjs --list

# Load all sample datasets (creates data in data-v3/)
node scripts/load-examples.mjs

# Load a specific dataset
node scripts/load-examples.mjs --dataset imdb-1m

# Verify existing datasets
node scripts/load-examples.mjs --verify

# Force regeneration
node scripts/load-examples.mjs --force
```

### Pre-loaded Datasets

The following datasets are available in `data-v3/`:

| Dataset | Description | Rows | Size |
|---------|-------------|------|------|
| `imdb-1m` | 1M movie/TV titles with cast | 3.5M | ~88MB |
| `imdb` | 100K sample IMDB data | 350K | ~10MB |
| `onet` | O*NET occupational data (sample) | ~1K | ~1MB |
| `onet-full` | Complete O*NET database | ~100K | ~10MB |
| `unspsc` | UNSPSC taxonomy (sample) | ~250 | ~50KB |
| `unspsc-full` | Complete UNSPSC taxonomy | ~75K | ~5MB |

### Dataset Schema

All datasets use the dual-variant architecture with indexed columns:

- `$id` - Entity identifier (e.g., `title:tt0000001`)
- `$index_*` - Indexed columns for efficient querying
- `name` - Human-readable name
- `$data` - Full entity data as JSON

## Examples

| Example | Description | Data Size | Key Concepts |
|---------|-------------|-----------|--------------|
| [ONET](./onet/) | US Department of Labor occupational database | ~1GB | Entity relationships, graph traversal |
| [UNSPSC](./unspsc/) | UN product/service classification taxonomy | ~50MB | Hierarchical data, tree traversal |
| [IMDB](./imdb/) | Movies, TV shows, and people graph | ~3GB | Bipartite graphs, partitioning by type |
| [Wiktionary](./wiktionary/) | Dictionary entries from Wiktionary | ~10GB | Streaming JSONL, multi-language |
| [Wikidata](./wikidata/) | Wikimedia knowledge graph | ~100GB | Massive datasets, subset extraction |
| [Common Crawl](./commoncrawl/) | Web host-level link graph | ~1TB | Graph metrics, TLD partitioning |

## ONET

The O\*NET occupational database contains standardized descriptors for ~1,000 occupations including skills, knowledge, abilities, and tasks.

```bash
# Download and load O*NET database
npx tsx examples/onet/load.ts

# Run example queries
npx tsx examples/onet/queries.ts
```

**Data model:** Occupations linked to Skills, Knowledge, Abilities, and Tasks through rating relationships with importance/level scores.

## UNSPSC

United Nations Standard Products and Services Code - a hierarchical taxonomy for procurement classification.

```bash
# Generate sample data
npx tsx examples/unspsc/load.ts --generate-sample -o ./data/unspsc-sample.csv

# Load to local storage
npx tsx examples/unspsc/load.ts -i ./data/unspsc-sample.csv -o ./output -v
```

**Data model:** Four-level hierarchy: Segment > Family > Class > Commodity (2/4/6/8-digit codes).

## IMDB

Movies, TV shows, episodes, and people with their relationships (acted in, directed, wrote).

```bash
# Load IMDB dataset (downloads ~3GB compressed)
npx tsx examples/imdb/load.ts --bucket my-bucket --prefix imdb
```

**Data model:** Bipartite graph between People and Titles with role metadata.

## Wiktionary

Dictionary entries with definitions, pronunciations, translations, and semantic relationships.

```bash
# Load English Wiktionary (~2GB)
npx tsx examples/wiktionary/load.ts https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl
```

**Data model:** Words with Definitions, Pronunciations, Translations, RelatedWords, and Etymologies.

## Wikidata

Wikimedia's knowledge graph with 100M+ entities and 1B+ claims.

```bash
# Full streaming load (100GB+, ~8-12 hours)
npx tsx examples/wikidata/load.ts ./latest-all.json.bz2 ./wikidata-db

# Extract subset (e.g., all humans)
npx tsx examples/wikidata/subset.ts ./latest-all.json.bz2 ./humans-db --type Q5
```

**Data model:** Items (Q-entities), Properties (P-entities), and Claims linking them.

## Common Crawl Host Graph

Web-scale host-level link graph with 250M+ hosts and 10B+ edges.

```bash
# Load a crawl (streams TB+ data)
npx tsx examples/commoncrawl/load.ts --crawl cc-main-2025-oct-nov-dec

# Compute graph metrics (PageRank, HITS)
npx tsx examples/commoncrawl/metrics.ts --tld com
```

**Data model:** Hosts partitioned by TLD, bidirectional link indexes, graph metrics.

## Common Patterns

### Streaming Large Datasets

All examples use Node.js streams to process data without loading entire files into memory:

```typescript
import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import { pipeline } from 'node:stream/promises'
import { createGunzip } from 'node:zlib'

// Stream -> Decompress -> Parse -> Transform -> Write
async function* parseFile(filePath: string) {
  const rl = createInterface({
    input: createReadStream(filePath)
  })
  for await (const line of rl) {
    yield parseRow(line)
  }
}

// With gzip decompression
await pipeline(
  createReadStream('data.tsv.gz'),
  createGunzip(),
  new TSVParser(),
  async function* (source) {
    for await (const row of source) {
      await processRow(row)
    }
  }
)
```

### Multipart Uploads to R2

For large Parquet files, use multipart uploads to avoid memory issues:

```typescript
const PART_SIZE = 10 * 1024 * 1024  // 10MB parts

async function uploadLargeFile(bucket: R2Bucket, key: string, data: Uint8Array) {
  if (data.length < 5 * 1024 * 1024) {
    // Small file: direct upload
    await bucket.put(key, data)
    return
  }

  // Large file: multipart upload
  const upload = await bucket.createMultipartUpload(key)
  const parts: R2UploadedPart[] = []

  for (let i = 0; i < data.length; i += PART_SIZE) {
    const chunk = data.slice(i, i + PART_SIZE)
    const part = await upload.uploadPart(parts.length + 1, chunk)
    parts.push(part)
  }

  await upload.complete(parts)
}
```

### Partitioning Strategies

Choose partitioning based on query patterns:

```typescript
// By type (IMDB: movies vs TV shows)
// Enables efficient queries like "all movies from 2020"
const path = `titles/type=${title.type}/data.parquet`

// By TLD (Common Crawl: .com, .org, .edu)
// Natural data locality for web graph queries
const path = `hosts/tld=${host.tld}/data.parquet`

// By first letter (Wiktionary: alphabetical)
// Enables prefix search optimization
const path = `words/lang=${lang}/letter=${word[0]}/data.parquet`

// By property (Wikidata: high-cardinality properties)
// Dedicated partitions for frequently queried relationships
const path = `claims/${isHighCardinality(prop) ? prop : 'other'}/data.parquet`
```

### Memory-Efficient Processing

Batch records before writing to minimize memory usage:

```typescript
const BATCH_SIZE = 50_000
const ROW_GROUP_SIZE = 100_000

class BatchWriter<T> {
  private buffer: T[] = []

  async add(record: T): Promise<void> {
    this.buffer.push(record)
    if (this.buffer.length >= BATCH_SIZE) {
      await this.flush()
    }
  }

  async flush(): Promise<void> {
    if (this.buffer.length === 0) return

    const parquet = await writeParquet(this.buffer, schema, {
      rowGroupSize: ROW_GROUP_SIZE
    })
    await this.storage.put(this.getNextPath(), parquet)
    this.buffer = []
  }
}
```

### Progress Monitoring

Track progress for long-running loads:

```typescript
interface LoadProgress {
  phase: 'downloading' | 'parsing' | 'transforming' | 'writing'
  rowsProcessed: number
  bytesProcessed: number
  elapsedMs: number
}

await loader.load((progress: LoadProgress) => {
  const rate = progress.rowsProcessed / (progress.elapsedMs / 1000)
  console.log(`[${progress.phase}] ${progress.rowsProcessed} rows (${rate.toFixed(0)}/sec)`)
})
```

## Storage Requirements

| Example | Raw Data | Parquet Output | Notes |
|---------|----------|----------------|-------|
| ONET | ~1GB | ~100MB | High compression on text |
| UNSPSC | ~10MB | ~2MB | Small dataset |
| IMDB | ~3GB | ~500MB | Partitioned by type |
| Wiktionary | ~10GB | ~500MB | Per-language partitions |
| Wikidata | ~100GB | ~20GB | Subset extraction recommended |
| Common Crawl | ~1TB | ~100GB | TLD partitions, dual indexes |

## License

Example code is MIT licensed. Individual datasets have their own licenses:

- **ONET**: Creative Commons Attribution 4.0
- **UNSPSC**: GS1 US licensing terms
- **IMDB**: Non-commercial use only
- **Wiktionary**: CC BY-SA 3.0
- **Wikidata**: CC0 Public Domain
- **Common Crawl**: Public domain (terms of use apply)
