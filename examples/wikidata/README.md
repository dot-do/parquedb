# Wikidata to ParqueDB

This example demonstrates loading [Wikidata](https://www.wikidata.org/) into ParqueDB, handling the massive knowledge graph with streaming and efficient partitioning.

## Overview

Wikidata is a free, collaborative, multilingual knowledge base containing:
- **100M+ Items** (Q-numbers) - entities like people, places, concepts
- **10K+ Properties** (P-numbers) - relationships and attributes
- **1B+ Claims** - statements linking items via properties
- **Qualifiers** - metadata on claims (dates, sources, etc.)

The full JSON dump is 100GB+ compressed, so this example provides both:
1. **Full streaming load** - process the entire dump with minimal memory
2. **Subset extraction** - filter specific entity types (humans, locations, etc.)

## Wikidata Dumps

Download from: https://dumps.wikimedia.org/wikidatawiki/entities/

| File | Size | Description |
|------|------|-------------|
| `latest-all.json.bz2` | ~100GB | Full dump (compressed) |
| `latest-all.json.gz` | ~130GB | Full dump (gzip) |
| `latest-lexemes.json.bz2` | ~2GB | Lexicographical data only |

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Download a Dump

```bash
# Full dump (WARNING: 100GB+)
curl -O https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2

# Or use a sample for testing
curl -O https://dumps.wikimedia.org/wikidatawiki/entities/latest-lexemes.json.bz2
```

### 3. Run the Loader

```bash
# Full streaming load
npx tsx examples/wikidata/load.ts ./latest-all.json.bz2 ./wikidata-db

# Extract subset (e.g., all humans)
npx tsx examples/wikidata/subset.ts ./latest-all.json.bz2 ./humans-db --type Q5

# Extract locations
npx tsx examples/wikidata/subset.ts ./latest-all.json.bz2 ./locations-db --type Q515,Q6256
```

### 4. Run Queries

```bash
npx tsx examples/wikidata/queries.ts ./wikidata-db
```

## Data Model

### Wikidata Structure

```json
{
  "id": "Q42",
  "type": "item",
  "labels": {
    "en": { "language": "en", "value": "Douglas Adams" }
  },
  "descriptions": {
    "en": { "language": "en", "value": "English writer and humourist" }
  },
  "aliases": {
    "en": [{ "language": "en", "value": "Douglas Noel Adams" }]
  },
  "claims": {
    "P31": [{ "mainsnak": { "datavalue": { "value": { "id": "Q5" } } } }],
    "P569": [{ "mainsnak": { "datavalue": { "value": { "time": "+1952-03-11T00:00:00Z" } } } }]
  },
  "sitelinks": {
    "enwiki": { "site": "enwiki", "title": "Douglas Adams" }
  }
}
```

### ParqueDB Mapping

#### Items (wikidata/items)

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Q-number (e.g., "Q42") |
| `type` | string | Entity type from P31 (e.g., "human", "city") |
| `labels` | variant | Multi-language labels |
| `descriptions` | variant | Multi-language descriptions |
| `aliases` | variant | Multi-language aliases |
| `sitelinks` | variant | Wikipedia/Wikimedia links |

#### Properties (wikidata/properties)

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | P-number (e.g., "P31") |
| `datatype` | string | Expected value type |
| `labels` | variant | Multi-language labels |
| `descriptions` | variant | Multi-language descriptions |

#### Claims (wikidata/claims)

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Claim GUID |
| `item_id` | string | Subject Q-number |
| `property_id` | string | Predicate P-number |
| `value` | variant | Object value |
| `rank` | string | preferred/normal/deprecated |
| `qualifiers` | variant | Qualifier map |
| `references` | variant | Source references |

## File Layout

```
wikidata-db/
├── _meta/
│   ├── manifest.json
│   └── schema.json
├── data/
│   ├── items/
│   │   ├── human/
│   │   │   └── data.0001.parquet    # ~1M humans per file
│   │   ├── location/
│   │   │   └── data.0001.parquet
│   │   └── other/
│   │       └── data.0001.parquet
│   ├── properties/
│   │   └── data.parquet             # ~10K properties
│   └── claims/
│       ├── P31/                     # Partitioned by property
│       │   └── data.0001.parquet
│       ├── P279/
│       │   └── data.0001.parquet
│       └── other/
│           └── data.0001.parquet
├── rels/
│   ├── forward/
│   │   └── claims.parquet           # item -> item via property
│   └── reverse/
│       └── claims.parquet           # reverse lookups
└── indexes/
    ├── fts/
    │   └── items/                   # Full-text on labels
    └── secondary/
        └── items.type.idx.parquet   # Index by type
```

## Partitioning Strategy

### Items by Type

Items are partitioned by their P31 (instance of) value:
- `human/` - P31=Q5 (10M+ people)
- `location/` - P31=Q515 (cities), Q6256 (countries), etc.
- `organization/` - P31=Q43229 (organizations)
- `creative_work/` - P31=Q17537576 (creative works)
- `other/` - Everything else

### Claims by Property

High-cardinality properties get their own partitions:
- `P31/` - instance of (~100M claims)
- `P279/` - subclass of (~5M claims)
- `P17/` - country (~10M claims)
- `P131/` - located in (~20M claims)
- `other/` - All other properties

## Memory-Efficient Streaming

The loader uses streaming JSON parsing with backpressure:

```typescript
// Process 100GB+ with ~256MB memory
const stream = createReadStream('latest-all.json.bz2')
  .pipe(createBunzip2())
  .pipe(new WikidataParser())
  .pipe(new BatchWriter(db, { batchSize: 10000 }))
```

### Chunked/Multipart Uploads

For large Parquet files, the loader uses multipart uploads:

```typescript
const upload = await storage.createMultipartUpload(path)
for (const chunk of chunks) {
  await upload.uploadPart(partNumber++, chunk)
}
await upload.complete(parts)
```

## Common Entity Types

| Q-ID | Type | Count | Description |
|------|------|-------|-------------|
| Q5 | Human | ~10M | People |
| Q515 | City | ~500K | Cities |
| Q6256 | Country | ~200 | Countries |
| Q7889 | Video game | ~70K | Games |
| Q11424 | Film | ~300K | Movies |
| Q571 | Book | ~1M | Books |
| Q5633421 | Scientific article | ~40M | Papers |

## Common Properties

| P-ID | Property | Count | Description |
|------|----------|-------|-------------|
| P31 | instance of | ~100M | Type declaration |
| P279 | subclass of | ~5M | Type hierarchy |
| P17 | country | ~10M | Country association |
| P131 | located in | ~20M | Geographic containment |
| P625 | coordinate location | ~10M | Lat/long |
| P569 | date of birth | ~5M | Birth dates |
| P570 | date of death | ~3M | Death dates |

## Example Queries

### Find all humans born in 1950

```typescript
const humans = await db.Items.find({
  type: 'human',
  'claims.P569.value.time': { $startsWith: '+1950' }
})
```

### Find cities in a country

```typescript
const germanCities = await db.Items.find({
  type: 'city',
  'claims.P17': { $contains: 'Q183' }  // Germany
})
```

### Graph traversal: person's works

```typescript
const works = await db.Claims.find({
  item_id: 'Q42',  // Douglas Adams
  property_id: 'P800'  // notable work
})
```

### Full-text search on labels

```typescript
const results = await db.Items.find({
  $text: { $search: 'douglas adams', $language: 'en' }
})
```

## Performance Characteristics

| Operation | Time | Notes |
|-----------|------|-------|
| Full load | ~8-12 hours | With bz2 decompression |
| Subset extraction (humans) | ~2-3 hours | 10M entities |
| Point lookup by Q-ID | <10ms | Using Parquet statistics |
| Type scan (all humans) | ~30s | Full partition scan |
| FTS query | <100ms | Using search index |

## Tips for Large Datasets

1. **Use bz2 format** - Better compression than gzip
2. **Increase batch size** - `--batch-size 50000` for faster loads
3. **Use SSD storage** - Random writes benefit from SSD
4. **Monitor memory** - Watch for backpressure issues
5. **Partition aggressively** - More partitions = faster queries

## References

- [Wikidata Data Model](https://www.mediawiki.org/wiki/Wikibase/DataModel)
- [Wikidata Query Service](https://query.wikidata.org/)
- [JSON Dump Format](https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON)
