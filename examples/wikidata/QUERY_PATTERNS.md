# Wikidata Query Patterns for ParqueDB

This document catalogs real-world query patterns for the Wikidata knowledge graph stored in ParqueDB. These patterns are derived from production use cases in knowledge panels, virtual assistants, fact-checking tools, and data enrichment services.

## ParqueDB Dual Variant Architecture

ParqueDB uses a `$id | $index_* | $data` column layout:

| Column | Type | Purpose |
|--------|------|---------|
| `$id` | `BYTE_ARRAY` | Primary key (e.g., `items/Q42`) |
| `$index_itemType` | `BYTE_ARRAY (DICT)` | Indexed: entity type category |
| `$index_labelEn` | `BYTE_ARRAY` | Indexed: English label for FTS |
| `$index_propertyId` | `BYTE_ARRAY (DICT)` | Indexed: property ID (claims) |
| `$index_objectId` | `BYTE_ARRAY` | Indexed: target entity ID (claims) |
| `$data` | `BYTE_ARRAY (Variant)` | Full entity data as Variant JSON |

## Query Pattern Summary Table

| # | Pattern | Use Case | Index Columns | Selectivity | Stats Pushdown |
|---|---------|----------|---------------|-------------|----------------|
| 1 | Entity Lookup by ID | Knowledge Panels | `$id` | Very High | Yes |
| 2 | Type-Filtered Listing | Search Results | `$index_itemType` | Medium | Yes |
| 3 | Full-Text Label Search | Autocomplete | `$index_labelEn` (FTS) | Variable | Partial |
| 4 | Claims by Subject | Entity Details | `$index_subjectId` | High | Yes |
| 5 | Claims by Property | Property Analysis | `$index_propertyId` | Medium-Low | Yes |
| 6 | Reverse Relationship Lookup | Backlinks | `$index_objectId` | Variable | Yes |
| 7 | Type + Property Filter | Filtered Search | `$index_itemType`, `$index_propertyId` | Medium | Yes |
| 8 | Geographic Bounding Box | Map Views | `$index_lat`, `$index_lng` | Medium | Yes |
| 9 | Date Range Filter | Historical Queries | `$index_dateOfBirth`, etc. | Medium | Yes |
| 10 | Occupation + Location | People Search | `$index_occupation`, `$index_citizenship` | Medium | Yes |
| 11 | Graph Traversal (1-hop) | Related Entities | `$index_subjectId`, `$index_objectId` | High per hop | Yes |
| 12 | Graph Traversal (2-hop) | Recommendations | Multiple lookups | Medium | Yes |
| 13 | Path Finding | Relationship Discovery | Multiple traversals | Low | No |
| 14 | Instance-of Hierarchy | Type Classification | `$index_propertyId` (P31, P279) | Medium | Yes |
| 15 | Sitelink Lookup | Wikipedia Integration | `$data.sitelinks.*` | High | No |
| 16 | Multi-language Label | Localization | `$data.labels.*` | High | No |
| 17 | Qualifier-Filtered Claims | Precise Facts | `$data.qualifiers.*` | Low | No |
| 18 | Aggregation by Type | Analytics | `$index_itemType` | Full Scan | Yes |
| 19 | Identifier Cross-Reference | Data Enrichment | `$index_imdbId`, `$index_doi`, etc. | Very High | Yes |
| 20 | Vector Similarity | Semantic Search | `$index_embedding` | Top-K | Partial |

---

## Detailed Query Patterns

### 1. Entity Lookup by ID

**Business Question:** "Get all information about Douglas Adams (Q42)"

**Use Case:** Knowledge panels, entity detail pages, virtual assistant fact retrieval

**ParqueDB Query:**
```typescript
const entity = await db.items.get('items/Q42')
// or
const entity = await db.items.findOne({ wikidataId: 'Q42' })
```

**Index Columns:** `$id` (primary key)

**Selectivity:** Very High (single row)

**Stats Pushdown:** Yes - Parquet row group statistics can quickly eliminate groups where min/max don't contain the target ID.

**Notes:** This is the most common query pattern. ULID-based IDs maintain insertion order which helps with row-group locality.

---

### 2. Type-Filtered Entity Listing

**Business Question:** "Show me all cities in the database"

**Use Case:** Browse interfaces, category pages, type-specific search

**ParqueDB Query:**
```typescript
const cities = await db.items.find({
  itemType: 'location',
  '$data.primaryType': 'Q515'  // City type
}, {
  limit: 100,
  sort: { name: 1 }
})
```

**Index Columns:** `$index_itemType`

**Selectivity:** Medium (thousands to millions depending on type)

**Stats Pushdown:** Yes - dictionary encoding on itemType allows efficient row-group pruning.

**Notes:** The loader pre-computes itemType from P31 claims during ingestion, enabling fast type-based queries without joining to claims.

---

### 3. Full-Text Label Search

**Business Question:** "Find all entities matching 'Einstein'"

**Use Case:** Autocomplete, search boxes, entity disambiguation, NLP entity linking

**ParqueDB Query:**
```typescript
const results = await db.items.find({
  $text: { $search: 'Einstein', $language: 'en' }
}, {
  limit: 20
})
```

**Index Columns:** `$index_labelEn` (FTS index)

**Selectivity:** Variable (depends on query specificity)

**Stats Pushdown:** Partial - FTS index provides candidate rows; stats help with post-filtering.

**Notes:** Essential for virtual assistants (Siri, Alexa) and entity linking in NLP pipelines. Consider also indexing aliases for better recall.

---

### 4. Claims by Subject (Forward Relationship)

**Business Question:** "What are all the claims/facts about Barack Obama (Q76)?"

**Use Case:** Entity detail pages, fact sheets, knowledge panel populating

**ParqueDB Query:**
```typescript
const claims = await db.claims.find({
  subjectId: 'Q76'
})
```

**Index Columns:** `$index_subjectId`

**Selectivity:** High (typically 50-500 claims per entity)

**Stats Pushdown:** Yes - row groups are sorted by subjectId for efficient range scans.

**Notes:** Core pattern for building entity profiles. Consider pre-aggregating common claim patterns for popular entities.

---

### 5. Claims by Property

**Business Question:** "Find all 'date of birth' (P569) claims in the database"

**Use Case:** Property analysis, data quality checks, statistical queries

**ParqueDB Query:**
```typescript
const birthDates = await db.claims.find({
  propertyId: 'P569'
}, {
  limit: 1000
})
```

**Index Columns:** `$index_propertyId`

**Selectivity:** Medium-Low (P31 has ~100M claims; P569 has ~5M)

**Stats Pushdown:** Yes - dictionary encoding on propertyId enables efficient pruning.

**Notes:** High-cardinality properties (P31, P279, P17, P131) are partitioned into separate files for better performance.

---

### 6. Reverse Relationship Lookup (Backlinks)

**Business Question:** "What entities link TO the United States (Q30)?"

**Use Case:** Finding related entities, "What links here", influence analysis

**ParqueDB Query:**
```typescript
const backlinks = await db.claims.find({
  objectId: 'Q30'
}, {
  limit: 100
})

// Then resolve subject entities
const subjects = await db.items.find({
  wikidataId: { $in: backlinks.map(c => c.subjectId) }
})
```

**Index Columns:** `$index_objectId`

**Selectivity:** Variable (popular entities like Q30 have millions of backlinks)

**Stats Pushdown:** Yes - requires reverse index sorted by objectId.

**Notes:** This is why ParqueDB maintains both forward and reverse relationship indexes. Essential for graph traversal.

---

### 7. Type + Property Compound Filter

**Business Question:** "Find all humans who were born in Berlin"

**Use Case:** Faceted search, filtered entity discovery

**ParqueDB Query:**
```typescript
// First find humans born in Berlin (Q64)
const birthClaims = await db.claims.find({
  propertyId: 'P19',  // place of birth
  objectId: 'Q64'    // Berlin
})

// Then filter to humans
const humans = await db.items.find({
  wikidataId: { $in: birthClaims.map(c => c.subjectId) },
  itemType: 'human'
})
```

**Index Columns:** `$index_propertyId`, `$index_objectId`, `$index_itemType`

**Selectivity:** Medium (intersection of two medium-selectivity filters)

**Stats Pushdown:** Yes - both stages benefit from statistics.

**Notes:** Compound queries often require multiple index lookups. Consider materialized views for common filter combinations.

---

### 8. Geographic Bounding Box Query

**Business Question:** "Find all entities within 50km of Paris"

**Use Case:** Map views, location-based services, geographic research

**ParqueDB Query:**
```typescript
const nearParis = await db.claims.find({
  propertyId: 'P625',  // coordinate location
  $geo: {
    $near: { lat: 48.8566, lng: 2.3522 },
    $maxDistance: 50000  // 50km in meters
  }
})
```

**Index Columns:** `$index_lat`, `$index_lng` (shredded from coordinate claims)

**Selectivity:** Medium (depends on area size and entity density)

**Stats Pushdown:** Yes - bounding box can eliminate row groups outside the search area.

**Notes:** Wikidata has ~10M coordinate claims. R-tree or geohash indexing significantly improves geo queries.

---

### 9. Date Range Filter

**Business Question:** "Find all humans born between 1900 and 1950"

**Use Case:** Historical research, timeline views, genealogy applications

**ParqueDB Query:**
```typescript
const bornEarly20c = await db.claims.find({
  propertyId: 'P569',  // date of birth
  '$data.value.time': {
    $gte: '+1900-01-01T00:00:00Z',
    $lt: '+1951-01-01T00:00:00Z'
  }
})
```

**Index Columns:** Shredded `$index_birthYear` or timestamp index

**Selectivity:** Medium (~500K-1M matches for 50-year range)

**Stats Pushdown:** Yes - min/max statistics on date columns enable efficient range pruning.

**Notes:** Wikidata dates use ISO 8601 with precision indicators. Consider extracting year as a separate indexed column for faster decade/century queries.

---

### 10. Occupation + Location Filter

**Business Question:** "Find all scientists who are citizens of Germany"

**Use Case:** Professional directories, research tools, demographic analysis

**ParqueDB Query:**
```typescript
// Find scientists (occupation = Q901, Q169470, etc.)
const scientists = await db.claims.find({
  propertyId: 'P106',  // occupation
  objectId: { $in: ['Q901', 'Q169470', 'Q4964182'] }  // scientist types
})

// Filter to German citizens
const germanScientists = await db.claims.find({
  subjectId: { $in: scientists.map(c => c.subjectId) },
  propertyId: 'P27',  // citizenship
  objectId: 'Q183'   // Germany
})
```

**Index Columns:** `$index_propertyId`, `$index_objectId`, `$index_subjectId`

**Selectivity:** Medium (compound filter reduces result set)

**Stats Pushdown:** Yes - each stage benefits from index pushdown.

**Notes:** Multi-hop queries are common in knowledge graph applications. Consider pre-computing common occupation+nationality combinations.

---

### 11. Single-Hop Graph Traversal

**Business Question:** "What are the notable works of Douglas Adams?"

**Use Case:** Related content, recommendation engines, entity expansion

**ParqueDB Query:**
```typescript
const notableWorks = await db.claims.find({
  subjectId: 'Q42',
  propertyId: 'P800'  // notable work
})

// Resolve the work entities
const works = await db.items.find({
  wikidataId: { $in: notableWorks.map(c => c.objectId) }
})
```

**Index Columns:** `$index_subjectId`, `$index_propertyId`

**Selectivity:** High per entity (typically 1-20 notable works)

**Stats Pushdown:** Yes

**Notes:** Single-hop traversals are fast. The key optimization is batching the second lookup.

---

### 12. Two-Hop Graph Traversal

**Business Question:** "Find collaborators of people Douglas Adams worked with"

**Use Case:** Network analysis, recommendation systems, "people you may know"

**ParqueDB Query:**
```typescript
// Hop 1: Find Douglas Adams' collaborators
const directCollabs = await db.claims.find({
  subjectId: 'Q42',
  propertyId: { $in: ['P50', 'P57', 'P58', 'P86'] }  // various collaboration properties
})

// Hop 2: Find their collaborators
const secondDegree = await db.claims.find({
  subjectId: { $in: directCollabs.map(c => c.objectId) },
  propertyId: { $in: ['P50', 'P57', 'P58', 'P86'] }
})

// Resolve and deduplicate
const collaborators = await db.items.find({
  wikidataId: { $in: [...new Set(secondDegree.map(c => c.objectId))] }
})
```

**Index Columns:** `$index_subjectId`, `$index_propertyId`, `$index_objectId`

**Selectivity:** Medium (exponential expansion per hop)

**Stats Pushdown:** Yes per hop

**Notes:** Two-hop queries can return large result sets. Always use limits and consider caching popular traversals.

---

### 13. Path Finding Between Entities

**Business Question:** "How are Einstein and Marie Curie connected?"

**Use Case:** Relationship discovery, story generation, research tools

**ParqueDB Query:**
```typescript
// Bidirectional BFS implementation
async function findPath(db, start: string, end: string, maxDepth = 4) {
  const forward = new Map([[start, [start]]])
  const backward = new Map([[end, [end]]])

  for (let depth = 0; depth < maxDepth / 2; depth++) {
    // Expand forward frontier
    const forwardFrontier = [...forward.keys()].filter(k =>
      forward.get(k)!.length === depth + 1
    )
    const forwardClaims = await db.claims.find({
      subjectId: { $in: forwardFrontier }
    })
    // ... expand and check intersection with backward
  }
  return null  // No path found
}
```

**Index Columns:** `$index_subjectId`, `$index_objectId`

**Selectivity:** Low (requires multiple hops and large intermediate sets)

**Stats Pushdown:** No significant benefit for BFS-style traversals

**Notes:** Path finding is expensive. Consider pre-computing shortest paths for popular entities or using approximations.

---

### 14. Instance-of / Subclass-of Hierarchy

**Business Question:** "Is a 'Tesla Model S' a 'vehicle'?"

**Use Case:** Type checking, ontology navigation, semantic reasoning

**ParqueDB Query:**
```typescript
async function isInstanceOf(db, entity: string, targetType: string): Promise<boolean> {
  // Direct instance check
  const directTypes = await db.claims.find({
    subjectId: entity,
    propertyId: 'P31'  // instance of
  })

  if (directTypes.some(c => c.objectId === targetType)) return true

  // Traverse subclass hierarchy
  const visited = new Set<string>()
  const queue = directTypes.map(c => c.objectId)

  while (queue.length > 0) {
    const current = queue.shift()!
    if (visited.has(current)) continue
    visited.add(current)

    if (current === targetType) return true

    const superclasses = await db.claims.find({
      subjectId: current,
      propertyId: 'P279'  // subclass of
    })
    queue.push(...superclasses.map(c => c.objectId))
  }

  return false
}
```

**Index Columns:** `$index_subjectId`, `$index_propertyId`

**Selectivity:** Medium (type hierarchies are typically 5-15 levels deep)

**Stats Pushdown:** Yes per hop

**Notes:** Type hierarchies are frequently traversed. Consider materializing transitive closures for common types (Q5 human, Q6256 country, etc.).

---

### 15. Sitelink Lookup (Wikipedia Integration)

**Business Question:** "Get the Wikipedia URL for entity Q42"

**Use Case:** Wikipedia integration, citation generation, content linking

**ParqueDB Query:**
```typescript
const entity = await db.items.get('items/Q42')
const enwikiLink = entity.sitelinks?.enwiki

// Result: { site: 'enwiki', title: 'Douglas Adams', url: '...' }
```

**Index Columns:** None required (data access within entity)

**Selectivity:** Very High (single entity lookup)

**Stats Pushdown:** No (sitelinks are in $data Variant)

**Notes:** Sitelinks connect Wikidata to Wikipedia articles. Consider shredding common sitelinks (enwiki, dewiki) for faster access.

---

### 16. Multi-Language Label Lookup

**Business Question:** "Get the German name for entity Q42"

**Use Case:** Localization, multilingual interfaces, translation tools

**ParqueDB Query:**
```typescript
const entity = await db.items.get('items/Q42')
const germanLabel = entity.labels?.de?.value

// For bulk localization
const entities = await db.items.find({ wikidataId: { $in: qids } })
const localizedNames = entities.map(e => ({
  id: e.wikidataId,
  name: e.labels?.de?.value || e.labels?.en?.value || e.name
}))
```

**Index Columns:** None for label access; `$index_labelEn` for search

**Selectivity:** High (depends on initial filter)

**Stats Pushdown:** No (labels are in $data Variant)

**Notes:** Wikidata has labels in 300+ languages. Consider creating language-specific indexes for primary UI languages.

---

### 17. Qualifier-Filtered Claims

**Business Question:** "When was Einstein's Nobel Prize awarded?"

**Use Case:** Precise fact extraction, timeline construction, data verification

**ParqueDB Query:**
```typescript
// Find Nobel Prize claim
const nobelClaims = await db.claims.find({
  subjectId: 'Q937',   // Einstein
  propertyId: 'P166',  // award received
  objectId: 'Q38104'   // Nobel Prize in Physics
})

// Extract the date qualifier
const awardDate = nobelClaims[0]?.qualifiers?.P585?.[0]?.datavalue?.value?.time
// Result: "+1921-00-00T00:00:00Z"
```

**Index Columns:** `$index_subjectId`, `$index_propertyId`, `$index_objectId`

**Selectivity:** Low (qualifiers require scanning claim data)

**Stats Pushdown:** No (qualifiers are in $data Variant)

**Notes:** Qualifiers add crucial context (dates, locations, sources). For frequent qualifier patterns, consider shredding common qualifier properties.

---

### 18. Aggregation by Type

**Business Question:** "How many entities of each type exist in the database?"

**Use Case:** Analytics dashboards, data quality monitoring, capacity planning

**ParqueDB Query:**
```typescript
const typeCounts = await db.items.aggregate([
  { $group: { _id: '$itemType', count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])

// Result: [{ _id: 'human', count: 10234567 }, { _id: 'location', count: 892345 }, ...]
```

**Index Columns:** `$index_itemType`

**Selectivity:** Full scan with group-by

**Stats Pushdown:** Yes - dictionary encoding allows count aggregation from metadata in some cases.

**Notes:** Aggregations benefit significantly from columnar storage. Consider maintaining pre-computed statistics for common aggregations.

---

### 19. External Identifier Cross-Reference

**Business Question:** "Find the Wikidata entity for IMDB ID 'nm0010930'"

**Use Case:** Data enrichment, entity resolution, system integration

**ParqueDB Query:**
```typescript
// Find entity with matching IMDB ID
const imdbClaim = await db.claims.findOne({
  propertyId: 'P345',  // IMDB ID
  '$data.value': 'nm0010930'
})

if (imdbClaim) {
  const entity = await db.items.findOne({ wikidataId: imdbClaim.subjectId })
}
```

**Index Columns:** Shredded `$index_imdbId`, `$index_doi`, `$index_isbn`, etc.

**Selectivity:** Very High (identifiers are unique)

**Stats Pushdown:** Yes (with shredded index columns)

**Notes:** External identifiers are crucial for data enrichment. High-value identifiers (IMDB, DOI, ISBN, ORCID) should be shredded into indexed columns.

---

### 20. Vector Similarity Search

**Business Question:** "Find entities semantically similar to 'artificial intelligence'"

**Use Case:** Semantic search, recommendation, concept exploration

**ParqueDB Query:**
```typescript
// Assuming embeddings are pre-computed and stored
const queryEmbedding = await embed('artificial intelligence')

const similar = await db.items.find({
  $vector: {
    $near: queryEmbedding,
    $k: 20,
    $field: 'embedding',
    $minScore: 0.7
  }
})
```

**Index Columns:** `$index_embedding` (vector index)

**Selectivity:** Top-K (configurable)

**Stats Pushdown:** Partial (vector indexes use specialized structures like HNSW)

**Notes:** Vector search enables semantic queries beyond keyword matching. Requires pre-computing embeddings during ingestion.

---

## Use Case Mapping

### Knowledge Panels (Google, Bing)

| Query Type | Primary Patterns |
|------------|-----------------|
| Entity lookup | #1 Entity Lookup by ID |
| Related facts | #4 Claims by Subject |
| Related entities | #11 Single-Hop Traversal |
| Type classification | #14 Instance-of Hierarchy |
| Wikipedia links | #15 Sitelink Lookup |

### Virtual Assistants (Siri, Alexa, Google Assistant)

| Query Type | Primary Patterns |
|------------|-----------------|
| Entity resolution | #3 Full-Text Label Search |
| Fact retrieval | #1, #4, #17 |
| "Who is..." | #1 + #4 |
| "What is the capital of..." | #7 Type + Property Filter |
| Disambiguation | #3 + #14 |

### Fact-Checking Tools

| Query Type | Primary Patterns |
|------------|-----------------|
| Claim verification | #4, #17 Qualifier-Filtered Claims |
| Source tracing | #17 (references in claims) |
| Date verification | #9 Date Range Filter |
| Entity verification | #19 Identifier Cross-Reference |

### Data Enrichment Services

| Query Type | Primary Patterns |
|------------|-----------------|
| ID mapping | #19 Identifier Cross-Reference |
| Entity augmentation | #1 + #4 |
| Type tagging | #14 Instance-of Hierarchy |
| Relationship discovery | #11, #12 Graph Traversal |

### Academic Research Tools

| Query Type | Primary Patterns |
|------------|-----------------|
| Population queries | #7 Type + Property Filter |
| Historical analysis | #9 Date Range Filter |
| Network analysis | #12, #13 Graph Traversal |
| Statistical aggregation | #18 Aggregation |

### Entity Linking for NLP

| Query Type | Primary Patterns |
|------------|-----------------|
| Candidate generation | #3 Full-Text Search |
| Disambiguation | #1 + #14 + #16 |
| Context matching | #4 Claims by Subject |
| Semantic similarity | #20 Vector Search |

### Genealogy Applications

| Query Type | Primary Patterns |
|------------|-----------------|
| Person lookup | #3, #1 |
| Family relationships | #11 (P22 father, P25 mother, P40 child) |
| Birth/death places | #7 Type + Property |
| Timeline construction | #9 Date Range |
| Ancestor tracing | #12, #13 Multi-hop Traversal |

### Geographic/Political Data

| Query Type | Primary Patterns |
|------------|-----------------|
| Location search | #8 Geographic Bounding Box |
| Administrative hierarchy | #14 (P131 located in) |
| Country facts | #1 + #4 |
| Capital/leader lookup | #7 Type + Property |

---

## Index Recommendations

Based on the query patterns above, here are the recommended shredded index columns:

### Items Collection

| Column | Source | Queries Benefiting |
|--------|--------|-------------------|
| `$index_itemType` | Computed from P31 | #2, #7, #10, #18 |
| `$index_labelEn` | `labels.en.value` | #3 |
| `$index_wikidataId` | `id` | #1 (if not using $id) |

### Claims Collection

| Column | Source | Queries Benefiting |
|--------|--------|-------------------|
| `$index_subjectId` | `subjectId` | #4, #7, #10, #11, #12, #14 |
| `$index_propertyId` | `propertyId` | #5, #7, #10, #11, #14 |
| `$index_objectId` | `objectId` | #6, #7, #10 |
| `$index_rank` | `rank` | All claim queries (filter deprecated) |

### High-Value Identifier Shredding

| Column | Property | Use Case |
|--------|----------|----------|
| `$index_imdbId` | P345 | Movie/TV integration |
| `$index_doi` | P356 | Academic paper linking |
| `$index_isbn` | P212 | Book identification |
| `$index_orcid` | P496 | Researcher identification |
| `$index_viafId` | P214 | Library authority control |

### Geographic Shredding

| Column | Property | Use Case |
|--------|----------|----------|
| `$index_lat` | P625.latitude | Geographic queries |
| `$index_lng` | P625.longitude | Geographic queries |
| `$index_country` | P17 object | Country filtering |

---

## Performance Considerations

### Row-Group Statistics Effectiveness

| Query Type | Stats Benefit | Notes |
|------------|---------------|-------|
| Point lookups | High | Min/max on sorted $id |
| Dictionary columns | High | Distinct value counts |
| Date ranges | High | Min/max on dates |
| Text search | Low | Requires FTS index |
| Variant access | None | Data in blob column |
| Geographic | Medium | Requires spatial index |
| Vector similarity | None | Requires vector index |

### Partitioning Strategy

The Wikidata loader partitions data for optimal query performance:

1. **Items by type category**: `human/`, `location/`, `organization/`, `other/`
2. **Claims by property**: `P31/`, `P279/`, `P17/`, `P131/`, `other/`
3. **Row-group size**: ~100K rows for balanced memory/parallelism

### Caching Recommendations

| Data | TTL | Reason |
|------|-----|--------|
| Popular entities (Q30, Q183, etc.) | Long | Frequently accessed |
| Type hierarchies | Long | Rarely change |
| Property labels | Long | Needed for display |
| Search results | Short | User-specific |
| Aggregations | Medium | Expensive to compute |

---

## References

- [Wikidata SPARQL Query Examples](https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples)
- [Google Knowledge Graph Search API](https://developers.google.com/knowledge-graph)
- [Wikidata Data Model](https://www.mediawiki.org/wiki/Wikibase/DataModel)
- [Knowledge Graphs in AI Applications](https://research.aimultiple.com/knowledge-graph/)
