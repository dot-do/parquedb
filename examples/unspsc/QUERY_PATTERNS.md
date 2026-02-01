# UNSPSC Query Patterns for ParqueDB

This document catalogs real-world query patterns for the UNSPSC taxonomy in ParqueDB, designed for production applications in procurement, e-commerce, and supply chain management.

## ParqueDB Storage Architecture

ParqueDB uses a dual Variant architecture with shredded index columns:

```
| $id | $index_code | $index_segmentCode | $index_familyCode | $index_classCode | $index_isActive | $data |
```

- **`$id`**: Primary key (e.g., `commodities/43101501`)
- **`$index_*`**: Shredded columns for indexed fields - enable predicate pushdown via Parquet min/max statistics
- **`$data`**: Variant column containing remaining fields (title, description, etc.)

### Index Configuration from Schema

| Field | Index Type | Shredded | Purpose |
|-------|-----------|----------|---------|
| `code` | unique | Yes | Primary lookup by UNSPSC code |
| `title` | fts | No | Full-text search on titles |
| `description` | fts | No | Full-text search on descriptions |
| `segmentCode` | true | Yes | Filter commodities by segment |
| `familyCode` | true | Yes | Filter by family |
| `classCode` | true | Yes | Filter by class |
| `isActive` | boolean | Yes | Filter active/inactive codes |

---

## Query Pattern Summary

| # | Use Case | Query Pattern | Index Used | Selectivity | Stats Pushdown |
|---|----------|---------------|------------|-------------|----------------|
| 1 | Code Lookup | `{ code: "43101501" }` | $index_code (unique) | Very High | Yes |
| 2 | Prefix Search | `{ code: { $startsWith: "4310" } }` | $index_code | Medium | Partial |
| 3 | Segment Filter | `{ segmentCode: "43" }` | $index_segmentCode | Low | Yes |
| 4 | Family Filter | `{ familyCode: "4310" }` | $index_familyCode | Medium | Yes |
| 5 | Class Filter | `{ classCode: "431015" }` | $index_classCode | High | Yes |
| 6 | Active Only | `{ isActive: true }` | $index_isActive | Low | Yes |
| 7 | Text Search | `{ $text: { $search: "computer" } }` | FTS (title) | Medium | No |
| 8 | Multi-Segment | `{ segmentCode: { $in: ["43", "44"] } }` | $index_segmentCode | Low | Yes |
| 9 | Hierarchy Join | `{ familyCode: "4310", $type: "Class" }` | $index_familyCode | High | Yes |
| 10 | Range Query | `{ code: { $gte: "43100000", $lt: "43200000" } }` | $index_code | Medium | Yes |
| 11 | Compound Filter | `{ segmentCode: "43", isActive: true }` | Both indexes | Medium | Yes |
| 12 | Exclude Pattern | `{ segmentCode: "43", classCode: { $ne: "431015" } }` | $index_segmentCode | Medium | Partial |
| 13 | Vendor Mapping | `{ code: { $in: [...vendorCodes] } }` | $index_code | High | Yes |
| 14 | Category Tree | `{ segmentCode: "43" }` + sort | $index_segmentCode | Low | Yes |
| 15 | Sibling Query | `{ classCode: "431015", code: { $ne: currentCode } }` | $index_classCode | High | Yes |
| 16 | Cross-Segment | `{ $or: [{ segmentCode: "43" }, { segmentCode: "81" }] }` | $index_segmentCode | Low | Partial |
| 17 | Deprecation Check | `{ isActive: false, segmentCode: "43" }` | Both indexes | High | Yes |
| 18 | Bulk Validate | `{ code: { $in: [...1000codes] } }` | $index_code | Very High | Yes |
| 19 | FTS + Hierarchy | `{ $text: {...}, segmentCode: "43" }` | FTS + $index | Medium | Partial |
| 20 | Count by Level | `{ $type: "Commodity", segmentCode: "43" }` | $index_segmentCode | Low | Yes |

---

## Detailed Query Patterns by Use Case

### 1. Procurement Systems (SAP Ariba, Coupa)

#### Pattern 1.1: Commodity Code Validation
**Business Question**: Is this UNSPSC code valid and active?

```typescript
// Direct lookup by exact code
const commodity = await db.Commodities.findOne({
  code: "43101501",
  isActive: true
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_code` (unique), `$index_isActive` |
| **Selectivity** | Very High (single row) |
| **Row Groups Scanned** | 1 (via min/max on code) |
| **Stats Pushdown** | Yes - code min/max eliminates most row groups |

#### Pattern 1.2: Bulk PO Line Validation
**Business Question**: Are all 500 line items on this PO using valid UNSPSC codes?

```typescript
const codes = poLines.map(line => line.unspscCode)
const validCodes = await db.Commodities.find({
  code: { $in: codes },
  isActive: true
}, {
  project: { code: 1, title: 1 }  // Minimal projection
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_code` with $in optimization |
| **Selectivity** | Very High (exact matches) |
| **Row Groups Scanned** | Proportional to code distribution |
| **Stats Pushdown** | Yes - each code checked against row group min/max |

#### Pattern 1.3: Category Spend Analysis
**Business Question**: What commodities are we purchasing in the IT segment?

```typescript
const itSpend = await db.Commodities.find({
  segmentCode: "43",  // IT Broadcasting & Telecommunications
  isActive: true
}, {
  sort: { code: 1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_segmentCode`, `$index_isActive` |
| **Selectivity** | Low (~5-10% of total commodities) |
| **Row Groups Scanned** | All groups containing segment 43 |
| **Stats Pushdown** | Yes - segment code is sortable, many groups eliminated |

#### Pattern 1.4: Contract Category Mapping
**Business Question**: Find all commodities in software and services families for a new IT contract.

```typescript
const contractCategories = await db.Commodities.find({
  familyCode: { $in: ["4311", "4313"] },  // Software, IT Services
  isActive: true
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_familyCode` |
| **Selectivity** | Medium (2 families ~ 200-500 commodities) |
| **Row Groups Scanned** | Groups containing either family |
| **Stats Pushdown** | Yes - family code ranges are contiguous |

---

### 2. E-Commerce Product Categorization

#### Pattern 2.1: Typeahead Search
**Business Question**: Find categories matching user input "lap" for autocomplete.

```typescript
const suggestions = await db.Commodities.find({
  $text: { $search: "lap" },
  isActive: true
}, {
  limit: 10,
  project: { code: 1, title: 1, classCode: 1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | FTS index on title |
| **Selectivity** | Medium (depends on term frequency) |
| **Row Groups Scanned** | All (FTS requires full scan of inverted index) |
| **Stats Pushdown** | No - FTS operates on separate index structure |

#### Pattern 2.2: Category Drill-Down
**Business Question**: User clicked on "Computer Equipment" family, show all classes.

```typescript
const classes = await db.Classes.find({
  familyCode: "4310",
  isActive: true
}, {
  sort: { code: 1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_familyCode` |
| **Selectivity** | High (~10-20 classes per family) |
| **Row Groups Scanned** | 1-2 row groups (classes are sorted by code) |
| **Stats Pushdown** | Yes - excellent for range queries within family |

#### Pattern 2.3: Product-to-Category Mapping
**Business Question**: Find the best UNSPSC category for a product titled "MacBook Pro 16 inch".

```typescript
const matches = await db.Commodities.find({
  $text: {
    $search: "MacBook Pro laptop notebook computer",
    $language: "en"
  },
  segmentCode: "43",  // Constrain to IT
  isActive: true
}, {
  limit: 5,
  sort: { $textScore: -1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | FTS + `$index_segmentCode` |
| **Selectivity** | High (FTS + segment filter) |
| **Row Groups Scanned** | FTS lookup, then filter by segment |
| **Stats Pushdown** | Partial - segment filter uses pushdown |

#### Pattern 2.4: Breadcrumb Generation
**Business Question**: Build navigation breadcrumb for commodity 43101501.

```typescript
// Parallel fetches for each level
const [segment, family, cls, commodity] = await Promise.all([
  db.Segments.findOne({ code: "43" }),
  db.Families.findOne({ code: "4310" }),
  db.Classes.findOne({ code: "431015" }),
  db.Commodities.findOne({ code: "43101501" })
])
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_code` (unique) on each collection |
| **Selectivity** | Very High (4 exact lookups) |
| **Row Groups Scanned** | 1 per collection |
| **Stats Pushdown** | Yes - single value equality |

---

### 3. Government Purchasing Portals

#### Pattern 3.1: Compliance Check - Approved Categories
**Business Question**: Is this purchase in an approved UNSPSC segment for this agency?

```typescript
const approvedSegments = ["43", "44", "81"]  // IT, Office, Services
const isApproved = await db.Commodities.findOne({
  code: purchaseCode,
  segmentCode: { $in: approvedSegments },
  isActive: true
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_code`, `$index_segmentCode` |
| **Selectivity** | Very High (exact code + segment validation) |
| **Row Groups Scanned** | 1 |
| **Stats Pushdown** | Yes |

#### Pattern 3.2: Set-Aside Category Identification
**Business Question**: Find all commodities eligible for small business set-asides (certain segments).

```typescript
const setAsideCategories = await db.Commodities.find({
  segmentCode: { $in: ["43", "80", "81"] },  // Segments with set-aside eligibility
  isActive: true
}, {
  project: { code: 1, title: 1, segmentCode: 1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_segmentCode` |
| **Selectivity** | Low (multiple segments) |
| **Row Groups Scanned** | Many (3 segments = ~15-20% of data) |
| **Stats Pushdown** | Yes - segments are well-distributed |

#### Pattern 3.3: NAICS-to-UNSPSC Cross-Reference
**Business Question**: Find UNSPSC codes that might correspond to a given NAICS code.

```typescript
// Assuming cross-reference table exists
const naicsMapping = await db.NAICSMapping.find({
  naicsCode: "541511"  // Custom Computer Programming Services
})

// Then lookup those UNSPSC codes
const unspscCodes = await db.Commodities.find({
  code: { $in: naicsMapping.map(m => m.unspscCode) },
  isActive: true
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_code` |
| **Selectivity** | High (specific code list) |
| **Row Groups Scanned** | Proportional to mapping size |
| **Stats Pushdown** | Yes |

---

### 4. Supply Chain Management

#### Pattern 4.1: Supplier Capability Matching
**Business Question**: Which suppliers can provide commodities in the networking equipment class?

```typescript
const networkingCommodities = await db.Commodities.find({
  classCode: "431210",  // Network equipment
  isActive: true
}, {
  project: { code: 1 }
})

// Then match against supplier catalog
const supplierMatches = await db.SupplierCatalog.find({
  unspscCode: { $in: networkingCommodities.map(c => c.code) }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_classCode` |
| **Selectivity** | High (single class = ~10-50 commodities) |
| **Row Groups Scanned** | 1-2 |
| **Stats Pushdown** | Yes - class codes are sortable ranges |

#### Pattern 4.2: Alternative Product Discovery
**Business Question**: Find substitute products in the same class as a discontinued item.

```typescript
const discontinuedItem = await db.Commodities.findOne({ code: "43101501" })

const alternatives = await db.Commodities.find({
  classCode: discontinuedItem.classCode,
  code: { $ne: "43101501" },
  isActive: true
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_classCode`, `$index_code` |
| **Selectivity** | High (siblings in same class) |
| **Row Groups Scanned** | 1 |
| **Stats Pushdown** | Yes |

#### Pattern 4.3: Category Consolidation Analysis
**Business Question**: Find all commodities across related families for consolidation opportunities.

```typescript
const relatedFamilies = ["4310", "4311", "4312"]  // Hardware, Software, Networking
const consolidationCandidates = await db.Commodities.find({
  familyCode: { $in: relatedFamilies },
  isActive: true
}, {
  sort: { familyCode: 1, classCode: 1, code: 1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_familyCode` |
| **Selectivity** | Medium (3 families) |
| **Row Groups Scanned** | Groups containing these families |
| **Stats Pushdown** | Yes |

---

### 5. Spend Analytics Dashboards

#### Pattern 5.1: Segment Distribution
**Business Question**: How many commodities exist in each segment?

```typescript
const segmentCounts = await db.Commodities.aggregate([
  { $match: { isActive: true } },
  { $group: { _id: "$segmentCode", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```

| Property | Value |
|----------|-------|
| **Index Used** | Full scan with grouping |
| **Selectivity** | Low (all active commodities) |
| **Row Groups Scanned** | All |
| **Stats Pushdown** | Partial - isActive filter can skip some groups |

#### Pattern 5.2: Hierarchy Completeness Check
**Business Question**: Which families have no active commodities?

```typescript
const familiesWithCommodities = await db.Commodities.distinct("familyCode", {
  isActive: true
})

const emptyFamilies = await db.Families.find({
  code: { $nin: familiesWithCommodities },
  isActive: true
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_familyCode`, `$index_code` |
| **Selectivity** | Medium |
| **Row Groups Scanned** | All for distinct, filtered for families |
| **Stats Pushdown** | Yes for second query |

#### Pattern 5.3: Time-Series Deprecation Tracking
**Business Question**: Which codes were deprecated in the last taxonomy update?

```typescript
const recentlyDeprecated = await db.Commodities.find({
  isActive: false,
  updatedAt: { $gte: lastUpdateDate }
}, {
  sort: { segmentCode: 1, code: 1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_isActive`, timestamp from $data |
| **Selectivity** | High (recently changed + inactive) |
| **Row Groups Scanned** | Groups with isActive=false |
| **Stats Pushdown** | Yes for isActive, limited for updatedAt |

---

### 6. Product Catalog Management

#### Pattern 6.1: Catalog Category Assignment
**Business Question**: Assign UNSPSC codes to products based on keyword matching.

```typescript
const productKeywords = "wireless bluetooth headset audio"
const categoryMatches = await db.Commodities.find({
  $text: { $search: productKeywords },
  segmentCode: { $in: ["43", "45"] },  // IT and Audio/Visual
  isActive: true
}, {
  limit: 10,
  sort: { $textScore: -1 }
})
```

| Property | Value |
|----------|-------|
| **Index Used** | FTS + `$index_segmentCode` |
| **Selectivity** | Medium |
| **Row Groups Scanned** | FTS index scan + segment filter |
| **Stats Pushdown** | Partial - segment filter benefits |

#### Pattern 6.2: Category Hierarchy Export
**Business Question**: Export the complete tree structure for a segment.

```typescript
const segment = await db.Segments.findOne({ code: "43" })
const families = await db.Families.find({ segmentCode: "43" }, { sort: { code: 1 } })
const classes = await db.Classes.find({ segmentCode: "43" }, { sort: { code: 1 } })
const commodities = await db.Commodities.find({ segmentCode: "43" }, { sort: { code: 1 } })

// Build tree in memory
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_segmentCode` on each collection |
| **Selectivity** | Low (entire segment) |
| **Row Groups Scanned** | All groups containing segment 43 |
| **Stats Pushdown** | Yes - segment is first 2 digits, excellent for range |

#### Pattern 6.3: Duplicate Detection
**Business Question**: Find commodities with similar titles in the same class.

```typescript
// First get commodities in class
const classItems = await db.Commodities.find({
  classCode: "431015",
  isActive: true
})

// Then check for near-duplicates via application logic or FTS
const potentialDuplicates = classItems.filter((item, idx) =>
  classItems.some((other, otherIdx) =>
    idx !== otherIdx &&
    similarity(item.title, other.title) > 0.9
  )
)
```

| Property | Value |
|----------|-------|
| **Index Used** | `$index_classCode` |
| **Selectivity** | High |
| **Row Groups Scanned** | 1 |
| **Stats Pushdown** | Yes |

---

## Index Recommendations

### Primary Indexes (Shredded Columns)

| Column | Data Type | Cardinality | Use Case |
|--------|-----------|-------------|----------|
| `$index_code` | string | ~75,000 | Exact lookups, prefix search |
| `$index_segmentCode` | string | ~55 | Segment filtering, aggregation |
| `$index_familyCode` | string | ~400 | Family filtering, drill-down |
| `$index_classCode` | string | ~4,000 | Class filtering, sibling queries |
| `$index_isActive` | boolean | 2 | Filter active/deprecated |

### Secondary Indexes

| Index Type | Fields | Use Case |
|------------|--------|----------|
| FTS | title, description | Search, typeahead |
| Bloom Filter | code | Existence checks in bulk validation |

### Row Group Optimization

For optimal predicate pushdown, organize row groups by:

1. **Primary sort**: `segmentCode` (enables segment-level filtering)
2. **Secondary sort**: `code` (enables prefix and range queries)

This ensures:
- Segment queries eliminate 90%+ of row groups
- Family queries eliminate 95%+ of row groups
- Class queries typically read 1-2 row groups

---

## Performance Expectations

| Query Type | Expected Latency | Notes |
|------------|------------------|-------|
| Exact code lookup | < 5ms | Single row group, unique index |
| Prefix search (4+ chars) | < 20ms | Few row groups via min/max |
| Segment filter | 50-200ms | Multiple row groups, depends on segment size |
| Family filter | 20-50ms | 1-3 row groups typically |
| Class filter | < 20ms | 1 row group typically |
| FTS search | 50-500ms | Depends on term frequency |
| Bulk validation (1000 codes) | 100-500ms | Parallelized lookups |
| Full segment export | 200-1000ms | Large result set |

---

## Query Anti-Patterns

### Avoid These Patterns

1. **Unindexed Field Filter**
   ```typescript
   // BAD: description not shredded, requires full Variant scan
   db.Commodities.find({ description: { $contains: "laptop" } })

   // GOOD: Use FTS instead
   db.Commodities.find({ $text: { $search: "laptop" } })
   ```

2. **Leading Wildcard**
   ```typescript
   // BAD: Cannot use index, full scan required
   db.Commodities.find({ code: { $regex: ".*501$" } })

   // GOOD: Use prefix when possible
   db.Commodities.find({ code: { $startsWith: "4310" } })
   ```

3. **Cross-Collection Joins Without Denormalization**
   ```typescript
   // BAD: N+1 queries
   const commodities = await db.Commodities.find({ segmentCode: "43" })
   for (const c of commodities) {
     c.class = await db.Classes.findOne({ code: c.classCode })  // N queries!
   }

   // GOOD: Denormalized data already in commodity
   const commodities = await db.Commodities.find({
     segmentCode: "43"
   })
   // classCode, familyCode, segmentCode already in each commodity
   ```

4. **Large $in Lists**
   ```typescript
   // BAD: Degrades to multiple row group scans
   db.Commodities.find({ code: { $in: [/* 10,000 codes */] } })

   // GOOD: Batch into smaller chunks or use different approach
   const batches = chunk(codes, 100)
   const results = await Promise.all(batches.map(batch =>
     db.Commodities.find({ code: { $in: batch } })
   ))
   ```

---

## Integration Examples

### SAP Ariba Integration

```typescript
// Validate supplier commodity codes during catalog upload
async function validateCatalogItems(items: CatalogItem[]): Promise<ValidationResult[]> {
  const codes = items.map(i => i.unspscCode)

  const validCodes = new Set(
    (await db.Commodities.find({
      code: { $in: codes },
      isActive: true
    }, {
      project: { code: 1 }
    })).map(c => c.code)
  )

  return items.map(item => ({
    lineNumber: item.lineNumber,
    valid: validCodes.has(item.unspscCode),
    code: item.unspscCode
  }))
}
```

### Coupa Spend Classification

```typescript
// Auto-classify invoices by matching description to UNSPSC
async function classifyInvoice(description: string): Promise<Commodity | null> {
  const matches = await db.Commodities.find({
    $text: { $search: description },
    isActive: true
  }, {
    limit: 1,
    sort: { $textScore: -1 }
  })

  return matches[0] || null
}
```

### E-commerce Category Facets

```typescript
// Build category facets for product search results
async function getCategoryFacets(segmentCode: string): Promise<Facet[]> {
  const classes = await db.Classes.find({
    segmentCode,
    isActive: true
  }, {
    project: { code: 1, title: 1 }
  })

  // Count commodities per class
  const counts = await Promise.all(classes.map(async cls => ({
    code: cls.code,
    title: cls.title,
    count: await db.Commodities.count({ classCode: cls.code, isActive: true })
  })))

  return counts.filter(c => c.count > 0)
}
```
