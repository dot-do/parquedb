# UNSPSC Example for ParqueDB

This example demonstrates loading and querying the **United Nations Standard Products and Services Code (UNSPSC)** taxonomy using ParqueDB.

## Overview

UNSPSC is a hierarchical taxonomy of products and services used globally for procurement and spend analysis. It provides a standardized way to classify goods and services across organizations and systems.

### Hierarchy Structure

```
Segment (2-digit)
└── Family (4-digit)
    └── Class (6-digit)
        └── Commodity (8-digit)
```

**Example:**
```
43 - Information Technology Broadcasting and Telecommunications
└── 4310 - Computer Equipment and Accessories
    └── 431015 - Computers
        └── 43101501 - Notebook computers
        └── 43101502 - Desktop computers
        └── 43101503 - Tablet computers
```

## Files

| File | Description |
|------|-------------|
| `schema.ts` | Type definitions and ParqueDB schema for UNSPSC entities |
| `load.ts` | Data loader for importing UNSPSC from CSV/Excel |
| `queries.ts` | Query examples for hierarchy traversal, search, and relationships |

## Quick Start

### 1. Generate Sample Data

```bash
npx tsx examples/unspsc/load.ts --generate-sample -o ./data/unspsc-sample.csv
```

### 2. Load Data

```bash
# Load to local filesystem (for development)
npx tsx examples/unspsc/load.ts -i ./data/unspsc-sample.csv -o ./output -v

# Load to R2 (production)
# See "Loading to R2" section below
```

### 3. Query the Data

```typescript
import { getHierarchyPath, searchUNSPSC, getChildren } from './queries'

// Get full path for a commodity code
const path = await getHierarchyPath(db, '43101501')
// {
//   segment: { code: '43', title: 'Information Technology...' },
//   family: { code: '4310', title: 'Computer Equipment...' },
//   class: { code: '431015', title: 'Computers' },
//   commodity: { code: '43101501', title: 'Notebook computers' }
// }

// Search across all levels
const results = await searchUNSPSC(db, 'laptop computer')

// Get children of a family
const classes = await getChildren(db, '4310')
```

## Schema

### Entity Types

**Segment** (Top-level category)
```typescript
{
  $type: 'Segment',
  code: '43',                // 2-digit code
  title: 'Information Technology...',
  isActive: true
}
```

**Family** (Subdivision of Segment)
```typescript
{
  $type: 'Family',
  code: '4310',              // 4-digit code
  title: 'Computer Equipment and Accessories',
  segmentCode: '43',         // Parent reference
  isActive: true
}
```

**Class** (Subdivision of Family)
```typescript
{
  $type: 'Class',
  code: '431015',            // 6-digit code
  title: 'Computers',
  familyCode: '4310',        // Parent reference
  segmentCode: '43',         // Denormalized for efficient queries
  isActive: true
}
```

**Commodity** (Specific product/service)
```typescript
{
  $type: 'Commodity',
  code: '43101501',          // 8-digit code
  title: 'Notebook computers',
  classCode: '431015',       // Parent reference
  familyCode: '4310',        // Denormalized
  segmentCode: '43',         // Denormalized
  isActive: true
}
```

### Relationships

```
Segment <-[families]-- Family
Family  <-[classes]--- Class
Class   <-[commodities]- Commodity
```

The schema uses ParqueDB's bidirectional relationship syntax:

```typescript
// In schema definition
Family: {
  segment: '-> Segment.families',  // Forward: Family points to Segment
}

Segment: {
  families: '<- Family.segment[]', // Reverse: Segment sees its Families
}
```

## CSV Format

### Standard UNSPSC Format

```csv
Segment,Segment Title,Family,Family Title,Class,Class Title,Commodity,Commodity Title
43,Information Technology...,4310,Computer Equipment...,431015,Computers,43101501,Notebook computers
```

### Flat Format

```csv
Code,Title,Level,ParentCode,Description
43,Information Technology...,Segment,,
4310,Computer Equipment...,Family,43,
431015,Computers,Class,4310,
43101501,Notebook computers,Commodity,431015,
```

## Query Examples

### Hierarchy Traversal

```typescript
import {
  getHierarchyPath,
  getChildren,
  getParent,
  getAncestors,
  getDescendants,
  getBreadcrumbs
} from './queries'

// Get complete path from segment to commodity
const path = await getHierarchyPath(db, '43101501')

// Get immediate children
const families = await getChildren(db, '43')  // Families in segment 43
const classes = await getChildren(db, '4310') // Classes in family 4310

// Navigate upward
const parent = await getParent(db, '43101501')     // Returns Class
const ancestors = await getAncestors(db, '43101501') // [Class, Family, Segment]

// Get all descendants
const allItems = await getDescendants(db, '43', { maxDepth: 3 })

// Build navigation breadcrumbs
const crumbs = await getBreadcrumbs(db, '43101501')
// Information Technology > Computer Equipment > Computers > Notebook computers
```

### Search

```typescript
import { searchUNSPSC, findByCodePrefix } from './queries'

// Full-text search
const results = await searchUNSPSC(db, 'computer laptop', {
  limit: 20,
  types: ['Class', 'Commodity']  // Optional: filter by level
})

// Find by code prefix
const items = await findByCodePrefix(db, '4310')  // All 4310.xxxx codes
```

### Tree Building

```typescript
import { buildTree, getSegmentsWithCounts } from './queries'

// Build a tree for navigation UI
const tree = await buildTree(db, '43', { depth: 2 })
// {
//   code: '43',
//   title: 'Information Technology...',
//   type: 'Segment',
//   children: [
//     { code: '4310', title: '...', children: [...] },
//     { code: '4311', title: '...', children: [...] }
//   ]
// }

// Get segments with counts for overview
const segments = await getSegmentsWithCounts(db)
// [{ code: '43', title: '...', familyCount: 15, commodityCount: 2500 }, ...]
```

### Related Items

```typescript
import { findRelated } from './queries'

// Find sibling commodities (same class)
const siblings = await findRelated(db, '43101501', {
  relationship: 'siblings'
})

// Find cousin commodities (same family, different class)
const cousins = await findRelated(db, '43101501', {
  relationship: 'cousins'
})

// Find all commodities in same segment
const sameSegment = await findRelated(db, '43101501', {
  relationship: 'same-segment',
  limit: 100
})
```

## Loading to R2

For production use with Cloudflare R2:

```typescript
import { loadUNSPSC } from './load'

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const formData = await request.formData()
    const file = formData.get('file') as File

    const result = await loadUNSPSC({
      inputPath: '/tmp/unspsc.csv',  // Or use streaming
      bucket: env.R2_BUCKET,
      verbose: true
    })

    return Response.json(result)
  }
}
```

## Storage Layout

After loading, the data is stored in ParqueDB's standard layout:

```
{bucket}/
├── _meta/
│   ├── manifest.json          # Database metadata
│   └── schema.json            # UNSPSC schema
├── data/
│   ├── segments/
│   │   └── data.parquet       # Segment entities
│   ├── families/
│   │   └── data.parquet       # Family entities
│   ├── classes/
│   │   └── data.parquet       # Class entities
│   └── commodities/
│       └── data.parquet       # Commodity entities
└── rels/
    ├── forward/
    │   ├── families.parquet   # Family -> Segment
    │   ├── classes.parquet    # Class -> Family
    │   └── commodities.parquet # Commodity -> Class
    └── reverse/
        └── segments.parquet   # Segment <- Families
```

## Performance Considerations

1. **Denormalized Parent References**: Each entity stores references to all ancestors (not just immediate parent) for efficient filtering without joins.

2. **Separate Namespaces**: Each level is stored in its own namespace/parquet file for efficient level-specific queries.

3. **Indexed Fields**: `code`, `segmentCode`, `familyCode`, `classCode` are indexed for fast lookups.

4. **Full-Text Search**: `title` and `description` fields are indexed for FTS to enable search across the taxonomy.

## Data Sources

Official UNSPSC data can be obtained from:
- [UNSPSC.org](https://www.unspsc.org/) - Official source (subscription required)
- [GS1 US](https://www.gs1us.org/resources/standards/unspsc) - GS1 member access

The sample data generator creates a subset for testing purposes.

## License

The example code is MIT licensed. UNSPSC codes and descriptions are owned by GS1 US and subject to their licensing terms.
