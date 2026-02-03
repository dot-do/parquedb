---
title: Variant Shredding
description: Implementation plan for Parquet Variant Shredding in hyparquet/hyparquet-writer to enable predicate pushdown on semi-structured data fields with statistics-based row group skipping.
---

## Goal

Implement Parquet Variant Shredding in hyparquet/hyparquet-writer to enable:
1. Unified data model: `$id | $index (Variant) | $data (Variant)`
2. Predicate pushdown on shredded Variant fields
3. Statistics-based row group skipping

## Parquet Variant Shredding Structure

Per [Parquet spec](https://parquet.apache.org/docs/file-format/types/variantshredding/):

```
optional group $index (VARIANT) {
  required binary metadata;
  optional binary value;           // null when fully shredded
  optional group typed_value {
    optional group titleType {     // shredded field
      optional binary value;       // null for typed
      optional UTF8 typed_value;   // <- STATISTICS HERE
    }
    optional group $type {
      optional binary value;
      optional UTF8 typed_value;   // <- STATISTICS HERE
    }
  }
}
```

**Key insight**: Statistics on `typed_value` columns enable predicate pushdown when the corresponding `value` is null.

## Implementation Tasks

### Phase 1: hyparquet Reader (Predicate Pushdown)

**1.1 Dot-notation filter parsing** (`plan.js`)
```javascript
// Current: only top-level columns
{ titleType: "movie" }

// New: support nested Variant fields
{ "$index.titleType": "movie" }
// Maps to: $index.typed_value.titleType.typed_value
```

**1.2 Nested column statistics lookup**
- Map dot-notation paths to Parquet column paths
- Read statistics from `typed_value.{field}.typed_value` columns
- Apply predicate pushdown using those statistics

**1.3 Filter evaluation on nested fields**
- Extend `matchesFilter()` to access nested Variant fields
- Handle the `typed_value` vs `value` structure

### Phase 2: hyparquet-writer (Shredded Variant Writing)

**2.1 VARIANT schema type** (`schema.js`)
```javascript
// New: VARIANT type with shredding
{
  name: '$index',
  type: 'VARIANT',
  shredFields: ['$type', 'titleType', 'startYear'],
}
```

**2.2 Variant column writing** (`column.js`)
- Create nested group structure per Parquet spec
- Write `metadata` binary
- Write `value` as null for shredded fields
- Write `typed_value.{field}.typed_value` with actual values
- Compute statistics for `typed_value` columns

**2.3 Variant encoding** (`unconvert.js`)
- Encode Variant metadata header
- Encode values in Variant binary format
- Handle shredding extraction

### Phase 3: Integration

**3.1 ParqueDB ETL updates**
```javascript
const buffer = parquetWriteBuffer({
  columnData: [
    { name: '$id', type: 'STRING', data: ids },
    {
      name: '$index',
      type: 'VARIANT',
      shredFields: ['$type', 'titleType', 'startYear'],
      data: indexObjects
    },
    { name: '$data', type: 'VARIANT', data: fullObjects },
  ],
})
```

**3.2 Query execution**
```javascript
// Filter on shredded Variant field
const results = await parquetQuery({
  file,
  filter: { '$index.titleType': 'movie' },  // Uses statistics!
  columns: ['$id', '$data'],
})
```

## Filter Path Mapping

| User Filter | Parquet Column Path | Statistics Source |
|-------------|---------------------|-------------------|
| `$index.titleType` | `$index.typed_value.titleType` | `typed_value.typed_value` |
| `$index.$type` | `$index.typed_value.$type` | `typed_value.typed_value` |
| `$data.genres` | Not shredded | Must scan |

## Statistics Behavior

From Parquet spec:
> "Statistics for typed_value columns can be used for file, row group, or page skipping when value is always null (missing)."

This means:
- Shredded fields (value=null) -> Statistics work
- Non-shredded fields (value!=null) -> Must scan

## Testing Plan

1. **Unit tests**: Write/read shredded Variant roundtrip
2. **Statistics tests**: Verify min/max are computed correctly
3. **Predicate pushdown tests**: Verify row groups are skipped
4. **Benchmark**: Compare with native columns approach
5. **DuckDB compatibility**: Verify files are readable

## Timeline

- Phase 1 (Reader): 2-3 days
- Phase 2 (Writer): 3-4 days
- Phase 3 (Integration): 1-2 days
- Testing: 2 days

## References

- [Parquet Variant Encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/)
- [Parquet Variant Shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/)
- [Spark Nested Predicate Pushdown](https://github.com/apache/spark/pull/27728)
- [DataFusion User-Defined Indexes](https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes/)
