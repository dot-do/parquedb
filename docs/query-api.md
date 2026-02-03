---
title: Query API
description: Comprehensive guide to querying data in ParqueDB
---

# Query API

ParqueDB provides a flexible MongoDB-style query API with support for filters, sorting, pagination, projections, and relationship traversal.

## Quick Reference

**Core Methods:**
- `find(filter?, options?)` - Query multiple documents
- `findOne(filter?, options?)` - Get first matching document
- `get(id, options?)` - Get by ID (throws if not found)
- `count(filter?)` - Count matching documents

**Filter Operators:**
- Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
- String: `$regex`, `$startsWith`*, `$endsWith`*, `$contains`*
- Array: `$all`, `$elemMatch`, `$size`
- Existence: `$exists`, `$type`
- Logical: `$and`, `$or`, `$not`, `$nor`
- Special: `$text`, `$vector`

*ParqueDB Extension - not available in MongoDB

**Query Options:**
- `sort` - Sort results
- `limit` / `skip` - Pagination
- `cursor` - Cursor-based pagination
- `project` - Field selection
- `populate` - Include related entities
- `includeDeleted` - Include soft-deleted items
- `asOf` - Time-travel queries

## Basic Queries

### find() - Query with filters

Retrieve multiple documents matching a filter condition.

```typescript
import { db } from 'parquedb'

// Find all published posts
const posts = await db.Posts.find({ status: 'published' })

// With options
const recent = await db.Posts.find(
  { status: 'published' },
  {
    sort: { createdAt: -1 },
    limit: 20
  }
)

// Find all (no filter)
const allPosts = await db.Posts.find()
```

**Options:**
- `sort` - Sort order (see Sorting section)
- `limit` - Maximum number of results
- `skip` - Number of results to skip (for pagination)
- `cursor` - Cursor-based pagination (recommended over skip)
- `project` - Field selection (see Projection section)
- `populate` - Include related entities
- `includeDeleted` - Include soft-deleted documents (default: false)
- `asOf` - Time-travel query (see Time-Travel section)
- `explain` - Explain query plan without executing (returns QueryPlan)
- `hint` - Hint for index to use (string or index specification)
- `maxTimeMs` - Maximum query execution time in milliseconds

### findOne() - Get single document

Retrieve the first document matching a filter.

```typescript
// Find first published post
const post = await db.Posts.findOne({ status: 'published' })

// With sorting to get most recent
const latest = await db.Posts.findOne(
  { status: 'published' },
  { sort: { createdAt: -1 } }
)

// Returns null if not found
const missing = await db.Posts.findOne({ $id: 'nonexistent' })
// => null
```

### get() - Get by ID

Retrieve a specific document by its ID. Throws an error if not found.

```typescript
// By full ID
const user = await db.Users.get('users/alice')

// Short form (namespace inferred from collection)
const user = await db.Users.get('alice')

// With options
const user = await db.Users.get('alice', {
  project: { email: 1, name: 1 },
  includeDeleted: false
})

// Throws if not found
try {
  await db.Users.get('nonexistent')
} catch (error) {
  console.error('Entity not found:', error.message)
}
```

**Options:**
- `project` - Field selection
- `includeDeleted` - Include soft-deleted documents
- `asOf` - Time-travel query
- `hydrate` - Array of relationship predicates to populate
- `maxInbound` - Maximum inbound relationships to inline (default: 10)

### count() - Count matching documents

Count the number of documents matching a filter.

```typescript
// Count all posts
const total = await db.Posts.count()

// Count published posts
const published = await db.Posts.count({ status: 'published' })

// Count with complex filter
const featured = await db.Posts.count({
  status: 'published',
  featured: true,
  views: { $gte: 1000 }
})
```

## Filter Operators

ParqueDB supports MongoDB-style filter operators for flexible querying.

### Comparison Operators

#### $eq - Equality

Match documents where a field equals a specific value.

```typescript
// Explicit equality
await db.Posts.find({ status: { $eq: 'published' } })

// Implicit equality (shorthand)
await db.Posts.find({ status: 'published' })
```

#### $ne - Not equal

Match documents where a field does not equal a specific value.

```typescript
await db.Posts.find({ status: { $ne: 'draft' } })
```

#### $gt / $gte - Greater than (or equal)

Match documents where a field is greater than (or equal to) a value.

```typescript
// Greater than
await db.Posts.find({ views: { $gt: 1000 } })

// Greater than or equal
await db.Posts.find({ score: { $gte: 80 } })

// Combine multiple operators for range queries
await db.Users.find({
  age: { $gte: 18, $lt: 65 }
})

// Date ranges
await db.Posts.find({
  createdAt: {
    $gte: new Date('2024-01-01'),
    $lt: new Date('2024-02-01')
  }
})
```

#### $lt / $lte - Less than (or equal)

Match documents where a field is less than (or equal to) a value.

```typescript
// Less than
await db.Posts.find({ priority: { $lt: 5 } })

// Less than or equal
await db.Posts.find({ retries: { $lte: 3 } })
```

### Array Operators

#### $in - Match any value in array

Match documents where a field's value is in a given array.

```typescript
// Status is either published or featured
await db.Posts.find({
  status: { $in: ['published', 'featured'] }
})

// Category is tech, science, or education
await db.Posts.find({
  category: { $in: ['tech', 'science', 'education'] }
})
```

#### $nin - Not in array

Match documents where a field's value is not in a given array.

```typescript
await db.Posts.find({
  status: { $nin: ['draft', 'archived'] }
})
```

#### $all - Array contains all values

Match documents where an array field contains all specified values.

```typescript
// Posts with both 'javascript' and 'typescript' tags
await db.Posts.find({
  tags: { $all: ['javascript', 'typescript'] }
})
```

#### $size - Array has specific length

Match documents where an array field has a specific number of elements.

```typescript
// Posts with exactly 3 tags
await db.Posts.find({ tags: { $size: 3 } })
```

#### $elemMatch - Array element matches filter

Match documents where at least one array element matches a sub-filter.

```typescript
// Posts with a comment by 'alice' with score > 10
await db.Posts.find({
  comments: {
    $elemMatch: {
      author: 'alice',
      score: { $gt: 10 }
    }
  }
})
```

### String Operators

#### $regex - Regular expression match

Match documents where a string field matches a regular expression.

```typescript
// Title starts with "Hello"
await db.Posts.find({
  title: { $regex: '^Hello' }
})

// Case-insensitive search
await db.Posts.find({
  title: { $regex: 'parquet', $options: 'i' }
})

// Using RegExp object
await db.Posts.find({
  title: { $regex: /database/i }
})
```

**Options:**
- `i` - Case-insensitive
- `m` - Multiline
- `s` - Dot matches newlines
- `x` - Extended (ignore whitespace)

#### $startsWith - String prefix match (ParqueDB Extension)

Match documents where a string field starts with a specific prefix.

NOTE: This operator is a ParqueDB extension and is not available in MongoDB. For MongoDB compatibility, use `$regex` with `^` anchor.

```typescript
// ParqueDB syntax
await db.Users.find({
  email: { $startsWith: 'admin' }
})

// MongoDB-compatible equivalent
await db.Users.find({
  email: { $regex: '^admin' }
})
```

#### $endsWith - String suffix match (ParqueDB Extension)

Match documents where a string field ends with a specific suffix.

NOTE: This operator is a ParqueDB extension and is not available in MongoDB. For MongoDB compatibility, use `$regex` with `$` anchor.

```typescript
// ParqueDB syntax
await db.Users.find({
  email: { $endsWith: '@example.com' }
})

// MongoDB-compatible equivalent
await db.Users.find({
  email: { $regex: '@example\\.com$' }
})
```

#### $contains - Substring match (ParqueDB Extension)

Match documents where a string field contains a substring.

NOTE: This operator is a ParqueDB extension and is not available in MongoDB. For MongoDB compatibility, use `$regex`.

```typescript
// ParqueDB syntax
await db.Posts.find({
  title: { $contains: 'database' }
})

// MongoDB-compatible equivalent
await db.Posts.find({
  title: { $regex: 'database' }
})
```

### Logical Operators

#### $and - All conditions must match

Combine multiple filter conditions with AND logic.

```typescript
// Explicit $and
await db.Posts.find({
  $and: [
    { status: 'published' },
    { featured: true },
    { views: { $gt: 1000 } }
  ]
})

// Implicit $and (default behavior)
await db.Posts.find({
  status: 'published',
  featured: true,
  views: { $gt: 1000 }
})
```

#### $or - Any condition must match

Combine multiple filter conditions with OR logic.

```typescript
// Status is published OR featured
await db.Posts.find({
  $or: [
    { status: 'published' },
    { featured: true }
  ]
})

// Complex OR with nested conditions
await db.Posts.find({
  $or: [
    { status: 'published', featured: true },
    { views: { $gt: 10000 } }
  ]
})
```

#### $not - Condition must not match

Negate a filter condition.

```typescript
await db.Posts.find({
  status: { $not: { $eq: 'draft' } }
})

// Equivalent to $ne
await db.Posts.find({
  status: { $ne: 'draft' }
})
```

#### $nor - None of the conditions must match

Match documents where none of the conditions are true.

```typescript
await db.Posts.find({
  $nor: [
    { status: 'draft' },
    { archived: true }
  ]
})
```

### Existence Operators

#### $exists - Field presence check

Match documents where a field exists (or does not exist).

```typescript
// Has publishedAt field (includes null values)
await db.Posts.find({
  publishedAt: { $exists: true }
})

// Does not have deletedAt field (truly missing/undefined)
await db.Posts.find({
  deletedAt: { $exists: false }
})
```

**Null/Undefined Behavior:**
- `$exists: true` matches if field is present, even if the value is `null`
- `$exists: false` only matches if field is truly missing (undefined), not if it's `null`
- For equality operators (`$eq`, `$in`, etc.), `null` and `undefined` are treated as equivalent
- Use `$exists` when you need to distinguish between missing fields and null values

#### $type - Type checking

Match documents where a field has a specific type.

```typescript
await db.Posts.find({
  score: { $type: 'number' }
})
```

**Supported types:**
- `'null'` - null or undefined
- `'boolean'` - true or false
- `'number'` - numeric values
- `'string'` - text values
- `'array'` - arrays
- `'object'` - objects
- `'date'` - Date objects

### Special Operators

#### $text - Full-text search

Perform full-text search on indexed text fields.

```typescript
// Simple text search
await db.Posts.find({
  $text: {
    $search: 'parquet database'
  }
})

// With language-specific stemming
await db.Posts.find({
  $text: {
    $search: 'running',
    $language: 'english'  // Matches: run, runs, running
  }
})

// Case-sensitive search
await db.Posts.find({
  $text: {
    $search: 'ParqueDB',
    $caseSensitive: true
  }
})
```

**Options:**
- `$search` - Search query string (required)
- `$language` - Language for stemming/stopwords
- `$caseSensitive` - Case-sensitive matching (default: false)
- `$diacriticSensitive` - Diacritic-sensitive matching (default: false)

**Note:** Full-text search requires a text index on the field. See [Secondary Indexes](./architecture/secondary-indexes.md) for setup.

#### $vector - Vector similarity search

Find documents with similar vector embeddings.

```typescript
// Find 10 most similar documents
await db.Posts.find({
  $vector: {
    $near: embeddingVector,  // Query vector (number[])
    $k: 10,                   // Number of results
    $field: 'embedding',      // Field containing vectors
    $minScore: 0.8            // Optional: minimum similarity
  }
})
```

**Note:** Vector search requires a vector index on the field. See [Secondary Indexes](./architecture/secondary-indexes.md) for setup.

#### $geo - Geospatial search (Future Feature)

Geospatial queries for location-based searches are planned for a future release.

```typescript
// Planned syntax
await db.Places.find({
  $geo: {
    $near: {
      lng: -122.4194,
      lat: 37.7749
    },
    $maxDistance: 5000,  // 5km in meters
    $minDistance: 100    // Optional minimum distance
  }
})
```

## Query Options

### Sorting

Sort results by one or more fields.

```typescript
// Sort by single field (descending)
await db.Posts.find({}, {
  sort: { createdAt: -1 }
})

// Multiple sort fields
await db.Posts.find({}, {
  sort: {
    featured: -1,      // Featured first
    createdAt: -1      // Then by date
  }
})

// Sort direction options
sort: { field: 1 }     // Ascending (1 or 'asc')
sort: { field: -1 }    // Descending (-1 or 'desc')
```

**Sort order:**
- Numbers: numeric order
- Strings: lexicographic order (case-sensitive)
- Dates: chronological order
- Booleans: false < true
- null/undefined: sort first

### Pagination

#### Offset-based pagination

Use `skip` and `limit` for simple pagination.

```typescript
// Page 1 (items 0-19)
await db.Posts.find({}, { limit: 20, skip: 0 })

// Page 2 (items 20-39)
await db.Posts.find({}, { limit: 20, skip: 20 })

// Helper function
function paginate(page: number, pageSize: number) {
  return {
    limit: pageSize,
    skip: (page - 1) * pageSize
  }
}

await db.Posts.find({}, paginate(3, 20)) // Page 3
```

**Note:** Offset-based pagination can be slow for large offsets. Consider cursor-based pagination for better performance.

#### Cursor-based pagination (recommended)

Use cursors for efficient pagination on large datasets.

```typescript
// First page
const page1 = await db.Posts.find(
  { status: 'published' },
  {
    sort: { createdAt: -1 },
    limit: 20
  }
)

// Next page using cursor
if (page1.length === 20) {
  const lastItem = page1[page1.length - 1]
  const page2 = await db.Posts.find(
    { status: 'published' },
    {
      sort: { createdAt: -1 },
      limit: 20,
      cursor: lastItem.$id  // Resume from last item
    }
  )
}
```

**Benefits:**
- Consistent results even when data changes
- Better performance on large datasets
- No issues with duplicate results

### Projection (Field Selection)

Select which fields to include or exclude in results.

```typescript
// Include specific fields only
await db.Posts.find({}, {
  project: { title: 1, author: 1, createdAt: 1 }
})
// => Returns only title, author, createdAt (plus $id, $type, name)

// Exclude specific fields
await db.Posts.find({}, {
  project: { content: 0, metadata: 0 }
})
// => Returns all fields except content and metadata

// Mix of include/exclude NOT supported (except for core fields)
```

**Rules:**
- Core fields (`$id`, `$type`, `name`) are always included
- Use `1` or `true` for inclusion mode
- Use `0` or `false` for exclusion mode
- Cannot mix inclusion and exclusion (except core fields)

### Relationship Population

Include related entities in query results.

```typescript
// Simple populate
await db.Posts.find({}, {
  populate: ['author', 'categories']
})

// Populate with options
await db.Posts.find({}, {
  populate: {
    author: true,
    comments: {
      limit: 5,
      sort: { createdAt: -1 }
    }
  }
})

// Nested populate
await db.Posts.find({}, {
  populate: {
    author: {
      populate: ['organization']
    }
  }
})
```

## Advanced Queries

### QueryBuilder API

Use the fluent QueryBuilder for complex queries.

```typescript
import { db } from 'parquedb'

// Basic query
const posts = await db.Posts.builder()
  .where('status', 'eq', 'published')
  .andWhere('featured', 'eq', true)
  .orderBy('createdAt', 'desc')
  .limit(10)
  .find()

// Using operators
const results = await db.Posts.builder()
  .where('views', '>=', 1000)
  .where('score', '<', 100)
  .select(['title', 'author', 'views'])
  .find()

// OR conditions
const mixed = await db.Posts.builder()
  .where('status', 'eq', 'published')
  .orWhere('featured', 'eq', true)
  .find()

// Build without executing
const { filter, options } = db.Posts.builder()
  .where('category', 'in', ['tech', 'science'])
  .orderBy('views', 'desc')
  .limit(20)
  .build()

// Use with find()
await db.Posts.find(filter, options)
```

**QueryBuilder methods:**
- `where(field, op, value)` - Add condition
- `andWhere(field, op, value)` - Add AND condition
- `orWhere(field, op, value)` - Add OR condition
- `orderBy(field, direction)` - Add sort field
- `limit(n)` - Set result limit
- `skip(n)` / `offset(n)` - Set result offset
- `select(fields)` - Set projection
- `build()` - Get filter and options
- `find()` - Execute and return results
- `findOne()` - Execute and return first result
- `count()` - Count matching documents
- `clone()` - Create independent copy

**Supported operators:**
- Equality: `'eq'`, `'='`, `'ne'`, `'!='`
- Comparison: `'gt'`, `'>'`, `'gte'`, `'>='`, `'lt'`, `'<'`, `'lte'`, `'<='`
- Arrays: `'in'`, `'nin'`
- String: `'regex'`, `'startsWith'`, `'endsWith'`, `'contains'`
- Existence: `'exists'`

### Aggregation Pipeline (Future Feature)

Aggregation pipelines for complex data transformations are planned for a future release. Currently, you can achieve many aggregation tasks using the query API combined with in-memory processing.

**Example workaround:**

```typescript
import { db } from 'parquedb'

// Count posts by status (manual grouping)
const posts = await db.Posts.find({ status: { $exists: true } })
const statusCounts = posts.reduce((acc, post) => {
  const status = post.status as string
  acc[status] = (acc[status] || 0) + 1
  return acc
}, {} as Record<string, number>)

// Top authors by post count
const published = await db.Posts.find({ status: 'published' })
const authorStats = published.reduce((acc, post) => {
  const authorId = post.authorId as string
  if (!acc[authorId]) {
    acc[authorId] = { postCount: 0, totalViews: 0 }
  }
  acc[authorId].postCount++
  acc[authorId].totalViews += (post.views as number) || 0
  return acc
}, {} as Record<string, { postCount: number; totalViews: number }>)

const topAuthors = Object.entries(authorStats)
  .map(([authorId, stats]) => ({ authorId, ...stats }))
  .sort((a, b) => b.postCount - a.postCount)
  .slice(0, 10)
```

**Planned Pipeline Stages:**
- `$match` - Filter documents
- `$group` - Group by field with accumulators ($sum, $avg, $min, $max, etc.)
- `$project` - Reshape documents
- `$sort` - Sort results
- `$limit` / `$skip` - Pagination
- `$unwind` - Deconstruct array fields
- `$lookup` - Join with other collections
- `$count` - Count documents
- `$addFields` / `$set` - Add computed fields
- `$unset` - Remove fields

### SQL Queries

ParqueDB provides a SQL template tag for familiar SQL syntax through the `parquedb/sql` integration.

```typescript
import { createSQL } from 'parquedb/sql'
import { db } from 'parquedb'

// Create SQL executor
const sql = createSQL(db)

// Simple SELECT
const users = await sql`
  SELECT * FROM users
  WHERE age > ${21}
`

// With parameters
const posts = await sql`
  SELECT * FROM posts
  WHERE status = ${'published'}
  ORDER BY createdAt DESC
  LIMIT ${10}
`

// Using raw queries
const result = await sql.raw(
  'SELECT * FROM users WHERE age > $1',
  [21]
)
```

**Currently Supported:**
- Basic SELECT queries with WHERE conditions
- Template literal parameter binding (prevents injection)
- ORDER BY with ASC/DESC
- LIMIT and OFFSET
- Automatic translation to ParqueDB filter/options

**Note:** Advanced SQL features like JOINs, GROUP BY, and aggregate functions are planned for future releases. For now, use the native query API or perform aggregations in-memory after fetching data.

## Performance Tips

### Using Indexed Fields

Queries on indexed fields are significantly faster.

```typescript
// Fast: Using indexed field
await db.Users.find({ email: 'alice@example.com' })

// Slow: Using non-indexed field
await db.Users.find({ bio: { $contains: 'engineer' } })
```

**Index types:**
- **Hash indexes** - Fast O(1) equality lookups (`$eq`, `$in`, `$ne`)
- **B-tree indexes** - Range queries and sorting (`$gt`, `$gte`, `$lt`, `$lte`)
- **FTS indexes** - Full-text search with stemming (`$text`)
- **Vector indexes** - Approximate nearest neighbor search (`$vector`)
- **Bloom filters** - Probabilistic set membership (automatic, no configuration needed)

See [Secondary Indexes](./architecture/secondary-indexes.md) for index configuration.

### Predicate Pushdown

ParqueDB automatically pushes filter predicates to the Parquet storage layer, skipping entire row groups that can't contain matching data.

**Pushable operators** (efficient):
- Comparison: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`
- Works best on typed columns with native Parquet types

**Non-pushable operators** (require row-level evaluation):
- String: `$regex`, `$startsWith`, `$endsWith`, `$contains`
- Array: `$all`, `$elemMatch`, `$size`
- Logical: `$or`, `$nor`, `$not`

```typescript
// Efficient: Uses predicate pushdown on native Parquet columns
await db.Posts.find({
  createdAt: { $gte: new Date('2024-01-01') },
  status: { $in: ['published', 'featured'] }
})

// Less efficient: Requires row-level filtering
await db.Posts.find({
  title: { $regex: /database/i }
})
```

### Bloom Filter Hints

Bloom filters provide probabilistic existence checks for equality queries.

```typescript
// Bloom filters automatically used for equality checks
await db.Users.find({ email: 'alice@example.com' })

// Also works with $in
await db.Posts.find({
  status: { $in: ['published', 'featured'] }
})
```

**Benefits:**
- Skip row groups that definitely don't contain values
- Very low memory overhead
- Particularly effective for high-cardinality fields

**Note:** Bloom filters are automatically built for all columns during write. No configuration needed.

### Query Optimization Tips

1. **Use indexes** for frequently queried fields
2. **Prefer equality and range operators** over regex/string operators
3. **Limit result sets** - Don't fetch more than you need
4. **Use projection** to reduce data transfer
5. **Cursor pagination** over offset-based for large datasets
6. **Filter before sort** when possible
7. **Consider in-memory processing** for complex calculations (aggregation pipeline coming soon)

```typescript
// Good: Efficient query
await db.Posts.find(
  {
    status: 'published',           // Indexed, pushable
    createdAt: { $gte: lastWeek }  // Pushable
  },
  {
    limit: 20,                      // Limit results
    project: { title: 1, author: 1 }, // Project fields
    sort: { createdAt: -1 }         // Sort on indexed field
  }
)

// Less efficient: Complex query
await db.Posts.find(
  {
    $or: [                          // Not pushable
      { title: { $regex: /data/i } }, // Not pushable
      { content: { $contains: 'db' } } // Not pushable
    ]
  },
  { skip: 1000 }                    // Large offset
)
```

## Time-Travel Queries

Query historical data using the `asOf` option.

```typescript
// View data as it was at a specific time
const historical = await db.Posts.find(
  { status: 'published' },
  { asOf: new Date('2024-01-01T00:00:00Z') }
)

// Get entity state at specific time
const oldVersion = await db.Posts.get('post-123', {
  asOf: new Date('2024-01-01T00:00:00Z')
})
```

**Note:** Time-travel requires the event log. See [Event Log](./architecture/graph-first-architecture.md) for details.

## Error Handling

Handle common query errors gracefully.

```typescript
try {
  const user = await db.Users.get('nonexistent')
} catch (error) {
  if (error.message.includes('not found')) {
    console.log('User does not exist')
  }
}

// Use findOne if you want null instead of error
const user = await db.Users.findOne({ $id: 'users/nonexistent' })
if (!user) {
  console.log('User does not exist')
}

// Validate filter operators
try {
  await db.Posts.find({ status: { $invalidOp: 'value' } })
} catch (error) {
  console.error('Invalid filter operator')
}
```

## Next Steps

- [Update Operators](./updates.md) - Modify data with $set, $inc, $push, etc.
- [Relationships](./relationships.md) - Query related entities
- [Secondary Indexes](./architecture/secondary-indexes.md) - Speed up queries with indexes
- [Schema Definition](./schemas.md) - Define typed collections
