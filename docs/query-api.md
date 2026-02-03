---
title: Query API
description: Comprehensive guide to querying data in ParqueDB
---

# Query API

ParqueDB provides a flexible MongoDB-style query API with support for filters, sorting, pagination, projections, and advanced aggregations.

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

// Combine multiple operators
await db.Posts.find({
  age: { $gte: 18, $lt: 65 }
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

#### $startsWith - String prefix match

Match documents where a string field starts with a specific prefix.

```typescript
await db.Users.find({
  email: { $startsWith: 'admin' }
})
```

#### $endsWith - String suffix match

Match documents where a string field ends with a specific suffix.

```typescript
await db.Users.find({
  email: { $endsWith: '@example.com' }
})
```

#### $contains - Substring match

Match documents where a string field contains a substring.

```typescript
await db.Posts.find({
  title: { $contains: 'database' }
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
// Has publishedAt field
await db.Posts.find({
  publishedAt: { $exists: true }
})

// Does not have deletedAt field
await db.Posts.find({
  deletedAt: { $exists: false }
})
```

**Note:** `$exists: true` matches if field is present, even if the value is `null`.

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

// Mix of include/exclude NOT supported (except for _id)
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

### Aggregation Pipeline

Use the aggregation pipeline for complex data transformations.

```typescript
import { db } from 'parquedb'

// Count posts by status
const statusCounts = await db.Posts.aggregate([
  { $match: { publishedAt: { $exists: true } } },
  { $group: {
      _id: '$status',
      count: { $sum: 1 }
  } }
])
// => [{ _id: 'published', count: 150 }, { _id: 'featured', count: 25 }]

// Top authors by post count
const topAuthors = await db.Posts.aggregate([
  { $match: { status: 'published' } },
  { $group: {
      _id: '$authorId',
      postCount: { $sum: 1 },
      totalViews: { $sum: '$views' }
  } },
  { $sort: { postCount: -1 } },
  { $limit: 10 }
])

// Average views by category
const avgByCategory = await db.Posts.aggregate([
  { $match: { status: 'published' } },
  { $group: {
      _id: '$category',
      avgViews: { $avg: '$views' },
      minViews: { $min: '$views' },
      maxViews: { $max: '$views' }
  } }
])
```

#### Pipeline Stages

**$match** - Filter documents

```typescript
{ $match: { status: 'published' } }
```

**$group** - Group by field with accumulators

```typescript
{
  $group: {
    _id: '$category',        // Group by field
    count: { $sum: 1 },      // Count documents
    total: { $sum: '$views' } // Sum field values
  }
}
```

**Accumulators:**
- `$sum` - Sum values or count documents
- `$avg` - Average of values
- `$min` - Minimum value
- `$max` - Maximum value
- `$first` - First value in group
- `$last` - Last value in group
- `$push` - Array of all values
- `$addToSet` - Array of unique values

**$project** - Reshape documents

```typescript
{ $project: { title: 1, status: 1, viewCount: '$views' } }
```

**$sort** - Sort results

```typescript
{ $sort: { createdAt: -1 } }
```

**$limit** - Limit results

```typescript
{ $limit: 10 }
```

**$skip** - Skip results

```typescript
{ $skip: 20 }
```

**$unwind** - Deconstruct array field

```typescript
// Simple unwind
{ $unwind: '$tags' }

// With options
{
  $unwind: {
    path: '$tags',
    preserveNullAndEmptyArrays: true
  }
}
```

**$count** - Count documents

```typescript
{ $count: 'totalPosts' }
```

**$addFields / $set** - Add computed fields

```typescript
{ $addFields: { fullName: { $concat: ['$firstName', ' ', '$lastName'] } } }
```

**$unset** - Remove fields

```typescript
{ $unset: ['internalField', 'tempData'] }
```

**$lookup** - Join with other collections

```typescript
{
  $lookup: {
    from: 'users',
    localField: 'authorId',
    foreignField: '$id',
    as: 'author'
  }
}
```

### SQL Queries

ParqueDB provides a SQL template tag for familiar SQL syntax.

```typescript
import { db } from 'parquedb'

// Simple SELECT
const users = await db.sql`
  SELECT * FROM users
  WHERE age > ${21}
`

// With JOIN
const posts = await db.sql`
  SELECT p.title, u.name as author
  FROM posts p
  LEFT JOIN users u ON p.authorId = u.$id
  WHERE p.status = ${'published'}
  ORDER BY p.createdAt DESC
  LIMIT ${10}
`

// Aggregation
const stats = await db.sql`
  SELECT
    category,
    COUNT(*) as post_count,
    AVG(views) as avg_views
  FROM posts
  WHERE status = ${'published'}
  GROUP BY category
  ORDER BY post_count DESC
`
```

**Supported SQL features:**
- SELECT with field list or *
- FROM with table names (collection namespaces)
- WHERE with comparison operators
- JOIN (LEFT JOIN, INNER JOIN)
- GROUP BY with aggregate functions
- ORDER BY with ASC/DESC
- LIMIT and OFFSET
- Template literal parameter binding (prevents injection)

**Aggregate functions:**
- `COUNT(*)` or `COUNT(field)`
- `SUM(field)`
- `AVG(field)`
- `MIN(field)`
- `MAX(field)`

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
- **Hash indexes** - Fast equality lookups (`$eq`, `$in`)
- **FTS indexes** - Full-text search (`$text`)
- **Vector indexes** - Similarity search (`$vector`)

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
// Efficient: Uses predicate pushdown
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
7. **Consider aggregation** for complex calculations

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
