# Query API

ParqueDB uses a MongoDB-style query API for filtering and retrieving documents. This guide covers all available filter operators with examples.

## Basic Queries

### Simple Equality

The simplest query matches documents where fields equal specific values:

```typescript
// Single field equality
const posts = await db.Posts.find({ status: 'published' })

// Multiple fields (implicit AND)
const featured = await db.Posts.find({
  status: 'published',
  featured: true
})

// Null value matching
const noAuthor = await db.Posts.find({ author: null })
```

### Direct Value Matching

When you specify a value directly (without an operator), ParqueDB performs equality matching:

```typescript
// These are equivalent
await db.Posts.find({ status: 'draft' })
await db.Posts.find({ status: { $eq: 'draft' } })
```

## Comparison Operators

### $eq - Equality

Matches values equal to the specified value:

```typescript
const drafts = await db.Posts.find({
  status: { $eq: 'draft' }
})

// Works with any type
const zeroPosts = await db.Posts.find({ viewCount: { $eq: 0 } })
const oldPosts = await db.Posts.find({ createdAt: { $eq: new Date('2024-01-01') } })
```

### $ne - Not Equal

Matches values not equal to the specified value:

```typescript
const notDrafts = await db.Posts.find({
  status: { $ne: 'draft' }
})

// Excludes null values
const hasAuthor = await db.Posts.find({
  author: { $ne: null }
})
```

### $gt - Greater Than

Matches values greater than the specified value:

```typescript
const popular = await db.Posts.find({
  viewCount: { $gt: 1000 }
})

// Works with dates
const recent = await db.Posts.find({
  createdAt: { $gt: new Date('2024-01-01') }
})

// Works with strings (lexicographic comparison)
const afterM = await db.Posts.find({
  title: { $gt: 'M' }
})
```

### $gte - Greater Than or Equal

Matches values greater than or equal to the specified value:

```typescript
const atLeast100 = await db.Posts.find({
  viewCount: { $gte: 100 }
})

const thisYear = await db.Posts.find({
  createdAt: { $gte: new Date('2024-01-01') }
})
```

### $lt - Less Than

Matches values less than the specified value:

```typescript
const lowViews = await db.Posts.find({
  viewCount: { $lt: 10 }
})

const oldPosts = await db.Posts.find({
  createdAt: { $lt: new Date('2020-01-01') }
})
```

### $lte - Less Than or Equal

Matches values less than or equal to the specified value:

```typescript
const freeItems = await db.Products.find({
  price: { $lte: 0 }
})

// Combine with other operators
const priceRange = await db.Products.find({
  price: { $gte: 10, $lte: 50 }
})
```

### $in - In Array

Matches any of the values in the specified array:

```typescript
const selectedStatuses = await db.Posts.find({
  status: { $in: ['published', 'featured'] }
})

// Works with any type
const specificIds = await db.Posts.find({
  authorId: { $in: ['users/alice', 'users/bob', 'users/charlie'] }
})
```

### $nin - Not In Array

Matches values not in the specified array:

```typescript
const active = await db.Posts.find({
  status: { $nin: ['deleted', 'archived', 'spam'] }
})
```

## String Operators

### $regex - Regular Expression

Matches strings against a regular expression pattern:

```typescript
// Using RegExp literal
const matching = await db.Posts.find({
  title: { $regex: /^Hello/i }
})

// Using string pattern with options
const caseInsensitive = await db.Posts.find({
  title: { $regex: 'hello', $options: 'i' }
})

// Available options:
// - 'i' - case insensitive
// - 'm' - multiline
// - 's' - dotall (. matches newlines)
```

### $startsWith - Prefix Match

Matches strings starting with the specified prefix:

```typescript
const apiRoutes = await db.Routes.find({
  path: { $startsWith: '/api/' }
})

const jsPosts = await db.Posts.find({
  title: { $startsWith: 'JavaScript' }
})
```

### $endsWith - Suffix Match

Matches strings ending with the specified suffix:

```typescript
const mdFiles = await db.Files.find({
  name: { $endsWith: '.md' }
})

const questions = await db.Posts.find({
  title: { $endsWith: '?' }
})
```

### $contains - Substring Match

Matches strings containing the specified substring:

```typescript
const reactPosts = await db.Posts.find({
  content: { $contains: 'React' }
})

const errorLogs = await db.Logs.find({
  message: { $contains: 'ERROR' }
})
```

## Array Operators

### $all - Contains All

Matches arrays containing all specified values:

```typescript
const fullStack = await db.Posts.find({
  tags: { $all: ['frontend', 'backend'] }
})

// Order doesn't matter - both these documents match:
// { tags: ['frontend', 'backend', 'devops'] }
// { tags: ['backend', 'frontend'] }
```

### $elemMatch - Element Match

Matches arrays where at least one element matches all specified conditions:

```typescript
// Find posts with a comment from a specific author with high score
const posts = await db.Posts.find({
  comments: {
    $elemMatch: {
      author: 'users/alice',
      score: { $gte: 10 }
    }
  }
})

// Find orders with at least one expensive item
const orders = await db.Orders.find({
  items: {
    $elemMatch: {
      price: { $gt: 100 },
      quantity: { $gte: 2 }
    }
  }
})
```

### $size - Array Length

Matches arrays with exactly the specified number of elements:

```typescript
const threeTags = await db.Posts.find({
  tags: { $size: 3 }
})

const noComments = await db.Posts.find({
  comments: { $size: 0 }
})
```

## Existence Operators

### $exists - Field Existence

Matches documents where the field exists (or doesn't exist):

```typescript
// Field exists (and is not undefined)
const withImage = await db.Posts.find({
  coverImage: { $exists: true }
})

// Field doesn't exist or is undefined
const noImage = await db.Posts.find({
  coverImage: { $exists: false }
})
```

### $type - Type Check

Matches documents where the field is of the specified type:

```typescript
// Find documents where price is a number
const numericPrices = await db.Products.find({
  price: { $type: 'number' }
})

// Available types:
// - 'null'
// - 'boolean'
// - 'number'
// - 'string'
// - 'array'
// - 'object'
// - 'date'
```

## Logical Operators

### $and - Logical AND

Matches documents satisfying all conditions. Useful when you need multiple conditions on the same field:

```typescript
// Multiple conditions on same field
const priceRange = await db.Products.find({
  $and: [
    { price: { $gte: 10 } },
    { price: { $lte: 100 } }
  ]
})

// Combine with other operators
const complexQuery = await db.Posts.find({
  $and: [
    { status: 'published' },
    { $or: [{ featured: true }, { viewCount: { $gt: 1000 } }] }
  ]
})
```

Note: Multiple fields at the top level are implicitly ANDed:

```typescript
// These are equivalent
await db.Posts.find({ status: 'published', featured: true })
await db.Posts.find({ $and: [{ status: 'published' }, { featured: true }] })
```

### $or - Logical OR

Matches documents satisfying any of the conditions:

```typescript
const highlighted = await db.Posts.find({
  $or: [
    { status: 'featured' },
    { pinned: true },
    { viewCount: { $gt: 10000 } }
  ]
})

// Combine with field conditions
const query = await db.Posts.find({
  category: 'tech',
  $or: [
    { status: 'published' },
    { author: 'users/admin' }
  ]
})
```

### $not - Logical NOT

Negates a filter condition:

```typescript
// Not archived
const active = await db.Posts.find({
  $not: { status: 'archived' }
})

// Not matching a pattern
const noNumbers = await db.Posts.find({
  title: { $not: { $regex: /\d+/ } }
})

// Complex negation
const notPopularOrFeatured = await db.Posts.find({
  $not: {
    $or: [
      { viewCount: { $gt: 1000 } },
      { featured: true }
    ]
  }
})
```

### $nor - Logical NOR

Matches documents that fail to match all conditions (neither A nor B):

```typescript
// Neither deleted nor archived
const active = await db.Posts.find({
  $nor: [
    { status: 'deleted' },
    { status: 'archived' }
  ]
})

// Exclude multiple patterns
const cleanContent = await db.Posts.find({
  $nor: [
    { content: { $contains: 'spam' } },
    { content: { $contains: 'advertisement' } },
    { flagged: true }
  ]
})
```

## Special Operators

### $text - Full-Text Search

Performs full-text search on indexed text fields:

```typescript
const results = await db.Posts.find({
  $text: {
    $search: 'parquet database columnar'
  }
})

// With options
const exactSearch = await db.Posts.find({
  $text: {
    $search: 'React Hooks',
    $language: 'en',
    $caseSensitive: true,
    $diacriticSensitive: false
  }
})
```

Note: Full-text search requires a text index on the collection.

### $vector - Vector Similarity Search

Performs approximate nearest neighbor search on vector embeddings:

```typescript
const similar = await db.Posts.find({
  $vector: {
    $near: queryEmbedding,  // Your query vector
    $k: 10,                  // Number of results
    $field: 'embedding',     // Field containing vectors
    $minScore: 0.8           // Optional minimum similarity
  }
})
```

Note: Vector search requires a vector index on the collection.

### $geo - Geospatial Queries

Performs geospatial queries on location fields:

```typescript
const nearby = await db.Places.find({
  $geo: {
    $near: { lng: -122.4194, lat: 37.7749 },
    $maxDistance: 5000,  // meters
    $minDistance: 100    // meters (optional)
  }
})
```

Note: Geospatial queries require a geo index on the collection.

## Nested Field Queries

Access nested fields using dot notation:

```typescript
// Query nested object fields
const sfPosts = await db.Posts.find({
  'location.city': 'San Francisco'
})

// Multiple levels deep
const active = await db.Users.find({
  'settings.notifications.email': true
})

// Combine with operators
const highScores = await db.Games.find({
  'stats.score': { $gte: 1000 }
})
```

## Query Options

### Sorting

```typescript
// Ascending (1 or 'asc')
const oldest = await db.Posts.find({}, {
  sort: { createdAt: 1 }
})

// Descending (-1 or 'desc')
const newest = await db.Posts.find({}, {
  sort: { createdAt: -1 }
})

// Multiple fields
const sorted = await db.Posts.find({}, {
  sort: { status: 1, createdAt: -1 }
})
```

### Pagination

```typescript
// Limit results
const top10 = await db.Posts.find({}, { limit: 10 })

// Skip results (offset-based)
const page2 = await db.Posts.find({}, { skip: 10, limit: 10 })

// Cursor-based pagination (recommended for large datasets)
const page1 = await db.Posts.find({}, { limit: 10 })
if (page1.hasMore) {
  const page2 = await db.Posts.find({}, {
    limit: 10,
    cursor: page1.items[page1.items.length - 1].$id
  })
}
```

### Projection

```typescript
// Include only specific fields (1 = include)
const titles = await db.Posts.find({}, {
  project: { title: 1, status: 1 }
})

// Exclude specific fields (0 = exclude)
const noContent = await db.Posts.find({}, {
  project: { content: 0, rawHtml: 0 }
})
```

### Include Deleted

```typescript
// Include soft-deleted documents
const allPosts = await db.Posts.find({}, {
  includeDeleted: true
})
```

## Performance Tips

1. **Use indexes**: Create secondary indexes on frequently queried fields
2. **Limit result size**: Always use `limit` for large collections
3. **Prefer $in over $or**: `{ status: { $in: ['a', 'b'] } }` is faster than `{ $or: [{ status: 'a' }, { status: 'b' }] }`
4. **Use cursor pagination**: More efficient than skip/offset for large datasets
5. **Project only needed fields**: Reduces data transfer and memory usage
6. **Put selective conditions first**: In `$and` queries, place the most restrictive condition first

## See Also

- [Update Operators](./updates.md) - Modify documents with $set, $inc, $push, etc.
- [Getting Started](./getting-started.md) - Basic CRUD operations
- [Secondary Indexes](./architecture/SECONDARY_INDEXES.md) - Index types and strategies
