---
title: MongoDB to ParqueDB Migration Guide
description: Complete guide for migrating from MongoDB to ParqueDB, including API comparisons, query syntax differences, and best practices
---

# MongoDB to ParqueDB Migration Guide

This guide helps developers familiar with MongoDB transition to ParqueDB. ParqueDB provides a MongoDB-compatible API while adding features like built-in relationships, time-travel queries, and columnar storage optimization.

## Table of Contents

1. [API Comparison Table](#api-comparison-table)
2. [Query Syntax Differences](#query-syntax-differences)
3. [Schema Migration Steps](#schema-migration-steps)
4. [Code Examples: Before and After](#code-examples-before-and-after)
5. [Aggregation Pipeline](#aggregation-pipeline)
6. [Indexing Differences](#indexing-differences)
7. [Common Gotchas and Tips](#common-gotchas-and-tips)
8. [Performance Considerations](#performance-considerations)

---

## API Comparison Table

### CRUD Operations

| Operation | MongoDB | ParqueDB | Notes |
|-----------|---------|----------|-------|
| **Create one** | `collection.insertOne(doc)` | `collection.create(doc)` | ParqueDB auto-generates `$id`, `$type`, timestamps |
| **Create many** | `collection.insertMany(docs)` | `Promise.all(docs.map(d => collection.create(d)))` | Bulk insert planned |
| **Find one** | `collection.findOne(filter)` | `collection.findOne(filter)` | Identical API |
| **Find many** | `collection.find(filter).toArray()` | `collection.find(filter)` | Returns array directly |
| **Get by ID** | `collection.findOne({ _id: id })` | `collection.get(id)` | Throws if not found |
| **Update one** | `collection.updateOne(filter, update)` | `collection.update(id, update)` | ID-based updates |
| **Update many** | `collection.updateMany(filter, update)` | `collection.updateMany(filter, update)` | Identical API |
| **Delete one** | `collection.deleteOne(filter)` | `collection.delete(id)` | Soft delete by default |
| **Delete many** | `collection.deleteMany(filter)` | `collection.deleteMany(filter)` | Soft delete by default |
| **Count** | `collection.countDocuments(filter)` | `collection.count(filter)` | Identical API |
| **Exists** | `collection.findOne(filter) !== null` | `collection.exists(id)` | Direct method |

### Query Options

| Option | MongoDB | ParqueDB | Notes |
|--------|---------|----------|-------|
| **Limit** | `.limit(n)` | `{ limit: n }` | Options object |
| **Skip** | `.skip(n)` | `{ skip: n }` | Options object |
| **Sort** | `.sort({ field: 1 })` | `{ sort: { field: 1 } }` | Options object |
| **Projection** | `.project({ field: 1 })` | `{ project: { field: 1 } }` | Options object |
| **Cursor pagination** | Manual with `_id` | `{ cursor: lastId }` | Built-in support |

### Filter Operators

| Operator | MongoDB | ParqueDB | Status |
|----------|---------|----------|--------|
| `$eq` | Supported | Supported | Identical |
| `$ne` | Supported | Supported | Identical |
| `$gt` | Supported | Supported | Identical |
| `$gte` | Supported | Supported | Identical |
| `$lt` | Supported | Supported | Identical |
| `$lte` | Supported | Supported | Identical |
| `$in` | Supported | Supported | Identical |
| `$nin` | Supported | Supported | Identical |
| `$and` | Supported | Supported | Identical |
| `$or` | Supported | Supported | Identical |
| `$not` | Supported | Supported | Identical |
| `$nor` | Supported | Supported | Identical |
| `$exists` | Supported | Supported | Identical |
| `$type` | Supported | Supported | Identical |
| `$regex` | Supported | Supported | Identical |
| `$all` | Supported | Supported | Identical |
| `$elemMatch` | Supported | Supported | Identical |
| `$size` | Supported | Supported | Identical |
| `$text` | Supported | Supported | Requires FTS index |
| `$startsWith` | Not native | Supported | ParqueDB extension |
| `$endsWith` | Not native | Supported | ParqueDB extension |
| `$contains` | Not native | Supported | ParqueDB extension |
| `$vector` | Not native | Supported | Vector similarity search |

### Update Operators

| Operator | MongoDB | ParqueDB | Status |
|----------|---------|----------|--------|
| `$set` | Supported | Supported | Identical |
| `$unset` | Supported | Supported | Identical |
| `$inc` | Supported | Supported | Identical |
| `$mul` | Supported | Supported | Identical |
| `$min` | Supported | Supported | Identical |
| `$max` | Supported | Supported | Identical |
| `$rename` | Supported | Supported | Identical |
| `$push` | Supported | Supported | With modifiers |
| `$pull` | Supported | Supported | Identical |
| `$pullAll` | Supported | Supported | Identical |
| `$addToSet` | Supported | Supported | Identical |
| `$pop` | Supported | Supported | Identical |
| `$currentDate` | Supported | Supported | Identical |
| `$setOnInsert` | Supported | Supported | For upserts |
| `$bit` | Supported | Supported | Identical |
| `$link` | Not available | Supported | Create relationships |
| `$unlink` | Not available | Supported | Remove relationships |

---

## Query Syntax Differences

### Connection and Database Access

**MongoDB:**
```javascript
import { MongoClient } from 'mongodb'

const client = new MongoClient('mongodb://localhost:27017')
await client.connect()
const db = client.db('myapp')
const users = db.collection('users')
```

**ParqueDB:**
```typescript
import { DB } from 'parquedb'

// Schema-less (flexible mode)
import { db } from 'parquedb'
const users = db.Users

// With schema (typed mode)
const database = DB({
  User: {
    email: 'email!#',
    name: 'string!'
  }
})
const users = database.User
```

### Document Structure

**MongoDB:**
```javascript
// Documents have _id (ObjectId by default)
{
  _id: ObjectId('507f1f77bcf86cd799439011'),
  name: 'Alice',
  email: 'alice@example.com',
  createdAt: new Date()  // Manual timestamp
}
```

**ParqueDB:**
```typescript
// Entities have $id (ULID-based) and automatic fields
{
  $id: 'users/01HQ3KPNM7...',      // Auto-generated
  $type: 'User',                    // Auto-set from collection
  name: 'Alice',
  email: 'alice@example.com',
  createdAt: new Date(),            // Auto-generated
  createdBy: 'system/system',       // Auto-tracked
  updatedAt: new Date(),            // Auto-generated
  updatedBy: 'system/system',       // Auto-tracked
  version: 1                        // Optimistic concurrency
}
```

### Finding Documents

**MongoDB:**
```javascript
// Find with cursor
const cursor = await users.find({ status: 'active' })
  .sort({ createdAt: -1 })
  .limit(10)
  .skip(20)
const results = await cursor.toArray()

// Find one
const user = await users.findOne({ email: 'alice@example.com' })
```

**ParqueDB:**
```typescript
// Find returns array directly
const results = await users.find(
  { status: 'active' },
  {
    sort: { createdAt: -1 },
    limit: 10,
    skip: 20
  }
)

// Find one
const user = await users.findOne({ email: 'alice@example.com' })

// Get by ID (throws if not found)
const user = await users.get('users/01HQ3KPNM7...')
```

### Inserting Documents

**MongoDB:**
```javascript
// Insert one
const result = await users.insertOne({
  name: 'Alice',
  email: 'alice@example.com'
})
console.log(result.insertedId)

// Insert many
const result = await users.insertMany([
  { name: 'Alice', email: 'alice@example.com' },
  { name: 'Bob', email: 'bob@example.com' }
])
```

**ParqueDB:**
```typescript
// Create one (returns full entity)
const user = await users.create({
  name: 'Alice',
  email: 'alice@example.com'
})
console.log(user.$id)  // 'users/01HQ3KPNM7...'

// Create many (use Promise.all)
const newUsers = await Promise.all([
  users.create({ name: 'Alice', email: 'alice@example.com' }),
  users.create({ name: 'Bob', email: 'bob@example.com' })
])
```

### Updating Documents

**MongoDB:**
```javascript
// Update by filter
await users.updateOne(
  { email: 'alice@example.com' },
  { $set: { status: 'verified' } }
)

// Update many
await users.updateMany(
  { status: 'pending' },
  { $set: { status: 'expired' } }
)
```

**ParqueDB:**
```typescript
// Update by ID
await users.update(user.$id, {
  $set: { status: 'verified' }
})

// Update many by filter
await users.updateMany(
  { status: 'pending' },
  { $set: { status: 'expired' } }
)

// Update with optimistic concurrency
await users.update(user.$id,
  { $set: { name: 'Alice Smith' } },
  { expectedVersion: user.version }
)
```

### Deleting Documents

**MongoDB:**
```javascript
// Delete one
await users.deleteOne({ email: 'alice@example.com' })

// Delete many
await users.deleteMany({ status: 'inactive' })
```

**ParqueDB:**
```typescript
// Soft delete (default) - sets deletedAt
await users.delete(user.$id)

// Hard delete (permanent)
await users.delete(user.$id, { hard: true })

// Delete many (soft delete by default)
await users.deleteMany({ status: 'inactive' })

// Hard delete many
await users.deleteMany({ status: 'spam' }, { hard: true })

// Query includes deleted
const allUsers = await users.find({}, { includeDeleted: true })
```

---

## Schema Migration Steps

### Step 1: Analyze Your MongoDB Schema

Export your MongoDB schema using tools like `variety.js` or `mongodb-schema`:

```bash
# Using mongosh
mongosh mydb --eval "db.users.find().limit(100).forEach(printjson)" > users_sample.json
```

### Step 2: Convert to ParqueDB Schema

**MongoDB implicit schema (from documents):**
```javascript
// Typical MongoDB document
{
  _id: ObjectId('...'),
  name: 'Alice',
  email: 'alice@example.com',
  age: 30,
  tags: ['developer', 'blogger'],
  address: {
    city: 'San Francisco',
    state: 'CA'
  },
  createdAt: ISODate('2024-01-01'),
  posts: [ObjectId('...'), ObjectId('...')]  // Reference array
}
```

**ParqueDB explicit schema:**
```typescript
const db = DB({
  User: {
    // Required fields with index
    email: 'email!#',
    name: 'string!',

    // Optional fields
    age: 'int',
    tags: 'string[]',

    // Nested object (stored in Variant)
    address: 'json',

    // Relationships (instead of ObjectId arrays)
    posts: '<- Post.author[]'  // Reverse relationship
  },

  Post: {
    title: 'string!',
    content: 'markdown',
    author: '-> User.posts'  // Forward relationship
  }
})
```

### Step 3: Schema Mapping Reference

| MongoDB Type | ParqueDB Type | Notes |
|--------------|---------------|-------|
| `String` | `string`, `text`, `markdown` | Use `text` for long content |
| `Number` (int) | `int` | 32-bit integer |
| `Number` (float) | `float`, `double` | Use `double` for precision |
| `Number` (decimal) | `decimal(p,s)` | For currency/financial |
| `Boolean` | `boolean` | |
| `Date` | `date`, `datetime`, `timestamp` | `timestamp` for microseconds |
| `ObjectId` | `uuid` or relationship | Use relationships for refs |
| `Array` | `type[]` | Append `[]` to any type |
| `Object` | `json` or nested fields | Or use `$shred` for hot fields |
| `Binary` | `binary` | |

### Step 4: Migrate Data

Create a migration script:

```typescript
import { MongoClient } from 'mongodb'
import { DB } from 'parquedb'

// Source MongoDB
const mongo = new MongoClient('mongodb://localhost:27017')
await mongo.connect()
const mongoDb = mongo.db('myapp')

// Target ParqueDB
const db = DB({
  User: {
    email: 'email!#',
    name: 'string!',
    age: 'int',
    tags: 'string[]'
  }
})

// Migrate users
const mongoCursor = mongoDb.collection('users').find()
for await (const doc of mongoCursor) {
  await db.User.create({
    // Map _id to custom ID if needed
    $id: doc._id.toString(),
    email: doc.email,
    name: doc.name,
    age: doc.age,
    tags: doc.tags || []
  })
}

// Close connections
await mongo.close()
```

### Step 5: Migrate Relationships

**MongoDB (reference pattern):**
```javascript
// Posts collection with author reference
{
  _id: ObjectId('...'),
  title: 'My Post',
  authorId: ObjectId('507f1f77bcf86cd799439011')
}
```

**ParqueDB (bidirectional relationships):**
```typescript
const db = DB({
  User: {
    name: 'string!',
    posts: '<- Post.author[]'  // Auto-populated reverse
  },
  Post: {
    title: 'string!',
    author: '-> User.posts'    // Forward reference
  }
})

// Create post with relationship
const post = await db.Post.create({
  title: 'My Post',
  author: user.$id  // Use entity ID
})

// Or link after creation
await db.Post.update(post.$id, {
  $link: { author: user.$id }
})
```

---

## Code Examples: Before and After

### Example 1: User Registration

**MongoDB:**
```javascript
async function registerUser(email, password, name) {
  const existing = await db.users.findOne({ email })
  if (existing) {
    throw new Error('Email already registered')
  }

  const result = await db.users.insertOne({
    email,
    passwordHash: await hash(password),
    name,
    status: 'pending',
    createdAt: new Date(),
    updatedAt: new Date()
  })

  return { id: result.insertedId.toString() }
}
```

**ParqueDB:**
```typescript
async function registerUser(email: string, password: string, name: string) {
  const existing = await db.User.findOne({ email })
  if (existing) {
    throw new Error('Email already registered')
  }

  const user = await db.User.create({
    email,
    passwordHash: await hash(password),
    name,
    status: 'pending'
    // createdAt, updatedAt auto-generated
  })

  return { id: user.$id }
}
```

### Example 2: Blog Post with Comments

**MongoDB:**
```javascript
// Create post
const post = await db.posts.insertOne({
  title: 'Hello World',
  content: 'My first post',
  authorId: new ObjectId(userId),
  comments: [],  // Embedded comments
  tags: ['intro'],
  createdAt: new Date()
})

// Add comment (embedded)
await db.posts.updateOne(
  { _id: post.insertedId },
  {
    $push: {
      comments: {
        _id: new ObjectId(),
        text: 'Great post!',
        authorId: new ObjectId(commenterId),
        createdAt: new Date()
      }
    }
  }
)

// Get post with author (manual join)
const postDoc = await db.posts.findOne({ _id: post.insertedId })
const author = await db.users.findOne({ _id: postDoc.authorId })
```

**ParqueDB:**
```typescript
// Schema with relationships
const db = DB({
  User: {
    name: 'string!',
    posts: '<- Post.author[]',
    comments: '<- Comment.author[]'
  },
  Post: {
    title: 'string!',
    content: 'text',
    tags: 'string[]',
    author: '-> User.posts',
    comments: '<- Comment.post[]'
  },
  Comment: {
    text: 'string!',
    post: '-> Post.comments',
    author: '-> User.comments'
  }
})

// Create post
const post = await db.Post.create({
  title: 'Hello World',
  content: 'My first post',
  tags: ['intro'],
  author: userId
})

// Add comment (separate entity with relationship)
const comment = await db.Comment.create({
  text: 'Great post!',
  post: post.$id,
  author: commenterId
})

// Get post - relationships available via traversal
const fullPost = await db.Post.get(post.$id)
const comments = await fullPost.referencedBy('comments')
const author = await db.User.get(post.author)
```

### Example 3: Pagination

**MongoDB:**
```javascript
async function getPosts(page = 1, pageSize = 10) {
  const skip = (page - 1) * pageSize

  const [posts, total] = await Promise.all([
    db.posts
      .find({ status: 'published' })
      .sort({ publishedAt: -1 })
      .skip(skip)
      .limit(pageSize)
      .toArray(),
    db.posts.countDocuments({ status: 'published' })
  ])

  return {
    posts,
    total,
    page,
    pageSize,
    totalPages: Math.ceil(total / pageSize)
  }
}
```

**ParqueDB:**
```typescript
async function getPosts(page = 1, pageSize = 10) {
  const skip = (page - 1) * pageSize

  // Using findPaginated for built-in pagination metadata
  const result = await db.Post.findPaginated(
    { status: 'published' },
    {
      sort: { publishedAt: -1 },
      skip,
      limit: pageSize
    }
  )

  return {
    posts: result.items,
    total: result.total,
    hasMore: result.hasMore,
    nextCursor: result.nextCursor
  }
}

// Or use cursor-based pagination (recommended for large datasets)
async function getPostsCursor(cursor?: string, limit = 10) {
  return await db.Post.findPaginated(
    { status: 'published' },
    {
      sort: { publishedAt: -1 },
      limit,
      cursor
    }
  )
}
```

### Example 4: Text Search

**MongoDB:**
```javascript
// Create text index
await db.posts.createIndex({ title: 'text', content: 'text' })

// Search
const results = await db.posts.find({
  $text: { $search: 'parquet database' }
}).toArray()
```

**ParqueDB:**
```typescript
// Schema with FTS index
const db = DB({
  Post: {
    title: {
      type: 'string',
      required: true,
      index: 'fts',
      ftsOptions: { weight: 2.0 }
    },
    content: {
      type: 'text',
      index: 'fts'
    }
  }
})

// Search
const results = await db.Post.find({
  $text: { $search: 'parquet database' }
})

// Or use string operators
const results = await db.Post.find({
  $or: [
    { title: { $contains: 'parquet' } },
    { content: { $contains: 'database' } }
  ]
})
```

---

## Aggregation Pipeline

ParqueDB supports MongoDB-style aggregation pipelines:

### Supported Stages

| Stage | MongoDB | ParqueDB | Notes |
|-------|---------|----------|-------|
| `$match` | Supported | Supported | Identical |
| `$group` | Supported | Supported | Identical |
| `$sort` | Supported | Supported | Identical |
| `$limit` | Supported | Supported | Identical |
| `$skip` | Supported | Supported | Identical |
| `$project` | Supported | Supported | Identical |
| `$addFields` | Supported | Supported | Identical |
| `$unwind` | Supported | Supported | Identical |
| `$lookup` | Supported | Supported | Experimental |
| `$count` | Supported | Supported | Identical |
| `$sample` | Supported | Supported | Identical |

### Aggregation Example

**MongoDB:**
```javascript
const results = await db.orders.aggregate([
  { $match: { status: 'completed' } },
  { $group: {
    _id: '$customerId',
    totalSpent: { $sum: '$total' },
    orderCount: { $sum: 1 },
    avgOrderValue: { $avg: '$total' }
  }},
  { $sort: { totalSpent: -1 } },
  { $limit: 10 }
]).toArray()
```

**ParqueDB:**
```typescript
const results = await db.Order.aggregate([
  { $match: { status: 'completed' } },
  { $group: {
    _id: '$customerId',
    totalSpent: { $sum: '$total' },
    orderCount: { $sum: 1 },
    avgOrderValue: { $avg: '$total' }
  }},
  { $sort: { totalSpent: -1 } },
  { $limit: 10 }
])
// Returns array directly, no .toArray() needed
```

---

## Indexing Differences

### MongoDB Index Types

```javascript
// Single field
db.users.createIndex({ email: 1 })

// Compound
db.users.createIndex({ status: 1, createdAt: -1 })

// Unique
db.users.createIndex({ email: 1 }, { unique: true })

// Text
db.posts.createIndex({ title: 'text', content: 'text' })

// TTL
db.sessions.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3600 })
```

### ParqueDB Index Types

```typescript
const db = DB({
  User: {
    // Single field index (via # modifier)
    email: 'email#!',

    // Unique index (via ## modifier)
    username: 'string##!',

    // Hash index (O(1) lookups)
    apiKey: 'string#hash',

    // Full-text search
    bio: 'text#fts',

    // Vector similarity
    embedding: 'vector(1536)#vec',

    // Compound indexes defined at type level
    $indexes: [
      {
        name: 'user_status_date',
        fields: [
          { field: 'status' },
          { field: 'createdAt', direction: -1 }
        ]
      },
      {
        name: 'unique_email_per_org',
        fields: [{ field: 'orgId' }, { field: 'email' }],
        unique: true
      }
    ]
  }
})
```

### Key Indexing Differences

| Feature | MongoDB | ParqueDB |
|---------|---------|----------|
| Primary key | `_id` (ObjectId) | `$id` (ULID-based) |
| Auto-index on PK | Yes | Yes |
| Compound indexes | Runtime `createIndex()` | Schema `$indexes` |
| Full-text | `text` index type | `#fts` modifier |
| Unique constraint | Index option | `##` modifier |
| Sparse indexes | Index option | Schema `sparse: true` |
| TTL indexes | Index option | Planned |
| Column statistics | Not available | Automatic (Parquet) |
| Bloom filters | Not native | Automatic |

---

## Common Gotchas and Tips

### 1. ID Format Differences

**Gotcha:** MongoDB uses `_id` (ObjectId), ParqueDB uses `$id` (string).

```typescript
// Wrong
const user = await db.User.get(mongoId._id)

// Correct
const user = await db.User.get('users/01HQ3KPNM7...')
// Or just the ULID part
const user = await db.User.get('01HQ3KPNM7...')
```

### 2. Required $type and name

**Gotcha:** ParqueDB entities require `$type` and `name` (or `title`/`label`).

```typescript
// With typed schema, $type is auto-derived
const user = await db.User.create({
  name: 'Alice',  // Required for entity name
  email: 'alice@example.com'
})

// Without typed schema
const user = await db.Users.create({
  $type: 'User',  // Must specify
  name: 'Alice',  // Must have name/title/label
  email: 'alice@example.com'
})
```

### 3. Soft Delete is Default

**Gotcha:** `delete()` is soft delete by default.

```typescript
// Soft delete - entity still exists with deletedAt set
await db.User.delete(user.$id)

// Entity excluded from queries by default
const users = await db.User.find()  // Won't include deleted

// Include deleted in queries
const all = await db.User.find({}, { includeDeleted: true })

// Permanent delete
await db.User.delete(user.$id, { hard: true })
```

### 4. No Cursor Chaining

**Gotcha:** ParqueDB uses options object, not cursor methods.

```typescript
// MongoDB style (not supported)
const results = await db.users
  .find({ status: 'active' })
  .sort({ createdAt: -1 })
  .limit(10)
  .toArray()

// ParqueDB style
const results = await db.User.find(
  { status: 'active' },
  {
    sort: { createdAt: -1 },
    limit: 10
  }
)
```

### 5. Relationships vs References

**Gotcha:** Use relationships (`->`, `<-`) instead of storing ObjectIds.

```typescript
// MongoDB pattern (works but not recommended)
const post = await db.Post.create({
  title: 'Hello',
  authorId: user.$id  // Just storing ID
})

// ParqueDB pattern (recommended)
const db = DB({
  User: { posts: '<- Post.author[]' },
  Post: { author: '-> User.posts' }
})

const post = await db.Post.create({
  title: 'Hello',
  author: user.$id  // Creates bidirectional relationship
})

// Traverse relationships
const authorPosts = await db.Post.find({ author: user.$id })
```

### 6. Transactions

**Gotcha:** ParqueDB uses optimistic concurrency, not ACID transactions.

```typescript
// MongoDB transactions
const session = client.startSession()
await session.withTransaction(async () => {
  await users.updateOne({ _id: senderId }, { $inc: { balance: -100 } })
  await users.updateOne({ _id: receiverId }, { $inc: { balance: 100 } })
})

// ParqueDB optimistic concurrency
const sender = await db.User.get(senderId)
const receiver = await db.User.get(receiverId)

await db.User.update(senderId,
  { $inc: { balance: -100 } },
  { expectedVersion: sender.version }
)
await db.User.update(receiverId,
  { $inc: { balance: 100 } },
  { expectedVersion: receiver.version }
)
// If version mismatch, throws error - retry logic needed
```

### 7. Embedded vs Referenced Documents

**Gotcha:** ParqueDB stores nested objects in Variant, use separate entities for queryable data.

```typescript
// Embedded (stored in Variant - not independently queryable)
const user = await db.User.create({
  name: 'Alice',
  address: { city: 'SF', state: 'CA' }  // JSON field
})

// Referenced (recommended for queryable data)
const db = DB({
  User: { address: '-> Address.user' },
  Address: {
    city: 'string!',
    state: 'string!',
    user: '-> User.address'
  }
})
```

### 8. Array Updates

**Gotcha:** Array operators work similarly but check modifier support.

```typescript
// Supported modifiers with $push
await db.Post.update(post.$id, {
  $push: {
    tags: {
      $each: ['new', 'tags'],
      $position: 0,
      $slice: -10,
      $sort: 1
    }
  }
})

// $addToSet with $each
await db.Post.update(post.$id, {
  $addToSet: {
    tags: { $each: ['unique1', 'unique2'] }
  }
})
```

---

## Performance Considerations

### 1. Columnar Storage Benefits

ParqueDB uses Parquet columnar storage, which provides:

- **Predicate pushdown**: Filters evaluated at storage level
- **Column pruning**: Only requested columns are read
- **Statistics-based skipping**: Row groups skipped using min/max

```typescript
// Efficient - uses column statistics
const recent = await db.Post.find({
  createdAt: { $gte: new Date('2024-01-01') }
})

// Use projection to reduce data transfer
const titles = await db.Post.find(
  { status: 'published' },
  { project: { title: 1, createdAt: 1 } }
)
```

### 2. Shredding Hot Fields

Extract frequently-queried fields from Variant:

```typescript
const db = DB({
  Event: {
    // Shredded fields become separate Parquet columns
    $shred: ['eventType', 'timestamp', 'userId'],

    eventType: 'string!',
    timestamp: 'timestamp!',
    userId: 'uuid!',
    payload: 'json'  // Stays in Variant
  }
})
```

### 3. Index Strategy

```typescript
// Index for equality filters
email: 'email#!'  // # creates index

// Index for range queries (use compound)
$indexes: [{
  fields: [
    { field: 'status' },
    { field: 'createdAt', direction: -1 }
  ]
}]

// Full-text for search
content: 'text#fts'

// Hash for exact lookups
apiKey: 'string#hash'
```

### 4. Pagination Best Practices

```typescript
// Cursor pagination (recommended for large datasets)
const page1 = await db.Post.findPaginated(
  { status: 'published' },
  { limit: 20 }
)

if (page1.hasMore) {
  const page2 = await db.Post.findPaginated(
    { status: 'published' },
    { limit: 20, cursor: page1.nextCursor }
  )
}

// Avoid large skip values
// Bad: { skip: 10000, limit: 10 }
// Good: Use cursor pagination
```

### 5. Batch Operations

```typescript
// Parallel creates for independent operations
const users = await Promise.all(
  userData.map(data => db.User.create(data))
)

// Use updateMany for bulk updates
await db.Post.updateMany(
  { status: 'draft', createdAt: { $lt: cutoffDate } },
  { $set: { status: 'archived' } }
)

// Batch in chunks for very large operations
const BATCH_SIZE = 100
for (let i = 0; i < data.length; i += BATCH_SIZE) {
  const batch = data.slice(i, i + BATCH_SIZE)
  await Promise.all(batch.map(d => db.User.create(d)))
}
```

### 6. Time-Travel Queries

Unique to ParqueDB - query historical state:

```typescript
// Get entity state at a point in time
const pastUser = await db.User.get(userId, {
  asOf: new Date('2024-01-01')
})

// Query historical data
const pastPosts = await db.Post.find(
  { status: 'published' },
  { asOf: new Date('2024-01-01') }
)
```

---

## Migration Checklist

- [ ] Export MongoDB schema and sample documents
- [ ] Define ParqueDB schema with types and relationships
- [ ] Map MongoDB collection names to ParqueDB collections
- [ ] Convert `_id` references to relationships (`->`, `<-`)
- [ ] Update code to use ParqueDB API (`create` vs `insertOne`, etc.)
- [ ] Replace cursor chaining with options object
- [ ] Handle soft delete behavior (or add `{ hard: true }`)
- [ ] Test queries with ParqueDB filter operators
- [ ] Update aggregation pipelines (remove `.toArray()`)
- [ ] Configure indexes in schema (`#`, `##`, `#fts`, etc.)
- [ ] Migrate data using migration script
- [ ] Verify relationships work bidirectionally
- [ ] Test time-travel queries if needed
- [ ] Update error handling for ParqueDB error codes

---

## See Also

- [Getting Started](../getting-started.md) - Quick start guide
- [Collection API](../collection-api.md) - Complete API reference
- [Query API](../queries.md) - Filter operators
- [Update Operators](../update-operators.md) - Update operators
- [Schema Definition](../schemas.md) - Schema types and relationships
