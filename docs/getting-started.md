---
title: Getting Started
description: Quick start guide for ParqueDB using the new DB() API
---

Get up and running with ParqueDB in minutes.

## 5-Minute Tutorial

Here is a complete working example you can run immediately:

```typescript
import { DB, FsBackend } from 'parquedb'

// 1. Define your schema with $id and $name directives
const db = DB({
  User: {
    $id: 'email',           // Use email as entity ID
    $name: 'name',          // Use name as display name
    email: 'string!#',
    name: 'string!',
    role: 'string = "user"',
    posts: '<- Post.author[]'  // Reverse relationship
  },
  Post: {
    $id: 'slug',            // Use slug as entity ID
    $name: 'title',         // Use title as display name
    slug: 'string!#',
    title: 'string!',
    content: 'text',
    published: 'boolean = false',
    author: '-> User'       // Forward relationship to User
  }
}, { storage: new FsBackend('.db') })

// 2. Create some data
const alice = await db.User.create({
  email: 'alice@example.com',
  name: 'Alice'
})
console.log(alice.$id)  // 'user/alice@example.com'

const post = await db.Post.create({
  slug: 'getting-started',
  title: 'Getting Started with ParqueDB',
  content: 'ParqueDB is a document database built on Parquet...',
  author: 'alice@example.com'  // Auto-resolves to user/alice@example.com
})
console.log(post.$id)  // 'post/getting-started'

// 3. Query your data - returns T[] directly with $total
const published = await db.Post.find({ published: true })
console.log(`Found ${published.$total} published posts`)
for (const p of published) {
  console.log(`  - ${p.title}`)
}

// 4. Get entity - relationships are auto-hydrated
const fetchedPost = await db.Post.get('getting-started')
console.log('Author:', fetchedPost?.author?.name)  // 'Alice' - auto-hydrated!

// 5. Reverse relationships are also auto-hydrated
const user = await db.User.get('alice@example.com')
console.log(`${user?.name} has ${user?.posts?.$total} posts`)

// 6. Update data
await db.Post.update('getting-started', {
  $set: { published: true }
})

// 7. Use SQL if you prefer
const results = await db.sql`SELECT * FROM posts WHERE published = ${true}`
```

## Installation

```bash
npm install parquedb
```

Or with pnpm:

```bash
pnpm add parquedb
```

## Quick Start

ParqueDB offers two modes: flexible (schema-less) and typed (with schema).

**Collection Naming**:
- With the auto-configured `db` export: Use plural names (e.g., `db.Users`, `db.Posts`)
- With typed `DB()` factory: Use exact schema names (e.g., if schema defines `User`, use `database.User`)

### Flexible Mode

The simplest way to get started - no schema required:

```typescript
import { db } from 'parquedb'

// Create entities in any collection
// Note: Collection name is typically plural (Posts, Users, etc.)
const post = await db.Posts.create({
  title: 'Hello World',
  content: 'My first post!',
  tags: ['intro', 'welcome']
})

// Query with MongoDB-style filters
const published = await db.Posts.find({ status: 'published' })

// Get by ID (supports both full ID or short ID)
const found = await db.Posts.get(post.$id)
```

### Typed Schema

Define your data model upfront for validation and relationships:

```typescript
import { DB } from 'parquedb'

const database = DB({
  User: {
    email: 'string!#',     // required, indexed
    name: 'string',
    age: 'int?'
  },
  Post: {
    title: 'string!',
    content: 'string',
    author: '-> User'      // relationship to User
  }
})

// TypeScript knows the shape of your data
// Note: With typed schema, collection names match your schema exactly (User, not Users)
const user = await database.User.create({ email: 'alice@example.com', name: 'Alice' })
await database.Post.create({ title: 'Hello', content: 'World', author: user.$id })
```

## Schema Notation

Field types use a concise string notation:

| Notation | Meaning |
|----------|---------|
| `$id: 'field'` | Use field value as entity ID |
| `$name: 'field'` | Use field value as display name |
| `string` | Optional string field |
| `string!` | Required field |
| `string#` | Indexed field |
| `string!#` | Required and indexed |
| `string?` | Explicitly optional |
| `int`, `float`, `boolean`, `date` | Other primitive types |
| `string[]` | Array of strings |
| `-> Target` | Forward relationship |
| `<- Target.field[]` | Reverse relationship |

### $id and $name Directives

The `$id` directive lets you use a meaningful field value as the entity ID:

```typescript
User: {
  $id: 'email',       // alice@example.com → user/alice@example.com
  email: 'string!#',
}

Post: {
  $id: 'slug',        // hello-world → post/hello-world
  $name: 'title',     // "Hello World" becomes the display name
  slug: 'string!#',
  title: 'string!',
}
```

This enables intuitive lookups:
```typescript
const user = await db.User.get('alice@example.com')  // Uses email as ID
const post = await db.Post.get('hello-world')         // Uses slug as ID
```

Example schema with bidirectional relationships:

```typescript
const db = DB({
  User: {
    email: 'email!#',
    name: 'string!',
    posts: '<- Post.author[]'    // Reverse: all posts by this user
  },
  Post: {
    title: 'string!',
    content: 'markdown',
    author: '-> User.posts',     // Forward: link to user (one-to-many)
    tags: '-> Tag.posts[]'       // Forward: link to tags (many-to-many)
  },
  Tag: {
    name: 'string!#',
    posts: '<- Post.tags[]'      // Reverse: all posts with this tag
  }
})

// Relationships are bidirectional - traverse in either direction
const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })
const post = await db.Post.create({ title: 'Hello', content: 'World', author: user.$id })

// Forward traversal (Post -> User)
const author = await db.User.get(post.author)

// Reverse traversal (User -> Posts) - automatically indexed
const userPosts = await db.Post.find({ author: user.$id })
```

## Entity Structure

All entities in ParqueDB have built-in fields that are automatically managed:

| Field | Type | Description |
|-------|------|-------------|
| `$id` | string | Unique identifier (format: `namespace/ulid`) |
| `$type` | string | Entity type (e.g., 'User', 'Post') - auto-derived from collection name |
| `name` | string | Human-readable name - auto-derived from `name`, `title`, or `label` fields |
| `createdAt` | Date | Creation timestamp |
| `createdBy` | string | Creator entity ID |
| `updatedAt` | Date | Last update timestamp |
| `updatedBy` | string | Last updater entity ID |
| `deletedAt` | Date? | Soft delete timestamp (null if not deleted) |
| `deletedBy` | string? | Deleter entity ID (null if not deleted) |

**Important**: When creating entities, provide a `name`, `title`, or `label` field - ParqueDB will automatically use this for the entity's `name` property.

## CRUD Operations

### Create

```typescript
const user = await db.Users.create({
  email: 'alice@example.com',
  name: 'Alice'
})

console.log(user.$id)       // 'users/abc123' (namespace/ulid format)
console.log(user.$type)     // 'User' (auto-derived from collection name)
console.log(user.name)      // 'Alice'
console.log(user.createdAt) // Date object
```

### Find

`find()` returns a `ResultArray<T>` - a standard array you can iterate directly, with additional metadata accessible via proxy properties:

```typescript
// Find all entities - iterate directly (no .items needed)
const allUsers = await db.Users.find()
for (const user of allUsers) {
  console.log(user.name)
}

// Access pagination metadata via proxy
console.log(allUsers.$total)   // Total count
console.log(allUsers.$next)    // Cursor for next page

// Filter with exact match
const active = await db.Users.find({ status: 'active' })
console.log(`Found ${active.$total} active users`)

// Filter with operators (MongoDB-style)
const adults = await db.Users.find({ age: { $gte: 18 } })

// Multiple conditions (AND)
const activeAdults = await db.Users.find({
  status: 'active',
  age: { $gte: 18 }
})

// With pagination and sorting
const page = await db.Users.find(
  { status: 'active' },
  {
    limit: 10,
    skip: 20,
    sort: { createdAt: -1 }  // -1 for descending, 1 for ascending
  }
)

// Cursor-based pagination using $next
if (page.$next) {
  const nextPage = await db.Users.find(
    { status: 'active' },
    { limit: 10, cursor: page.$next }
  )
}

// Projection (select specific fields)
const names = await db.Users.find(
  {},
  { project: { name: 1, email: 1 } }  // Only return name and email
)
```

### Get

`get()` returns entities with relationships automatically hydrated:

```typescript
// By full ID
const user = await db.Users.get('users/abc123')

// By short ID (namespace inferred from collection)
const sameUser = await db.Users.get('abc123')

// With $id directive, use the field value directly
const user = await db.Users.get('alice@example.com')  // When $id: 'email'
const post = await db.Posts.get('hello-world')         // When $id: 'slug'

// Returns null if not found
const missing = await db.Users.get('nonexistent')  // null

// Relationships are auto-hydrated
const post = await db.Posts.get('hello-world')
console.log(post.author.name)      // 'Alice' - fully hydrated, not just an ID!

// Reverse relationships are arrays with metadata
const user = await db.Users.get('alice@example.com')
console.log(user.posts.$total)     // Total count of posts
for (const p of user.posts) {
  console.log(p.title)             // First 10 posts (default limit)
}
```

### Update

ParqueDB supports MongoDB-style update operators:

```typescript
// $set - Set or update field values
await db.Users.update(user.$id, {
  $set: { name: 'Alice Smith', status: 'verified' }
})

// $inc - Increment or decrement numeric values
await db.Posts.update(post.$id, {
  $inc: { viewCount: 1, likes: -1 }
})

// $push - Add item(s) to an array
await db.Posts.update(post.$id, {
  $push: { tags: 'featured' }  // Single item
})

await db.Posts.update(post.$id, {
  $push: { tags: ['featured', 'popular'] }  // Multiple items
})

// $unset - Remove fields
await db.Users.update(user.$id, {
  $unset: { temporaryFlag: true }
})

// $link - Create a relationship
await db.Posts.update(post.$id, {
  $link: { author: user.$id }
})
```

### Delete

```typescript
// Soft delete (default) - sets deletedAt timestamp
await db.Posts.delete(post.$id)

// Hard delete (permanent) - completely removes the entity
await db.Posts.delete(post.$id, { hard: true })

// Soft-deleted entities are excluded from queries by default
const posts = await db.Posts.find()  // Won't include soft-deleted posts
```

## SQL Support

ParqueDB includes a SQL template tag for familiar queries. SQL queries are automatically translated to the underlying ParqueDB operations:

```typescript
import { db } from 'parquedb'

// Template literal syntax with automatic parameter binding
const users = await db.sql`SELECT * FROM users WHERE age > ${21}`

// Complex queries with joins and aggregations
const results = await db.sql`
  SELECT u.name, COUNT(p.$id) as post_count
  FROM users u
  LEFT JOIN posts p ON p.author = u.$id
  WHERE u.status = ${'active'}
  GROUP BY u.name
  ORDER BY post_count DESC
  LIMIT ${10}
`
```

SQL can also be destructured from a typed DB instance:

```typescript
import { DB } from 'parquedb'

const database = DB({
  User: { email: 'string!#', name: 'string', role: 'string' }
})

// Destructure sql for direct use
const { sql } = database

const admins = await sql`SELECT * FROM users WHERE role = ${'admin'}`
```

## Configuration File

Create `parquedb.config.ts` in your project root for persistent configuration. The auto-configured `db` and `sql` exports will automatically detect and load this configuration:

```typescript
import { defineConfig, defineSchema } from 'parquedb/config'

export const schema = defineSchema({
  User: {
    email: 'string!#',
    name: 'string',
    role: 'string'
  },
  Post: {
    title: 'string!',
    content: 'text',
    status: 'string',
    author: '-> User',

    // Optional: Studio layout configuration
    $layout: [['title'], 'content'],
    $sidebar: ['$id', 'status', 'createdAt'],
    $studio: {
      label: 'Blog Posts',
      status: { options: ['draft', 'published'] }
    }
  }
})

export default defineConfig({
  storage: { type: 'fs', path: './data' },
  schema,
  studio: {
    port: 3000,
    theme: 'auto'
  }
})
```

With this configuration file in place, `db` and `sql` imports automatically use your schema and storage settings:

```typescript
import { db, sql } from 'parquedb'

// Uses the schema and storage from parquedb.config.ts
await db.Users.create({ email: 'user@example.com', name: 'User' })
const users = await sql`SELECT * FROM users`
```

## Type-Safe Imports

ParqueDB provides two approaches for full TypeScript type safety:

### Option 1: Schema Export (Recommended)

Export and reuse your schema for type inference:

```typescript
// parquedb.config.ts
import { defineConfig, defineSchema } from 'parquedb/config'

export const schema = defineSchema({
  User: { email: 'string!#', name: 'string' },
  Post: { title: 'string!', author: '-> User' }
})

export default defineConfig({
  storage: { type: 'fs', path: './data' },
  schema
})
```

```typescript
// src/db.ts
import { DB } from 'parquedb'
import { schema } from '../parquedb.config'

// Fully typed - TypeScript knows your collection names and fields
export const db = DB(schema)
export const { sql } = db

// Usage with full autocomplete and type checking
await db.User.create({ email: 'alice@example.com', name: 'Alice' })
const posts = await db.Post.find({ title: { $contains: 'Hello' } })
```

### Option 2: Generate Types

Generate typed exports from your `parquedb.config.ts` file:

```bash
# Generate to default location (src/db.generated.ts)
npx parquedb generate

# Custom output path
npx parquedb generate --output lib/database.ts
```

Then import the generated, fully-typed database instance:

```typescript
import { db, sql } from './db.generated'

// Fully typed with autocomplete and type checking
await db.User.create({ email: 'bob@example.com', name: 'Bob' })
const users = await db.User.find({ role: 'admin' })

// SQL is also fully typed
const result = await sql`SELECT * FROM users WHERE role = ${'admin'}`
```

## ParqueDB Studio

Launch the admin interface to browse and edit your data:

```bash
# Auto-discover Parquet files in current directory
npx parquedb studio

# Specify data directory
npx parquedb studio ./data

# Custom port
npx parquedb studio --port 8080

# Read-only mode (prevents edits)
npx parquedb studio --read-only
```

Studio automatically uses your `parquedb.config.ts` if present, including schema definitions and layout configurations.

See [Studio Documentation](./studio.md) for advanced layout configuration and deployment options.

## Storage Backends

ParqueDB supports multiple storage backends for different environments:

```typescript
import { DB, MemoryBackend, FsBackend, R2Backend } from 'parquedb'

// In-memory (testing/development)
const memDb = DB({ schema: 'flexible' }, {
  storage: new MemoryBackend()
})

// Filesystem (Node.js)
const fsDb = DB({ schema: 'flexible' }, {
  storage: new FsBackend('./data')
})

// Cloudflare R2 (Workers)
const r2Db = DB({ schema: 'flexible' }, {
  storage: new R2Backend(env.BUCKET)
})
```

In `parquedb.config.ts`, you can configure storage using shortcuts:

```typescript
export default defineConfig({
  storage: 'fs',  // Uses default ./data directory
  // or
  storage: { type: 'fs', path: './my-data' },
  // or
  storage: { type: 'memory' },
  schema
})
```

## Quick Reference

| Operation | Code Example |
|-----------|--------------|
| **Create** | `await db.Users.create({ name: 'Alice', email: 'alice@example.com' })` |
| **Find all** | `await db.Users.find()` |
| **Find with filter** | `await db.Users.find({ status: 'active' })` |
| **Find with operators** | `await db.Users.find({ age: { $gte: 18 } })` |
| **Get by ID** | `await db.Users.get('users/abc123')` |
| **Update** | `await db.Users.update(id, { $set: { name: 'Bob' } })` |
| **Delete (soft)** | `await db.Users.delete(id)` |
| **Delete (hard)** | `await db.Users.delete(id, { hard: true })` |
| **SQL query** | `await db.sql\`SELECT * FROM users WHERE age > \${21}\`` |
| **Pagination** | `await db.Users.find({}, { limit: 10, skip: 20 })` |
| **Sorting** | `await db.Users.find({}, { sort: { createdAt: -1 } })` |
| **Projection** | `await db.Users.find({}, { project: { name: 1, email: 1 } })` |

## Practical Examples

### Building a Blog API

```typescript
import { DB } from 'parquedb'

const db = DB({
  Author: {
    email: 'email!#',
    name: 'string!',
    bio: 'text',
    posts: '<- Post.author[]'
  },
  Post: {
    title: 'string!',
    slug: 'string!#',
    content: 'markdown!',
    status: 'enum(draft,published,archived) = "draft"',
    publishedAt: 'datetime',
    author: '-> Author.posts',
    tags: '-> Tag.posts[]'
  },
  Tag: {
    name: 'string!#',
    posts: '<- Post.tags[]'
  }
})

// Create an author
const author = await db.Author.create({
  email: 'writer@blog.com',
  name: 'Jane Writer',
  bio: 'Tech blogger and coffee enthusiast'
})

// Create tags
const techTag = await db.Tag.create({ name: 'Technology' })
const tutorialTag = await db.Tag.create({ name: 'Tutorial' })

// Create a published post with relationships
const post = await db.Post.create({
  title: 'Introduction to ParqueDB',
  slug: 'intro-to-parquedb',
  content: '# Welcome\n\nParqueDB is a modern database...',
  status: 'published',
  publishedAt: new Date(),
  author: author.$id,
  tags: [techTag.$id, tutorialTag.$id]
})

// Query published posts with pagination
const publishedPosts = await db.Post.find(
  { status: 'published' },
  {
    sort: { publishedAt: -1 },
    limit: 10,
    skip: 0
  }
)

// Find posts by tag
const techPosts = await db.Post.find({
  tags: { $in: [techTag.$id] }
})
```

### E-commerce Product Catalog

```typescript
const db = DB({
  Category: {
    name: 'string!',
    slug: 'string!#',
    parent: '-> Category.children',
    children: '<- Category.parent[]',
    products: '<- Product.category[]'
  },
  Product: {
    name: 'string!',
    sku: 'string!#',
    price: 'decimal(10,2)!',
    stock: 'int = 0',
    description: 'text',
    category: '-> Category.products'
  }
})

// Create category hierarchy
const electronics = await db.Category.create({ name: 'Electronics', slug: 'electronics' })
const phones = await db.Category.create({
  name: 'Phones',
  slug: 'phones',
  parent: electronics.$id
})

// Create products
await db.Product.create({
  name: 'Smartphone Pro',
  sku: 'PHONE-001',
  price: 999.99,
  stock: 50,
  category: phones.$id
})

// Find products in stock, sorted by price
const inStock = await db.Product.find(
  { stock: { $gt: 0 } },
  { sort: { price: 1 } }
)

// Update stock after sale
await db.Product.update('products/abc123', {
  $inc: { stock: -1 }
})
```

### User Authentication System

```typescript
const db = DB({
  User: {
    email: 'email!#',
    passwordHash: 'string!',
    name: 'string!',
    role: 'enum(user,admin,moderator) = "user"',
    emailVerified: 'boolean = false',
    lastLoginAt: 'datetime',
    sessions: '<- Session.user[]'
  },
  Session: {
    token: 'string!#',
    expiresAt: 'datetime!',
    userAgent: 'string',
    ipAddress: 'string',
    user: '-> User.sessions'
  }
})

// Find user by email for login
const user = await db.User.find({ email: 'user@example.com' })

// Create session on successful login
const session = await db.Session.create({
  token: crypto.randomUUID(),
  expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000), // 7 days
  userAgent: request.headers.get('user-agent'),
  ipAddress: request.headers.get('cf-connecting-ip'),
  user: user[0].$id
})

// Update last login time
await db.User.update(user[0].$id, {
  $set: { lastLoginAt: new Date() }
})

// Find and validate session
const [activeSession] = await db.Session.find({
  token: sessionToken,
  expiresAt: { $gt: new Date() }
})
```

## Next Steps

Now that you have the basics, explore these topics:

- [Schema Definition](./schemas.md) - Complete guide to types, validation, and relationships
- [Query API](./queries.md) - MongoDB-style filtering, sorting, and pagination
- [Update Operators](./updates.md) - All available update operators ($set, $inc, $push, etc.)

### Tools & Integrations

- [ParqueDB Studio](./studio.md) - Admin UI for viewing and editing data
- [Payload CMS Adapter](./integrations/payload.md) - Use ParqueDB with Payload CMS

### Deployment Guides

- [Cloudflare Workers](./deployment/cloudflare-workers.md) - Deploy to the edge with R2 storage
- [Node.js Standalone](./deployment/node-standalone.md) - Run as a standalone server
- [R2 Setup](./deployment/r2-setup.md) - Configure Cloudflare R2 storage

### Architecture

- [Graph-First Architecture](./architecture/graph-first-architecture.md) - How relationships are indexed
- [Secondary Indexes](./architecture/secondary-indexes.md) - B-tree, hash, and full-text indexes
- [Bloom Filter Indexes](./architecture/bloom-filter-indexes.md) - Probabilistic existence checks
