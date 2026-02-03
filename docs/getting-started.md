---
title: Getting Started
description: Quick start guide for ParqueDB using the new DB() API
---

Get up and running with ParqueDB in minutes.

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

### Flexible Mode

The simplest way to get started - no schema required:

```typescript
import { db } from 'parquedb'

// Create entities in any collection
const post = await db.Posts.create({
  title: 'Hello World',
  content: 'My first post!',
  tags: ['intro', 'welcome']
})

// Query with MongoDB-style filters
const published = await db.Posts.find({ status: 'published' })

// Get by ID
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
await database.User.create({ email: 'alice@example.com', name: 'Alice' })
await database.Post.create({ title: 'Hello', content: 'World', author: 'users/alice' })
```

## Schema Notation

Field types use a concise string notation:

| Notation | Meaning |
|----------|---------|
| `string` | Optional string field |
| `string!` | Required field |
| `string#` | Indexed field |
| `string!#` | Required and indexed |
| `string?` | Explicitly optional |
| `int`, `float`, `boolean`, `date` | Other primitive types |
| `string[]` | Array of strings |
| `-> Target` | Forward relationship |
| `<- Target.field[]` | Reverse relationship |

Example schema with relationships:

```typescript
const db = DB({
  User: {
    email: 'email!#',
    name: 'string!',
    posts: '<- Post.author[]'    // reverse: all posts by this user
  },
  Post: {
    title: 'string!',
    content: 'markdown',
    author: '-> User.posts',     // forward: link to user
    tags: '-> Tag.posts[]'       // many-to-many
  },
  Tag: {
    name: 'string!#',
    posts: '<- Post.tags[]'
  }
})
```

## CRUD Operations

### Create

```typescript
const user = await db.Users.create({
  email: 'alice@example.com',
  name: 'Alice'
})

console.log(user.$id)       // 'users/abc123'
console.log(user.createdAt) // Date object
```

### Find

```typescript
// Find all
const allUsers = await db.Users.find()

// With filter
const active = await db.Users.find({ status: 'active' })

// With options
const page = await db.Users.find(
  { status: 'active' },
  { limit: 10, sort: { createdAt: -1 } }
)
```

### Get

```typescript
// By full ID
const user = await db.Users.get('users/abc123')

// Short ID (namespace inferred)
const user = await db.Users.get('abc123')

// Returns null if not found
const missing = await db.Users.get('nonexistent')  // null
```

### Update

```typescript
// $set - Set field values
await db.Users.update(user.$id, {
  $set: { name: 'Alice Smith', status: 'verified' }
})

// $inc - Increment numbers
await db.Posts.update(post.$id, {
  $inc: { viewCount: 1 }
})

// $push - Add to array
await db.Posts.update(post.$id, {
  $push: { tags: 'featured' }
})

// $link - Create relationship
await db.Posts.update(post.$id, {
  $link: { author: user.$id }
})
```

### Delete

```typescript
// Soft delete (default)
await db.Posts.delete(post.$id)

// Hard delete (permanent)
await db.Posts.delete(post.$id, { hard: true })
```

## SQL Support

ParqueDB includes a SQL template tag for familiar queries:

```typescript
import { db } from 'parquedb'

// Template literal syntax with parameter binding
const users = await db.sql`SELECT * FROM users WHERE age > ${21}`

// Complex queries
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

SQL can also be destructured from a DB instance:

```typescript
const { sql } = DB({
  User: { email: 'string!#', name: 'string' }
})

const admins = await sql`SELECT * FROM users WHERE role = ${'admin'}`
```

## Configuration File

Create `parquedb.config.ts` for persistent configuration:

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

    // Studio layout configuration
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

export default defineConfig({ storage: 'fs', schema })
```

```typescript
// src/db.ts
import { DB } from 'parquedb'
import { schema } from '../parquedb.config'

// Fully typed - TypeScript knows your collections
export const db = DB(schema)
export const { sql } = db

// Usage with full autocomplete
await db.User.create({ email: 'alice@example.com', name: 'Alice' })
await db.Post.find({ title: { $contains: 'Hello' } })
```

### Option 2: Generate Types

Generate typed exports from your config file:

```bash
# Generate to default location (src/db.generated.ts)
npx parquedb generate

# Custom output path
npx parquedb generate --output lib/database.ts
```

Then import the generated file:

```typescript
import { db, sql } from './db.generated'

// Fully typed!
await db.User.create({ email: 'bob@example.com', name: 'Bob' })
const users = await db.User.find({ role: 'admin' })
```

## ParqueDB Studio

Launch the admin interface:

```bash
# Auto-discover Parquet files
npx parquedb studio

# Specify directory
npx parquedb studio ./data

# Custom port, read-only mode
npx parquedb studio --port 8080 --read-only
```

See [Studio Documentation](./studio.md) for layout configuration and deployment.

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
