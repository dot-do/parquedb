# SQL Integration

ParqueDB provides first-class SQL support through a tagged template literal, plus adapters for popular ORMs like Drizzle and Prisma.

## SQL Tagged Template

The simplest way to use SQL with ParqueDB. Write standard SQL with automatic parameter binding:

```typescript
import { db, sql } from 'parquedb'

// Simple queries with inline parameters
const users = await sql`SELECT * FROM users WHERE status = ${'active'}`

// Multiple parameters
const posts = await sql`
  SELECT * FROM posts
  WHERE author_id = ${userId}
  AND created_at > ${startDate}
  ORDER BY created_at DESC
  LIMIT ${10}
`

// Access results
console.log(posts.rows)      // Array of matching records
console.log(posts.rowCount)  // Number of rows returned
console.log(posts.command)   // 'SELECT', 'INSERT', 'UPDATE', or 'DELETE'
```

### All CRUD Operations

```typescript
// SELECT with WHERE, ORDER BY, LIMIT
const result = await sql`
  SELECT id, name, email
  FROM users
  WHERE age > ${21} AND status = ${'active'}
  ORDER BY created_at DESC
  LIMIT ${20} OFFSET ${0}
`

// INSERT
const inserted = await sql`
  INSERT INTO posts (title, content, author_id)
  VALUES (${'My Post'}, ${'Content here'}, ${userId})
`

// UPDATE
const updated = await sql`
  UPDATE users
  SET status = ${'inactive'}, updated_at = ${new Date()}
  WHERE id = ${userId}
`

// DELETE
const deleted = await sql`
  DELETE FROM posts WHERE id = ${postId}
`
```

### WHERE Clause Support

The SQL parser supports standard SQL operators:

```typescript
// Comparison operators
await sql`SELECT * FROM users WHERE age > ${25}`
await sql`SELECT * FROM users WHERE age >= ${25}`
await sql`SELECT * FROM users WHERE age < ${30}`
await sql`SELECT * FROM users WHERE age <= ${30}`
await sql`SELECT * FROM users WHERE age != ${25}`

// IN clause
await sql`SELECT * FROM users WHERE status IN ('active', 'pending')`

// NULL checks
await sql`SELECT * FROM users WHERE deleted_at IS NULL`
await sql`SELECT * FROM users WHERE email IS NOT NULL`

// LIKE patterns
await sql`SELECT * FROM users WHERE name LIKE ${'%john%'}`

// AND/OR conditions
await sql`SELECT * FROM users WHERE status = ${'active'} AND age > ${21}`
await sql`SELECT * FROM users WHERE role = ${'admin'} OR role = ${'moderator'}`
```

### Raw Queries

For dynamic SQL or when you need more control:

```typescript
const sql = createSQL(db)

// Using .raw() with parameter array
const users = await sql.raw(
  'SELECT * FROM users WHERE status = $1 AND age > $2',
  ['active', 25]
)

// Build query separately
import { buildQuery } from 'parquedb/sql'

const { query, params } = buildQuery`SELECT * FROM users WHERE age > ${25}`
// query: "SELECT * FROM users WHERE age > $1"
// params: [25]
```

### Configuration

```typescript
import { createSQL } from 'parquedb/sql'
import { ParqueDB } from 'parquedb'

const db = new ParqueDB({ storage: backend })
const sql = createSQL(db, {
  debug: true,          // Log all queries
  actor: 'users/admin', // Default actor for mutations
})
```

## Drizzle ORM Integration

Use ParqueDB as a backend for [Drizzle ORM](https://orm.drizzle.team/):

```typescript
import { drizzle } from 'drizzle-orm/pg-proxy'
import { createDrizzleProxy } from 'parquedb/sql'
import { ParqueDB } from 'parquedb'

// Create ParqueDB instance
const parquedb = new ParqueDB({ storage: backend })

// Create Drizzle proxy
const proxy = createDrizzleProxy(parquedb, {
  debug: true,           // Log SQL queries
  actor: 'system/app',   // Actor for audit fields
})

// Initialize Drizzle with the proxy
const db = drizzle(proxy)

// Now use Drizzle normally!
const users = await db.select().from(users).where(eq(users.status, 'active'))
```

### Define Your Schema

```typescript
import { pgTable, text, integer, timestamp } from 'drizzle-orm/pg-core'

export const users = pgTable('users', {
  id: text('id').primaryKey(),
  name: text('name').notNull(),
  email: text('email').notNull(),
  age: integer('age'),
  status: text('status').default('active'),
  createdAt: timestamp('created_at').defaultNow(),
})

export const posts = pgTable('posts', {
  id: text('id').primaryKey(),
  title: text('title').notNull(),
  content: text('content'),
  authorId: text('author_id').references(() => users.id),
})
```

### Drizzle Queries

```typescript
import { eq, gt, and, or, like, isNull } from 'drizzle-orm'

// Select with conditions
const activeUsers = await db
  .select()
  .from(users)
  .where(eq(users.status, 'active'))

// Complex queries
const results = await db
  .select()
  .from(users)
  .where(
    and(
      gt(users.age, 21),
      or(
        eq(users.status, 'active'),
        eq(users.status, 'pending')
      )
    )
  )
  .orderBy(users.createdAt)
  .limit(20)

// Insert
await db.insert(users).values({
  id: 'user-123',
  name: 'Alice',
  email: 'alice@example.com',
})

// Update
await db
  .update(users)
  .set({ status: 'inactive' })
  .where(eq(users.id, 'user-123'))

// Delete
await db.delete(users).where(eq(users.id, 'user-123'))
```

## Prisma Adapter

Use ParqueDB with [Prisma](https://www.prisma.io/) via the driver adapter interface:

```typescript
import { PrismaClient } from '@prisma/client'
import { createPrismaAdapter } from 'parquedb/sql'
import { ParqueDB } from 'parquedb'

// Create ParqueDB instance
const parquedb = new ParqueDB({ storage: backend })

// Create Prisma adapter
const adapter = createPrismaAdapter(parquedb, {
  debug: true,           // Log SQL queries
  actor: 'system/app',   // Actor for audit fields
})

// Initialize Prisma with the adapter
const prisma = new PrismaClient({ adapter })

// Use Prisma normally!
const users = await prisma.user.findMany({
  where: { status: 'active' },
})
```

### Prisma Schema

```prisma
// schema.prisma
generator client {
  provider        = "prisma-client-js"
  previewFeatures = ["driverAdapters"]
}

datasource db {
  provider = "sqlite"
  url      = "file:./dev.db"
}

model User {
  id        String   @id
  name      String
  email     String   @unique
  status    String   @default("active")
  createdAt DateTime @default(now())
  posts     Post[]
}

model Post {
  id        String   @id
  title     String
  content   String?
  author    User     @relation(fields: [authorId], references: [id])
  authorId  String
}
```

### Prisma Queries

```typescript
// Find many with filtering
const users = await prisma.user.findMany({
  where: {
    status: 'active',
    age: { gt: 21 },
  },
  orderBy: { createdAt: 'desc' },
  take: 20,
})

// Find unique
const user = await prisma.user.findUnique({
  where: { email: 'alice@example.com' },
})

// Create
const newUser = await prisma.user.create({
  data: {
    id: 'user-123',
    name: 'Alice',
    email: 'alice@example.com',
  },
})

// Update
await prisma.user.update({
  where: { id: 'user-123' },
  data: { status: 'inactive' },
})

// Delete
await prisma.user.delete({
  where: { id: 'user-123' },
})

// Raw queries
const result = await prisma.$queryRaw`
  SELECT * FROM users WHERE age > ${25}
`
```

### Transactions

```typescript
const adapter = createPrismaAdapter(parquedb)
const transaction = await adapter.startTransaction()

try {
  await transaction.executeRaw({
    sql: 'INSERT INTO users (name, email) VALUES ($1, $2)',
    args: ['Alice', 'alice@example.com'],
  })

  await transaction.executeRaw({
    sql: 'INSERT INTO posts (title, author_id) VALUES ($1, $2)',
    args: ['First Post', 'user-123'],
  })

  await transaction.commit()
} catch (error) {
  await transaction.rollback()
  throw error
}
```

> **Note**: ParqueDB uses append-only Parquet storage. True ACID rollback is not supported for Parquet files. For full transactional semantics, use the SQLite backend in Workers.

## How It Works

Under the hood, ParqueDB's SQL integration:

1. **Parses** SQL statements into an AST
2. **Translates** to ParqueDB's MongoDB-style query format
3. **Executes** against collections with predicate pushdown
4. **Returns** results in the expected format for each ORM

### SQL → ParqueDB Translation

| SQL | ParqueDB Filter |
|-----|-----------------|
| `WHERE status = 'active'` | `{ status: 'active' }` |
| `WHERE age > 25` | `{ age: { $gt: 25 } }` |
| `WHERE age >= 25` | `{ age: { $gte: 25 } }` |
| `WHERE status IN ('a', 'b')` | `{ status: { $in: ['a', 'b'] } }` |
| `WHERE name LIKE '%john%'` | `{ name: { $regex: 'john' } }` |
| `WHERE deleted_at IS NULL` | `{ deleted_at: { $exists: false } }` |
| `WHERE a = 1 AND b = 2` | `{ a: 1, b: 2 }` |
| `WHERE a = 1 OR b = 2` | `{ $or: [{ a: 1 }, { b: 2 }] }` |

### Audit Fields

All mutations automatically populate audit fields when an actor is provided:

```typescript
const sql = createSQL(db, { actor: 'users/admin' })

await sql`INSERT INTO posts (title) VALUES (${'My Post'})`
// Creates post with:
// - createdAt: current timestamp
// - createdBy: 'users/admin'
// - updatedAt: current timestamp
// - updatedBy: 'users/admin'
// - version: 1
```

## Best Practices

1. **Use the SQL template tag** for simple queries - it's the most ergonomic
2. **Use Drizzle** when you want type-safe queries and migrations
3. **Use Prisma** when you want a full ORM with relations and the Prisma ecosystem
4. **Always set an actor** for mutations to track who made changes
5. **Enable debug mode** during development to see translated queries
