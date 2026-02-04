# Getting Started with ParqueDB

This example demonstrates the core ParqueDB workflow using realistic local storage.

## Run

```bash
npx tsx examples/01-getting-started/index.ts
```

## What it demonstrates

1. **Schema definition** - Typed collections with `$id` and `$name` directives
2. **CRUD operations** - Create, read, update, delete
3. **Relationships** - Forward (`-> User`) and reverse (`<- Post.author[]`) links
4. **Elegant array results** - `find()` returns `T[]` directly with `$total`, `$next` metadata
5. **Auto-hydrated relationships** - Forward and reverse relationships populated on `get()`
6. **Soft delete** - Deleted entities can be recovered

## Schema DSL

ParqueDB uses a concise schema notation:

| Notation | Meaning |
|----------|---------|
| `$id: 'email'` | Use `email` field as entity ID |
| `$name: 'title'` | Use `title` field as display name |
| `'string'` | Optional string field |
| `'string!'` | Required string field |
| `'string!#'` | Required + indexed |
| `'-> User'` | Forward relationship to User |
| `'<- Post.author[]'` | Reverse relationship (array) |

The `$id` and `$name` directives allow using human-readable fields:

```typescript
Post: {
  $id: 'slug',        // hello-world becomes $id: post/hello-world
  $name: 'title',     // "Hello World" becomes name: "Hello World"
  slug: 'string!#',
  title: 'string!',
}
```

## Elegant Array Results

Query results are plain arrays with metadata accessible via proxy:

```typescript
const published = await db.Post.find({ status: 'published' })

// Direct iteration - no .items needed
for (const post of published) {
  console.log(post.title)
}

// Metadata via proxy
console.log(published.$total)    // Total count
console.log(published.$next)     // Cursor for next page
console.log(published.length)    // Array length (current page)
```

## Auto-Hydrated Relationships

Entities returned from `get()` have relationships automatically populated:

```typescript
// Forward relationships are fully hydrated
const post = await db.Post.get('hello-world')
console.log(post.author.name)     // 'Alice' - not just an ID

// Reverse relationships are arrays with pagination
const user = await db.User.get('alice@example.com')
console.log(user.posts.$total)    // Total posts count
for (const p of user.posts) {
  console.log(p.title)            // First 10 posts (default limit)
}
```

## Output

After running, you'll see a `.db/` directory containing Parquet files:

```
.db/
  data/users/data.parquet
  data/posts/data.parquet
  rels/forward/posts.parquet
  rels/reverse/users.parquet
```

## Next Steps

- [02-storage-modes](../02-storage-modes/) - Typed vs Flexible storage
- [03-sql](../03-sql/) - SQL queries
- [04-branching](../04-branching/) - Git-style version control
- [05-sync-to-r2](../05-sync-to-r2/) - Cloud sync
