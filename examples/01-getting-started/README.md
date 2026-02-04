# Getting Started with ParqueDB

This example demonstrates the core ParqueDB workflow using realistic local storage.

## Run

```bash
npx tsx examples/01-getting-started/index.ts
```

## What it demonstrates

1. **Schema definition** - Typed collections with relationships
2. **CRUD operations** - Create, read, update, delete
3. **Relationships** - Forward (`-> User`) and reverse (`<- Post.author[]`) links
4. **Querying** - MongoDB-style filters returning paginated results
5. **Soft delete** - Deleted entities can be recovered

## Schema DSL

ParqueDB uses a concise schema notation:

| Notation | Meaning |
|----------|---------|
| `'string'` | Optional string field |
| `'string!'` | Required string field |
| `'string!#'` | Required + indexed |
| `'-> User'` | Forward relationship to User |
| `'<- Post.author[]'` | Reverse relationship (array) |

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

- [02-local-development](../02-local-development/) - Dev workflow patterns
- [03-branching](../03-branching/) - Git-style version control
- [04-sync-to-r2](../04-sync-to-r2/) - Cloud sync
