# Local Development Workflow

Best practices for local ParqueDB development.

## Run

```bash
npx tsx examples/02-local-development/index.ts
```

## What it demonstrates

1. **Schema definition** - Define schema in a separate file
2. **Configuration** - Use `defineConfig()` for type-safe config
3. **Runtime detection** - Detect Node.js vs Workers vs browser
4. **Development patterns** - Watch mode, hot reload

## Project Structure

Recommended structure for local development:

```
my-project/
├── .db/                    # Local database (add to .gitignore)
├── src/
│   ├── schema.ts           # Schema definitions
│   ├── config.ts           # Database configuration
│   └── index.ts            # Application entry
├── package.json
└── parquedb.config.ts      # Optional: config file
```

## Schema File

```typescript
// src/schema.ts
import { defineSchema } from 'parquedb'

export const schema = defineSchema({
  User: {
    email: 'string!#',
    name: 'string',
    posts: '<- Post.author[]'
  },
  Post: {
    title: 'string!',
    content: 'text',
    author: '-> User'
  }
})
```

## Config File

```typescript
// src/config.ts
import { defineConfig, FsBackend, R2Backend } from 'parquedb'

const isDev = process.env.NODE_ENV !== 'production'

export const config = defineConfig({
  storage: isDev
    ? new FsBackend('.db')
    : new R2Backend(process.env.R2_BUCKET!)
})
```

## Watch Mode

For rapid development with hot reload:

```bash
# Using tsx watch
npx tsx watch src/index.ts

# Using nodemon
npx nodemon --exec 'npx tsx' src/index.ts
```

## .gitignore

Add to your `.gitignore`:

```
.db/
*.parquet
```

## Environment Variables

```bash
# .env.local
NODE_ENV=development
DATABASE_PATH=.db
```

## Debugging

```bash
# View parquet file contents
npx parquet-tools cat .db/data/users/data.parquet

# View file metadata
npx parquet-tools meta .db/data/users/data.parquet

# List database files
ls -la .db/
```

## Next Steps

- [03-branching](../03-branching/) - Version control for database
- [04-sync-to-r2](../04-sync-to-r2/) - Sync to cloud storage
