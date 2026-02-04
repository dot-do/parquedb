# ParqueDB Cloudflare Worker Example

A REST API for a blog using ParqueDB with R2 storage.

## Setup

```bash
# Install dependencies
pnpm install

# Create R2 bucket
wrangler r2 bucket create parquedb-data

# Start local development
pnpm dev
```

## Project Structure

```
src/
  db.ts        # Schema definition (required in Workers - no config file support)
  index.ts     # Worker handler - re-exports ParqueDBDO
wrangler.jsonc # R2 + DO bindings
```

### Why db.ts in Workers?

Workers don't have filesystem access, so `parquedb.config.ts` doesn't work.
Define your schema with `DB()` in `db.ts`:

```ts
// src/db.ts
import { DB } from 'parquedb'

export const db = DB({
  User: {
    $id: 'email',
    $name: 'name',
    email: 'string!#',
    name: 'string!',
    posts: '<- Post.author[]'
  },
  Post: {
    $id: 'slug',
    $name: 'title',
    slug: 'string!#',
    title: 'string!',
    author: '-> User'
  }
})
```

No storage configuration needed - ParqueDB auto-detects R2 and DO bindings.

### index.ts - Worker Handler

```ts
import { db } from './db'

// Re-export ParqueDBDO for Cloudflare binding
export { ParqueDBDO } from 'parquedb/worker'

export default {
  async fetch(request: Request): Promise<Response> {
    const post = await db.Post.get('hello-world')
    return Response.json({
      title: post?.title,
      author: post?.author?.name  // Auto-hydrated
    })
  }
}
```

## Endpoints

```bash
# List published posts
curl http://localhost:8787/posts

# Get single post (auto-hydrates author)
curl http://localhost:8787/posts/hello-world

# Create new post
curl -X POST http://localhost:8787/posts \
  -H "Content-Type: application/json" \
  -d '{"slug":"my-post","title":"My Post","content":"Hello!","author":"alice@example.com"}'

# Get user with their posts
curl http://localhost:8787/users/alice%40example.com
```

## wrangler.jsonc

```jsonc
{
  "r2_buckets": [{ "binding": "BUCKET", "bucket_name": "parquedb-data" }],
  "durable_objects": {
    "bindings": [{ "name": "PARQUEDB", "class_name": "ParqueDBDO" }]
  },
  "migrations": [{ "tag": "v1", "new_classes": ["ParqueDBDO"] }]
}
```

## Deploy

```bash
pnpm deploy
```

## Comparison with Next.js

| Feature | Worker | Next.js |
|---------|--------|---------|
| Config file | No (no FS access) | Yes (`parquedb.config.ts`) |
| Schema location | `src/db.ts` with `DB()` | `parquedb.config.ts` |
| Import | `import { db } from './db'` | `import { db } from 'parquedb'` |
| Storage | Auto-detects R2 | Auto-detects `.parquedb/` |
| DO export | Required | Not needed |
