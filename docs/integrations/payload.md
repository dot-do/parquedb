---
title: Payload CMS Adapter
description: Use ParqueDB as the database backend for Payload CMS
---

# Payload CMS Database Adapter

ParqueDB provides a database adapter for [Payload CMS](https://payloadcms.com/), enabling you to use Parquet files as your content storage. This is ideal for:

- **Edge deployments**: Deploy Payload to Cloudflare Workers with R2 storage
- **Portable content**: Data stored in standard Parquet format
- **Analytics-ready**: Query content with SQL tools like DuckDB
- **Version control**: Store content alongside code in git

## Installation

```bash
npm install parquedb payload
```

## Quick Start

```typescript
// payload.config.ts
import { buildConfig } from 'payload'
import { parquedbAdapter } from 'parquedb/payload'
import { MemoryBackend } from 'parquedb'

export default buildConfig({
  db: parquedbAdapter({
    storage: new MemoryBackend()
  }),

  collections: [
    {
      slug: 'posts',
      fields: [
        { name: 'title', type: 'text', required: true },
        { name: 'content', type: 'richText' },
        { name: 'status', type: 'select', options: ['draft', 'published'] }
      ]
    }
  ]
})
```

## Configuration

### Adapter Options

```typescript
parquedbAdapter({
  // Required: Storage backend
  storage: new FsBackend('./data'),

  // Optional: Collection for Payload migrations (default: '_payload_migrations')
  migrationCollection: '_payload_migrations',

  // Optional: Collection for global documents (default: '_payload_globals')
  globalsCollection: '_payload_globals'
})
```

### Storage Backends

#### Filesystem (Node.js)

```typescript
import { FsBackend } from 'parquedb'

parquedbAdapter({
  storage: new FsBackend('./data')
})
```

#### Cloudflare R2

```typescript
import { R2Backend } from 'parquedb'

// In your Worker
export default {
  async fetch(request, env) {
    const config = buildConfig({
      db: parquedbAdapter({
        storage: new R2Backend(env.BUCKET)
      }),
      // ...
    })
  }
}
```

#### Memory (Testing)

```typescript
import { MemoryBackend } from 'parquedb'

parquedbAdapter({
  storage: new MemoryBackend()
})
```

## Features

### Full CRUD Support

The adapter implements all Payload database operations:

- `create` - Create documents
- `find` / `findOne` - Query with filters
- `updateOne` / `updateMany` - Update documents
- `deleteOne` / `deleteMany` - Delete documents
- `count` - Count matching documents

### Filter Translation

Payload's `where` clauses are translated to ParqueDB filters:

| Payload | ParqueDB |
|---------|----------|
| `equals` | `$eq` |
| `not_equals` | `$ne` |
| `greater_than` | `$gt` |
| `greater_than_equal` | `$gte` |
| `less_than` | `$lt` |
| `less_than_equal` | `$lte` |
| `in` | `$in` |
| `not_in` | `$nin` |
| `exists` | `$exists` |
| `contains` | `$contains` |
| `like` | `$regex` |
| `and` / `or` | `$and` / `$or` |

### Transactions

```typescript
const session = await payload.db.beginTransaction()

try {
  await payload.create({
    collection: 'posts',
    data: { title: 'New Post' },
    req: { transactionID: session }
  })

  await payload.db.commitTransaction(session)
} catch (error) {
  await payload.db.rollbackTransaction(session)
}
```

### Versions and Drafts

The adapter supports Payload's versioning system:

```typescript
{
  slug: 'posts',
  versions: {
    drafts: true,
    maxPerDoc: 10
  },
  fields: [...]
}
```

Versions are stored in a separate `{collection}_versions` namespace.

### Globals

Global documents are stored in the `_payload_globals` collection:

```typescript
{
  slug: 'site-settings',
  fields: [
    { name: 'siteName', type: 'text' },
    { name: 'logo', type: 'upload', relationTo: 'media' }
  ]
}
```

## Deploying to Cloudflare Workers

### Using OpenNext

The recommended way to deploy Payload to Cloudflare Workers is with [OpenNext](https://opennextjs.org/):

```bash
# Install dependencies
pnpm add @opennextjs/cloudflare

# Create open-next.config.ts
```

```typescript
// open-next.config.ts
import type { OpenNextConfig } from '@opennextjs/cloudflare'

export default {
  default: {
    override: {
      wrapper: 'cloudflare-node',
      converter: 'edge',
    }
  }
} satisfies OpenNextConfig
```

```typescript
// wrangler.toml
name = "payload-parquedb"
main = ".open-next/worker.js"
compatibility_date = "2024-09-23"
compatibility_flags = ["nodejs_compat"]

[[r2_buckets]]
binding = "BUCKET"
bucket_name = "payload-data"

[vars]
PAYLOAD_SECRET = "your-secret-key"
```

### Worker Entry Point

```typescript
// src/worker.ts
import { getRequestContext } from '@cloudflare/next-on-pages'
import { R2Backend } from 'parquedb'

// Make R2 bucket available to Payload config
declare global {
  var R2_BUCKET: R2Bucket | undefined
}

export default {
  async fetch(request, env, ctx) {
    globalThis.R2_BUCKET = env.BUCKET
    // OpenNext handles the rest
  }
}
```

```typescript
// payload.config.ts
import { buildConfig } from 'payload'
import { parquedbAdapter } from 'parquedb/payload'
import { R2Backend, MemoryBackend } from 'parquedb'

const storage = globalThis.R2_BUCKET
  ? new R2Backend(globalThis.R2_BUCKET)
  : new MemoryBackend()

export default buildConfig({
  db: parquedbAdapter({ storage }),
  // ...
})
```

### Build and Deploy

```bash
# Build for Workers
pnpm opennextjs-cloudflare build

# Deploy
wrangler deploy
```

## Example Project Structure

```
my-payload-app/
├── src/
│   ├── app/
│   │   ├── (payload)/
│   │   │   ├── admin/
│   │   │   │   └── [[...segments]]/page.tsx
│   │   │   ├── api/
│   │   │   │   └── [...slug]/route.ts
│   │   │   └── layout.tsx
│   │   └── (frontend)/
│   │       └── page.tsx
│   ├── collections/
│   │   ├── Posts.ts
│   │   └── Users.ts
│   └── payload.config.ts
├── open-next.config.ts
├── wrangler.toml
└── package.json
```

## Collections Example

```typescript
// src/collections/Posts.ts
import type { CollectionConfig } from 'payload'

export const Posts: CollectionConfig = {
  slug: 'posts',
  admin: {
    useAsTitle: 'title',
    defaultColumns: ['title', 'status', 'createdAt']
  },
  versions: {
    drafts: true
  },
  fields: [
    {
      name: 'title',
      type: 'text',
      required: true
    },
    {
      name: 'slug',
      type: 'text',
      required: true,
      unique: true
    },
    {
      name: 'content',
      type: 'richText'
    },
    {
      name: 'status',
      type: 'select',
      options: [
        { label: 'Draft', value: 'draft' },
        { label: 'Published', value: 'published' }
      ],
      defaultValue: 'draft'
    },
    {
      name: 'author',
      type: 'relationship',
      relationTo: 'users'
    }
  ]
}
```

## Limitations

### Current Limitations

- **No database-level indexes**: Indexes are managed at the Parquet file level
- **Single-node writes**: For Workers, writes go through a single Durable Object
- **No full-text search**: Use ParqueDB's FTS index separately

### Planned Features

- [ ] Database-level migrations
- [ ] Automatic index creation from Payload schema
- [ ] Live preview support
- [ ] Real-time subscriptions

## Comparison with Other Adapters

| Feature | ParqueDB | MongoDB | Postgres |
|---------|----------|---------|----------|
| Edge deployable | Yes (R2) | No | No |
| Serverless | Yes | Atlas | Neon/Supabase |
| Analytics-ready | Yes (Parquet) | Limited | Yes |
| Self-hosted | Yes | Yes | Yes |
| Storage format | Parquet | BSON | PostgreSQL |

## Troubleshooting

### Common Issues

**"Storage backend not configured"**

Ensure you're passing a storage backend to the adapter:

```typescript
parquedbAdapter({
  storage: new FsBackend('./data')  // Required!
})
```

**"Cannot read R2 bucket"**

In Workers, ensure the R2 bucket binding is correct in `wrangler.toml`:

```toml
[[r2_buckets]]
binding = "BUCKET"
bucket_name = "your-bucket-name"
```

**"Transaction timeout"**

ParqueDB transactions have a default timeout. For long operations, consider breaking them into smaller batches.

## Next Steps

- [ParqueDB Studio](../studio.md) - Admin UI for ParqueDB
- [Cloudflare Workers Deployment](../deployment/cloudflare-workers.md) - Full deployment guide
- [Schema Definition](../schemas.md) - ParqueDB schema syntax
