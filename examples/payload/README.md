# Payload CMS with ParqueDB Example

This example demonstrates how to use Payload CMS with ParqueDB as the database backend. ParqueDB stores data in Apache Parquet files, which can be stored locally or in cloud object storage like Cloudflare R2.

Uses **OpenNext** (`@opennextjs/cloudflare`) for seamless deployment to Cloudflare Workers.

## Features

- **Next.js 15**: Built on Payload 3 with the latest Next.js
- **Local Development**: Use filesystem storage for quick local development
- **Cloud Deployment**: Deploy to Cloudflare Workers via OpenNext
- **Parquet Format**: Data stored in efficient, queryable Parquet files
- **Full Payload Features**: Supports collections, globals, versions, drafts, and more

## Quick Start

### Local Development

1. Install dependencies:

```bash
npm install
# or
pnpm install
```

2. Start the development server:

```bash
npm run dev
```

3. Open [http://localhost:3000/admin](http://localhost:3000/admin) to access the admin panel.

4. Create your first admin user and start adding content!

### Data Storage

In development mode, data is stored in the `./data` directory as Parquet files:

```
data/
├── posts/
│   └── data.parquet
├── users/
│   └── data.parquet
├── categories/
│   └── data.parquet
├── posts_versions/
│   └── data.parquet
└── payload_globals/
    └── data.parquet
```

## Deployment to Cloudflare Workers

This example uses **OpenNext** (`@opennextjs/cloudflare`) to deploy Payload CMS (a Next.js app) to Cloudflare Workers.

### Prerequisites

1. Create a Cloudflare account
2. Install Wrangler CLI: `npm install -g wrangler`
3. Login to Wrangler: `wrangler login`

### Setup R2 Bucket

1. Create R2 buckets for data and media:

```bash
wrangler r2 bucket create payload-data
wrangler r2 bucket create payload-media
```

2. Update `wrangler.toml` with your configuration:

```toml
name = "payload-parquedb"
compatibility_date = "2024-09-02"
compatibility_flags = ["nodejs_compat"]

[[r2_buckets]]
binding = "DATA"
bucket_name = "payload-data"

[[r2_buckets]]
binding = "MEDIA"
bucket_name = "payload-media"

[vars]
PAYLOAD_SECRET = "your-secret-here-change-in-production"
```

### Build and Deploy

1. Build for Cloudflare Workers:

```bash
npm run build:workers
```

2. Preview locally:

```bash
npm run preview
```

3. Deploy to production:

```bash
npm run deploy
```

### Environment Variables

Set production secrets using Wrangler:

```bash
wrangler secret put PAYLOAD_SECRET
```

## Project Structure

```
examples/payload/
├── src/
│   ├── app/                 # Next.js App Router
│   │   ├── (payload)/       # Payload admin routes
│   │   │   ├── admin/       # Admin panel
│   │   │   └── api/         # REST & GraphQL APIs
│   │   ├── layout.tsx       # Root layout
│   │   └── page.tsx         # Home page
│   ├── collections/         # Payload collection definitions
│   │   ├── Posts.ts
│   │   ├── Categories.ts
│   │   ├── Media.ts
│   │   ├── Users.ts
│   │   └── index.ts
│   ├── globals/             # Payload global definitions
│   │   ├── SiteSettings.ts
│   │   └── index.ts
│   └── payload.config.ts    # Main Payload configuration
├── package.json
├── tsconfig.json
├── next.config.mjs
├── wrangler.toml
└── README.md
```

## Configuration

### Database Adapter

The ParqueDB adapter is configured in `payload.config.ts`:

```typescript
import { parquedbAdapter } from 'parquedb/payload'
import { FileSystemBackend } from 'parquedb'

export default buildConfig({
  db: parquedbAdapter({
    storage: new FileSystemBackend('./data'),
    debug: true,
  }),
  // ... rest of config
})
```

### Storage Backends

ParqueDB supports multiple storage backends:

#### Filesystem (Local Development)

```typescript
import { FileSystemBackend } from 'parquedb'

const storage = new FileSystemBackend('./data')
```

#### R2 (Cloudflare Workers)

```typescript
import { R2Backend } from 'parquedb'

// In Workers, use the R2 bucket binding
const storage = new R2Backend(env.DATA)
```

#### Memory (Testing)

```typescript
import { MemoryBackend } from 'parquedb'

const storage = new MemoryBackend()
```

## Scripts

| Script | Description |
|--------|-------------|
| `npm run dev` | Start local development server |
| `npm run build` | Build for Node.js |
| `npm run start` | Start production server (Node.js) |
| `npm run build:workers` | Build for Cloudflare Workers |
| `npm run preview` | Preview Workers build locally |
| `npm run deploy` | Deploy to Cloudflare Workers |

## Features Supported

| Feature | Status |
|---------|--------|
| Collections | ✅ |
| Fields (all types) | ✅ |
| Relationships | ✅ |
| Globals | ✅ |
| Versions | ✅ |
| Drafts | ✅ |
| Authentication | ✅ |
| Localization | ✅ |
| Transactions | ✅ |
| Migrations | ✅ |

## API Usage

Once running, you can interact with the API:

```bash
# Get all posts
curl http://localhost:3000/api/posts

# Get a single post
curl http://localhost:3000/api/posts/123

# Create a post (requires auth)
curl -X POST http://localhost:3000/api/posts \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"title": "Hello World", "content": "..."}'
```

## Querying Parquet Data Directly

One advantage of ParqueDB is that your data is stored in standard Parquet files, which can be queried directly with tools like DuckDB:

```sql
-- Query posts directly with DuckDB
SELECT * FROM 'data/posts/data.parquet'
WHERE status = 'published'
ORDER BY createdAt DESC
LIMIT 10;
```

## Troubleshooting

### OpenNext Build Issues

If you encounter build issues with OpenNext:

1. Ensure Node.js version is 18+
2. Clear build cache: `rm -rf .next .open-next`
3. Reinstall dependencies: `rm -rf node_modules && npm install`

### R2 Access Issues

If R2 operations fail:

1. Verify bucket names in `wrangler.toml` match created buckets
2. Check binding names match what's used in code
3. Ensure `nodejs_compat` flag is enabled

## License

MIT
