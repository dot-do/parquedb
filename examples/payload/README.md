# Payload CMS with ParqueDB Example

This example demonstrates how to use Payload CMS with ParqueDB as the database backend. ParqueDB stores data in Apache Parquet files, which can be stored locally or in cloud object storage like Cloudflare R2.

## Features

- **Local Development**: Use filesystem storage for quick local development
- **Cloud Deployment**: Deploy to Cloudflare Workers with R2 storage
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

### Setup R2 Bucket

1. Create an R2 bucket in your Cloudflare dashboard:

```bash
wrangler r2 bucket create payload-data
```

2. Update `wrangler.toml` with your bucket name and secret:

```toml
name = "payload-parquedb"
main = "src/worker.ts"

[[r2_buckets]]
binding = "DATA"
bucket_name = "payload-data"

[vars]
PAYLOAD_SECRET = "your-secret-here"
```

3. Deploy:

```bash
npm run deploy
```

## Project Structure

```
examples/payload/
├── src/
│   ├── collections/     # Payload collection definitions
│   │   ├── Posts.ts
│   │   ├── Categories.ts
│   │   ├── Media.ts
│   │   ├── Users.ts
│   │   └── index.ts
│   ├── globals/         # Payload global definitions
│   │   ├── SiteSettings.ts
│   │   └── index.ts
│   ├── payload.config.ts # Main Payload configuration
│   ├── server.ts         # Express server for local dev
│   └── worker.ts         # Cloudflare Worker for production
├── package.json
├── tsconfig.json
├── wrangler.toml
└── README.md
```

## Configuration

### Database Adapter

The ParqueDB adapter is configured in `payload.config.ts`:

```typescript
import { parquedbAdapter } from 'parquedb/payload'
import { FsBackend } from 'parquedb'

export default buildConfig({
  db: parquedbAdapter({
    storage: new FsBackend('./data'),
    debug: true,
  }),
  // ... rest of config
})
```

### Storage Backends

ParqueDB supports multiple storage backends:

#### Filesystem (Local Development)

```typescript
import { FsBackend } from 'parquedb'

const storage = new FsBackend('./data')
```

#### R2 (Cloudflare Workers)

```typescript
import { R2Backend } from 'parquedb'

const storage = new R2Backend(env.DATA)
```

#### Memory (Testing)

```typescript
import { MemoryBackend } from 'parquedb'

const storage = new MemoryBackend()
```

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

## License

MIT
