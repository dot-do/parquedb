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

## Features Demonstrated

- **R2Backend** - Cloudflare R2 object storage
- **$id directive** - Human-readable IDs (slug, email)
- **Auto-hydration** - `post.author.name` works directly
- **Reverse relationships** - `user.posts` with `$total`

## Deploy

```bash
pnpm deploy
```
