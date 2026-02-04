# ParqueDB Cloudflare Worker Example

A REST API using ParqueDB with R2 storage.

## Setup

```bash
pnpm install
wrangler r2 bucket create parquedb-data
pnpm dev
```

## Schema in db.ts

Workers don't have filesystem access, so use `DB()` directly:

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

## Worker Handler

```ts
// src/index.ts
import { db } from './db'
export { ParqueDBDO } from 'parquedb/worker'

export default {
  async fetch(request: Request) {
    const post = await db.Post.get('hello-world')
    return Response.json({
      title: post?.title,
      author: post?.author?.name
    })
  }
}
```

## wrangler.jsonc

```jsonc
{
  "r2_buckets": [{ "binding": "BUCKET", "bucket_name": "parquedb-data" }],
  "durable_objects": {
    "bindings": [{ "name": "PARQUEDB", "class_name": "ParqueDBDO" }]
  }
}
```

## Endpoints

```bash
curl http://localhost:8787/posts
curl http://localhost:8787/posts/hello-world
curl http://localhost:8787/users/alice%40example.com
```
