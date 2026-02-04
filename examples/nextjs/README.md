# ParqueDB Next.js Example

## Setup

```bash
pnpm install
pnpm seed
pnpm dev
```

## Type-Safe Schema

```ts
// lib/db.ts
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

```tsx
// app/posts/page.tsx
import { db } from '@/lib/db'

export default async function PostsPage() {
  const posts = await db.Post.find({ status: 'published' })
  // posts is typed, post.author?.name is auto-hydrated
}
```

## Pages

- `/posts` - List posts
- `/posts/[slug]` - Single post
- `/posts/new` - Create post (Server Action)
- `/users/[email]` - User profile
- `/api/posts` - REST API
