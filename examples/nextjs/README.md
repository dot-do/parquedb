# ParqueDB Next.js Example

A blog using ParqueDB with Next.js App Router.

## Setup

```bash
pnpm install
pnpm generate   # Generate typed exports
pnpm seed
pnpm dev
```

## Type-Safe Setup

### 1. Define schema in `parquedb.config.ts`

```ts
import { defineConfig } from 'parquedb/config'

export default defineConfig({
  schema: {
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
  }
})
```

### 2. Generate typed exports

```bash
npx parquedb generate
# Creates src/db.generated.ts with full TypeScript types
```

### 3. Import and use

```tsx
import { db } from './db.generated'

// Fully typed!
const posts = await db.Post.find({ status: 'published' })
const post = await db.Post.get('hello-world')
console.log(post.author?.name)  // Auto-hydrated!
```

## Pages

- `/posts` - List published posts
- `/posts/[slug]` - Single post with author
- `/posts/new` - Create new post (Server Action)
- `/users/[email]` - User profile with their posts
- `/api/posts` - REST API endpoint
