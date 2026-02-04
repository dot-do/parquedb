# ParqueDB Next.js Example

A blog application using ParqueDB with Next.js App Router.

## Setup

```bash
# Install dependencies
pnpm install

# Seed the database
pnpm seed

# Start development server
pnpm dev
```

## Project Structure

```
parquedb.config.ts    # Schema + config - auto-detected by parquedb
app/
  posts/page.tsx      # Server Component - just `import { db } from 'parquedb'`
  posts/[slug]/page.tsx
  posts/new/page.tsx  # Server Action for mutations
  users/[email]/page.tsx
  api/posts/route.ts  # REST API route
scripts/
  seed.ts             # Database seeding
```

## The Magic: parquedb.config.ts

```ts
import { defineConfig, defineSchema } from 'parquedb/config'

export const schema = defineSchema({
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
    author: '-> User',

    // Studio layout
    $layout: { Content: [['title', 'slug'], 'content'] },
    $studio: { label: 'Blog Posts' }
  }
})

export default defineConfig({ schema })
```

Then just import `db` anywhere:

```tsx
import { db } from 'parquedb'

const posts = await db.Post.find({ status: 'published' })
```

No `lib/db.ts` needed - ParqueDB auto-detects the config file!

## Features Demonstrated

### Server Components

```tsx
// app/posts/page.tsx
import { db } from 'parquedb'

export default async function PostsPage() {
  const posts = await db.Post.find({ status: 'published' })

  return (
    <ul>
      {posts.map(post => (
        <li>{post.title} by {post.author?.name}</li>
      ))}
    </ul>
  )
}
```

### Auto-Hydrated Relationships

```tsx
const post = await db.Post.get('hello-world')

// Forward relationship auto-hydrated
<p>By {post.author?.name}</p>

// Author's posts also available (reverse relationship)
{post.author?.posts.map(p => <Link href={`/posts/${p.slug}`}>{p.title}</Link>)}
```

### Server Actions

```tsx
async function createPost(formData: FormData) {
  'use server'
  await db.Post.create({
    slug: formData.get('slug'),
    title: formData.get('title'),
    author: formData.get('author')
  })
  redirect(`/posts/${slug}`)
}
```

### SQL Queries

```tsx
import { sql } from 'parquedb'

const posts = await sql`SELECT * FROM posts WHERE status = ${'published'}`
```

## Pages

- `/posts` - List published posts
- `/posts/[slug]` - Single post with author
- `/posts/new` - Create new post (Server Action)
- `/users/[email]` - User profile with their posts
- `/api/posts` - REST API endpoint

## Database Location

Data stored in `.parquedb/` (auto-created).
