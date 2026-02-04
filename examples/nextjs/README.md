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

## Features Demonstrated

### Server Components

```tsx
// app/posts/page.tsx
const posts = await db.Post.find({ status: 'published' })

// Direct iteration - no .items needed
{posts.map(post => (
  <li>{post.title} by {post.author?.name}</li>
))}

// Pagination metadata via proxy
<span>Total: {posts.$total}</span>
```

### Auto-Hydrated Relationships

```tsx
// app/posts/[slug]/page.tsx
const post = await db.Post.get(slug)

// Forward relationship auto-hydrated
<p>By {post.author?.name}</p>

// Author's posts also available (reverse relationship)
{post.author?.posts.map(p => <Link href={`/posts/${p.slug}`}>{p.title}</Link>)}
```

### Server Actions

```tsx
// app/posts/new/page.tsx
async function createPost(formData: FormData) {
  'use server'
  const post = await db.Post.create({
    slug: formData.get('slug'),
    title: formData.get('title'),
    author: formData.get('author')
  })
  redirect(`/posts/${post.slug}`)
}
```

### API Routes

```ts
// app/api/posts/route.ts
export async function GET(request: NextRequest) {
  const posts = await db.Post.find({ status: 'published' })
  return NextResponse.json({ posts, total: posts.$total })
}
```

## Pages

- `/posts` - List published posts
- `/posts/[slug]` - Single post with author
- `/posts/new` - Create new post (Server Action)
- `/users/[email]` - User profile with their posts
- `/api/posts` - REST API endpoint

## Database Location

Data is stored in `.data/` directory as Parquet files.
