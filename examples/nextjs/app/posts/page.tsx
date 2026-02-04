/**
 * Posts listing page - Server Component
 *
 * Uses auto-configured `db` from parquedb - no lib/db.ts needed!
 */
import Link from 'next/link'
import { db } from 'parquedb'

export default async function PostsPage() {
  const posts = await db.Post.find({ status: 'published' })

  return (
    <main>
      <h1>Blog Posts ({posts.$total})</h1>

      <ul>
        {posts.map(post => (
          <li key={post.$id}>
            <Link href={`/posts/${post.slug}`}>
              {post.title}
            </Link>
            <span> by {post.author?.name}</span>
          </li>
        ))}
      </ul>

      {posts.$next && (
        <Link href={`/posts?cursor=${posts.$next}`}>
          Load more
        </Link>
      )}
    </main>
  )
}
