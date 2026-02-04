import Link from 'next/link'
import { db } from '@/lib/db'

export default async function PostsPage() {
  const posts = await db.Post.find({ status: 'published' })

  return (
    <main>
      <h1>Blog Posts ({posts.$total})</h1>
      <ul>
        {posts.map(post => (
          <li key={post.$id}>
            <Link href={`/posts/${post.slug}`}>{post.title}</Link>
            <span> by {post.author?.name}</span>
          </li>
        ))}
      </ul>
    </main>
  )
}
