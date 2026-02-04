/**
 * Single post page - Server Component with auto-hydrated relationships
 */
import Link from 'next/link'
import { notFound } from 'next/navigation'
import { db } from '@/lib/db'

interface Props {
  params: Promise<{ slug: string }>
}

export default async function PostPage({ params }: Props) {
  const { slug } = await params
  const post = await db.Post.get(slug)

  if (!post) {
    notFound()
  }

  // post.author is auto-hydrated - no extra query needed!
  const author = post.author

  return (
    <article>
      <h1>{post.title}</h1>

      <p>
        By <Link href={`/users/${author?.email}`}>{author?.name}</Link>
      </p>

      <div>{post.content}</div>

      {author?.posts && author.posts.$total > 1 && (
        <aside>
          <h3>More from {author.name}</h3>
          <ul>
            {author.posts
              .filter(p => p.slug !== slug)
              .slice(0, 3)
              .map(p => (
                <li key={p.$id}>
                  <Link href={`/posts/${p.slug}`}>{p.title}</Link>
                </li>
              ))}
          </ul>
        </aside>
      )}
    </article>
  )
}
