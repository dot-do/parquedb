import Link from 'next/link'
import { notFound } from 'next/navigation'
import { db } from '@/lib/db'

export default async function PostPage({ params }: { params: Promise<{ slug: string }> }) {
  const { slug } = await params
  const post = await db.Post.get(slug)

  if (!post) notFound()

  return (
    <article>
      <h1>{post.title}</h1>
      <p>By <Link href={`/users/${post.author?.email}`}>{post.author?.name}</Link></p>
      <div>{post.content}</div>
    </article>
  )
}
