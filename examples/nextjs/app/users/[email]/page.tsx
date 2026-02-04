/**
 * User profile page - Reverse relationships auto-hydrated
 */
import Link from 'next/link'
import { notFound } from 'next/navigation'
import { db } from '../../../src/db.generated'

interface Props {
  params: Promise<{ email: string }>
}

export default async function UserPage({ params }: Props) {
  const { email } = await params
  const user = await db.User.get(decodeURIComponent(email))

  if (!user) {
    notFound()
  }

  const posts = user.posts ?? []

  return (
    <main>
      <h1>{user.name}</h1>
      <p>{user.email}</p>
      <p>Role: {user.role}</p>

      <h2>Posts ({posts.$total})</h2>
      <ul>
        {posts.map(post => (
          <li key={post.$id}>
            <Link href={`/posts/${post.slug}`}>
              {post.title}
            </Link>
            <span> ({post.status})</span>
          </li>
        ))}
      </ul>

      {posts.$next && (
        <p>
          <Link href={`/users/${email}?cursor=${posts.$next}`}>
            Load more posts
          </Link>
        </p>
      )}
    </main>
  )
}
