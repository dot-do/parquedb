import Link from 'next/link'
import { notFound } from 'next/navigation'
import { db } from '@/lib/db'

export default async function UserPage({ params }: { params: Promise<{ email: string }> }) {
  const { email } = await params
  const user = await db.User.get(decodeURIComponent(email))

  if (!user) notFound()

  return (
    <main>
      <h1>{user.name}</h1>
      <p>{user.email} ({user.role})</p>
      <h2>Posts ({user.posts?.$total})</h2>
      <ul>
        {user.posts?.map(p => (
          <li key={p.$id}><Link href={`/posts/${p.slug}`}>{p.title}</Link></li>
        ))}
      </ul>
    </main>
  )
}
