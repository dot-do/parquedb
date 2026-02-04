import { redirect } from 'next/navigation'
import { db } from '@/lib/db'

async function createPost(formData: FormData) {
  'use server'
  const post = await db.Post.create({
    slug: formData.get('slug') as string,
    title: formData.get('title') as string,
    content: formData.get('content') as string,
    status: 'draft',
    author: formData.get('author') as string
  })
  redirect(`/posts/${post.slug}`)
}

export default async function NewPostPage() {
  const users = await db.User.find()

  return (
    <form action={createPost}>
      <input name="slug" placeholder="slug" required />
      <input name="title" placeholder="title" required />
      <textarea name="content" placeholder="content" />
      <select name="author" required>
        {users.map(u => <option key={u.$id} value={u.email}>{u.name}</option>)}
      </select>
      <button type="submit">Create</button>
    </form>
  )
}
