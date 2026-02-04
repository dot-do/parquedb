/**
 * New post form - Server Actions for mutations
 */
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

async function publishPost(formData: FormData) {
  'use server'

  const slug = formData.get('slug') as string
  await db.Post.update(slug, {
    $set: {
      status: 'published',
      publishedAt: new Date()
    }
  })

  redirect(`/posts/${slug}`)
}

export default async function NewPostPage() {
  const users = await db.User.find()

  return (
    <main>
      <h1>New Post</h1>

      <form action={createPost}>
        <div>
          <label htmlFor="slug">Slug</label>
          <input name="slug" id="slug" required />
        </div>

        <div>
          <label htmlFor="title">Title</label>
          <input name="title" id="title" required />
        </div>

        <div>
          <label htmlFor="content">Content</label>
          <textarea name="content" id="content" rows={10} />
        </div>

        <div>
          <label htmlFor="author">Author</label>
          <select name="author" id="author" required>
            {users.map(user => (
              <option key={user.$id} value={user.email}>
                {user.name}
              </option>
            ))}
          </select>
        </div>

        <button type="submit">Create Draft</button>
      </form>
    </main>
  )
}
