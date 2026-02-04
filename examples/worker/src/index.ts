/**
 * ParqueDB Cloudflare Worker Example
 *
 * Deploy: wrangler deploy
 */
import { db } from './db'

// Re-export ParqueDBDO for Cloudflare to bind
export { ParqueDBDO } from 'parquedb/worker'

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // GET /posts - List published posts
    if (url.pathname === '/posts' && request.method === 'GET') {
      const posts = await db.Post.find({ status: 'published' })

      return Response.json({
        total: posts.$total,
        posts: posts.map(p => ({
          slug: p.slug,
          title: p.title,
          author: p.author?.name
        }))
      })
    }

    // GET /posts/:slug - Get single post with author
    if (url.pathname.startsWith('/posts/') && request.method === 'GET') {
      const slug = url.pathname.split('/')[2]
      const post = await db.Post.get(slug)

      if (!post) {
        return Response.json({ error: 'Not found' }, { status: 404 })
      }

      return Response.json({
        slug: post.slug,
        title: post.title,
        content: post.content,
        author: {
          name: post.author?.name,
          email: post.author?.email
        }
      })
    }

    // POST /posts - Create new post
    if (url.pathname === '/posts' && request.method === 'POST') {
      const body = await request.json() as {
        slug: string
        title: string
        content: string
        author: string
      }

      const post = await db.Post.create({
        slug: body.slug,
        title: body.title,
        content: body.content,
        status: 'draft',
        author: body.author
      })

      return Response.json({ id: post.$id, slug: post.slug }, { status: 201 })
    }

    // GET /users/:email - Get user with their posts
    if (url.pathname.startsWith('/users/') && request.method === 'GET') {
      const email = decodeURIComponent(url.pathname.split('/')[2])
      const user = await db.User.get(email)

      if (!user) {
        return Response.json({ error: 'Not found' }, { status: 404 })
      }

      return Response.json({
        email: user.email,
        name: user.name,
        role: user.role,
        posts: {
          total: user.posts?.$total ?? 0,
          items: (user.posts ?? []).map(p => ({
            slug: p.slug,
            title: p.title,
            status: p.status
          }))
        }
      })
    }

    return Response.json({ error: 'Not found' }, { status: 404 })
  }
}
