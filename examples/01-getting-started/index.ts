/**
 * ParqueDB Getting Started
 *
 * Run: npx tsx examples/01-getting-started/index.ts
 */
import { DB, FsBackend } from 'parquedb'

async function main() {
  const db = DB({
    User: {
      $id: 'email',
      $name: 'name',        // Use name field for display
      email: 'string!#',
      name: 'string!',
      role: 'string',
      posts: '<- Post.author[]'
    },
    Post: {
      $id: 'slug',
      $name: 'title',       // Use title as display name
      slug: 'string!#',
      title: 'string!',
      content: 'text',
      status: 'string',
      author: '-> User'
    }
  }, {
    storage: new FsBackend('.db')
  })

  // Create users - email becomes the $id
  const alice = await db.User.create({
    email: 'alice@example.com',
    name: 'Alice',
    role: 'admin'
  })
  console.log('Created:', alice.$id)  // user/alice@example.com

  await db.User.create({
    email: 'bob@example.com',
    name: 'Bob',
    role: 'author'
  })

  // Create posts - author auto-resolves via schema
  await db.Post.create({
    slug: 'hello-world',
    title: 'Hello World',
    content: 'My first post',
    status: 'published',
    author: 'alice@example.com'  // -> user/alice@example.com
  })

  await db.Post.create({
    slug: 'getting-started',
    title: 'Getting Started Guide',
    content: 'Learn ParqueDB',
    status: 'published',
    author: 'alice@example.com'
  })

  await db.Post.create({
    slug: 'draft-ideas',
    title: 'Draft Ideas',
    content: 'Work in progress...',
    status: 'draft',
    author: 'bob@example.com'
  })

  // Query
  const published = await db.Post.find({ status: 'published' })
  console.log(`Found ${published.total} published posts`)

  // Get by readable ID
  const post = await db.Post.get('hello-world')
  console.log('Post:', post?.title)

  // Get related
  const alicePosts = await db.User.getRelated('alice@example.com', 'posts')
  console.log(`Alice has ${alicePosts.total} posts`)

  db.dispose()
  console.log('Done!')
}

main().catch(console.error)
