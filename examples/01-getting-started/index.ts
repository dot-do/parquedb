/**
 * ParqueDB Getting Started
 *
 * This example demonstrates the core workflow:
 * - Define schema with relationships
 * - CRUD operations
 * - Linking entities
 * - Querying with filters
 *
 * Run: npx tsx examples/01-getting-started/index.ts
 */
import { DB, FsBackend } from '../../src'
import { rm } from 'fs/promises'

async function main() {
  // Clean start for reproducible demo
  await rm('.db', { recursive: true, force: true })

  // 1. Define schema with relationships
  // -----------------------------------
  // ParqueDB uses a concise schema DSL:
  // - 'string!'  = required string
  // - 'string!#' = required string + indexed
  // - '-> User'  = forward relationship to User
  // - '<- Post.author[]' = reverse relationship (posts that link to this user)

  const db = DB({
    User: {
      email: 'string!#',      // required + indexed
      name: 'string',
      role: 'string',
      posts: '<- Post.author[]'  // reverse relationship
    },
    Post: {
      title: 'string!',
      content: 'text',
      status: 'string',
      author: '-> User'       // forward relationship
    }
  }, {
    storage: new FsBackend('.db')
  })

  console.log('Database initialized with FsBackend at .db/')

  // 2. Create users
  // ---------------
  const alice = await db.User.create({
    $type: 'User',
    name: 'Alice',
    email: 'alice@example.com',
    role: 'admin'
  })
  console.log('Created:', alice.name, `(${alice.$id})`)

  const bob = await db.User.create({
    $type: 'User',
    name: 'Bob',
    email: 'bob@example.com',
    role: 'author'
  })
  console.log('Created:', bob.name, `(${bob.$id})`)

  // 3. Create posts
  // ---------------
  const post1 = await db.Post.create({
    $type: 'Post',
    name: 'Hello World',
    title: 'Hello World',
    content: 'My first post with ParqueDB',
    status: 'published'
  })
  console.log('Created post:', post1.title)

  const post2 = await db.Post.create({
    $type: 'Post',
    name: 'Getting Started Guide',
    title: 'Getting Started Guide',
    content: 'Learn how to use ParqueDB effectively',
    status: 'published'
  })

  const post3 = await db.Post.create({
    $type: 'Post',
    name: 'Draft Ideas',
    title: 'Draft Ideas',
    content: 'Work in progress...',
    status: 'draft'
  })

  // 4. Link posts to authors
  // ------------------------
  // Relationships are established using $link in updates
  await db.Post.update(post1.$id, {
    $link: { author: [alice.$id] }
  })

  await db.Post.update(post2.$id, {
    $link: { author: [alice.$id] }
  })

  await db.Post.update(post3.$id, {
    $link: { author: [bob.$id] }
  })
  console.log('Linked posts to authors')

  // 5. Query - find() returns PaginatedResult
  // -----------------------------------------
  // Results have: { items, total, hasMore, nextCursor }
  const published = await db.Post.find({ status: 'published' })
  console.log(`\nFound ${published.items.length} published posts (total: ${published.total})`)
  for (const p of published.items) {
    console.log(`  - ${p.title}`)
  }

  // 6. Query with MongoDB-style operators
  // -------------------------------------
  const admins = await db.User.find({ role: { $eq: 'admin' } })
  console.log(`\nAdmin users: ${admins.items.map(u => u.name).join(', ')}`)

  // 7. Update with operators
  // ------------------------
  await db.Post.update(post3.$id, { $set: { status: 'published' } })
  console.log('\nUpdated draft post to published')

  // Verify the update
  const allPublished = await db.Post.find({ status: 'published' })
  console.log(`Now ${allPublished.total} published posts`)

  // 8. Get related entities
  // -----------------------
  // Traverse relationships to find connected entities
  const alicePosts = await db.User.getRelated(alice.$id, 'posts')
  console.log(`\nAlice has ${alicePosts.items.length} posts`)

  // 9. Delete (soft delete by default)
  // ----------------------------------
  await db.Post.delete(post3.$id)
  const afterDelete = await db.Post.find()
  console.log(`\nAfter soft delete: ${afterDelete.total} visible posts`)

  // Can still find with includeDeleted
  const withDeleted = await db.Post.find({}, { includeDeleted: true })
  console.log(`With deleted: ${withDeleted.total} total posts`)

  // Clean up
  db.dispose()
  console.log('\nDone! Check .db/ for parquet files')
}

main().catch(console.error)
