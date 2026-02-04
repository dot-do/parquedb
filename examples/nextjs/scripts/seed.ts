/**
 * Seed the database with sample data
 *
 * Run: pnpm seed
 */
import { db } from '../lib/db'

// Create users
await db.User.create({
  email: 'alice@example.com',
  name: 'Alice',
  role: 'admin'
})

await db.User.create({
  email: 'bob@example.com',
  name: 'Bob',
  role: 'author'
})

// Create posts
await db.Post.create({
  slug: 'hello-world',
  title: 'Hello World',
  content: 'Welcome to our blog!',
  status: 'published',
  publishedAt: new Date(),
  author: 'alice@example.com'
})

await db.Post.create({
  slug: 'getting-started',
  title: 'Getting Started with ParqueDB',
  content: 'ParqueDB is a document database built on Parquet...',
  status: 'published',
  publishedAt: new Date(),
  author: 'alice@example.com'
})

await db.Post.create({
  slug: 'draft-post',
  title: 'Work in Progress',
  content: 'This is a draft...',
  status: 'draft',
  author: 'bob@example.com'
})

console.log('Seeded database!')
db.dispose()
