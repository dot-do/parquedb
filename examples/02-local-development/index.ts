/**
 * ParqueDB Local Development Workflow
 *
 * Shows best practices for local development:
 * - Auto-configured database
 * - Schema definition
 * - Hot reload patterns
 * - Development vs production
 *
 * Run: npx tsx examples/02-local-development/index.ts
 */
import { DB, FsBackend, defineConfig, defineSchema, detectRuntime } from '../../src'
import { rm } from 'fs/promises'

// 1. Define schema separately (can be in its own file)
// ----------------------------------------------------
const schema = defineSchema({
  User: {
    email: 'string!#',
    name: 'string',
    role: 'string',
    posts: '<- Post.author[]'
  },
  Post: {
    title: 'string!',
    content: 'text',
    status: 'string',
    author: '-> User'
  },
  Comment: {
    body: 'text!',
    rating: 'int',
    post: '-> Post',
    author: '-> User'
  }
})

// 2. Configure based on environment
// ---------------------------------
const config = defineConfig({
  storage: new FsBackend('.db'),
  // In production, you might use:
  // storage: new R2Backend(env.MY_BUCKET)
})

async function main() {
  // Clean start for demo
  await rm('.db', { recursive: true, force: true })

  // Detect runtime
  const runtime = detectRuntime()
  console.log(`Runtime: ${runtime}`)

  // Create database
  const db = DB(schema, config)
  console.log('Database ready\n')

  // Show the schema
  console.log('Schema:')
  for (const [name, fields] of Object.entries(schema)) {
    console.log(`  ${name}:`)
    for (const [field, type] of Object.entries(fields)) {
      console.log(`    ${field}: ${type}`)
    }
  }

  // Create test data
  console.log('\nCreating test data...')

  const admin = await db.User.create({
    $type: 'User',
    name: 'Admin',
    email: 'admin@example.com',
    role: 'admin'
  })

  const post = await db.Post.create({
    $type: 'Post',
    name: 'Test Post',
    title: 'Test Post',
    content: 'Testing local development workflow',
    status: 'draft'
  })

  await db.Post.update(post.$id, {
    $link: { author: [admin.$id] }
  })

  // Query
  const posts = await db.Post.find()
  console.log(`Created ${posts.total} post(s)`)

  // Development tip: watch mode
  console.log('\n--- Development Tips ---')
  console.log(`
  # Run with watch mode for hot reload:
  npx tsx watch src/index.ts

  # Or use nodemon:
  npx nodemon --exec 'npx tsx' src/index.ts

  # Check database files:
  ls -la .db/

  # View parquet file metadata:
  npx parquet-tools meta .db/data/posts/data.parquet
  `)

  db.dispose()
  console.log('Done!')
}

main().catch(console.error)
