/**
 * ai-database Quickstart Example with ParqueDB
 *
 * This example demonstrates the basic usage of ParqueDB as a backend
 * for ai-database:
 * - Provider setup and configuration
 * - CRUD operations (create, read, update, delete)
 * - Simple relationships
 * - Listing and filtering
 *
 * Run with: npx tsx examples/ai-database/01-quickstart.ts
 */

import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { createParqueDBProvider } from '../../src/integrations/ai-database'

async function main() {
  console.log('=== ai-database + ParqueDB Quickstart ===\n')

  // ============================================================================
  // Step 1: Initialize ParqueDB and create the provider
  // ============================================================================

  console.log('--- Initializing Database ---\n')

  // Create a ParqueDB instance with in-memory storage
  // For production, use FsBackend or R2Backend
  const parquedb = new ParqueDB({
    storage: new MemoryBackend(),
  })

  // Create the ai-database provider
  const provider = createParqueDBProvider(parquedb)

  console.log('ParqueDB provider initialized with MemoryBackend')

  // ============================================================================
  // Step 2: Create some authors
  // ============================================================================

  console.log('\n--- Creating Authors ---\n')

  const jane = await provider.create('Author', undefined, {
    name: 'Jane Smith',
    email: 'jane@example.com',
    bio: 'Tech writer and software engineer',
  })
  console.log(`Created author: ${jane.name} (${jane.$id})`)

  const john = await provider.create('Author', undefined, {
    name: 'John Doe',
    email: 'john@example.com',
    bio: 'Full-stack developer and blogger',
  })
  console.log(`Created author: ${john.name} (${john.$id})`)

  // ============================================================================
  // Step 3: Create posts with relationships to authors
  // ============================================================================

  console.log('\n--- Creating Posts ---\n')

  const post1 = await provider.create('Post', undefined, {
    title: 'Introduction to ParqueDB',
    content: 'ParqueDB is a powerful database built on Apache Parquet...',
    status: 'published',
  })

  // Create relationship: post1 -> jane (author)
  await provider.relate('Post', post1.$id as string, 'author', 'Author', jane.$id as string)
  console.log(`Created post: "${post1.title}" by Jane`)

  const post2 = await provider.create('Post', undefined, {
    title: 'Working with ai-database',
    content: 'The ai-database library provides a simple API for AI-powered databases...',
    status: 'draft',
  })
  await provider.relate('Post', post2.$id as string, 'author', 'Author', jane.$id as string)
  console.log(`Created post: "${post2.title}" by Jane`)

  const post3 = await provider.create('Post', undefined, {
    title: 'Full-Stack Development Tips',
    content: 'Here are some tips for becoming a better full-stack developer...',
    status: 'published',
  })
  await provider.relate('Post', post3.$id as string, 'author', 'Author', john.$id as string)
  console.log(`Created post: "${post3.title}" by John`)

  // ============================================================================
  // Step 4: Read operations
  // ============================================================================

  console.log('\n--- Read Operations ---\n')

  // Get by ID
  const fetchedPost = await provider.get('Post', post1.$id as string)
  console.log(`Fetched post by ID: "${fetchedPost?.title}"`)

  // List all posts
  const allPosts = await provider.list('Post')
  console.log(`Total posts: ${allPosts.length}`)

  // Filter posts by status
  const publishedPosts = await provider.list('Post', {
    where: { status: 'published' },
  })
  console.log(`Published posts: ${publishedPosts.length}`)

  // List with sorting and pagination
  const recentPosts = await provider.list('Post', {
    orderBy: 'createdAt',
    order: 'desc',
    limit: 2,
  })
  console.log(`Recent posts (limit 2): ${recentPosts.map(p => p.title).join(', ')}`)

  // ============================================================================
  // Step 5: Navigate relationships
  // ============================================================================

  console.log('\n--- Relationships ---\n')

  // Get author for a post
  const post1Authors = await provider.related('Post', post1.$id as string, 'author')
  console.log(`Author of "${post1.title}": ${post1Authors[0]?.name || 'Unknown'}`)

  // ============================================================================
  // Step 6: Update operations
  // ============================================================================

  console.log('\n--- Update Operations ---\n')

  // Update a post
  const updatedPost = await provider.update('Post', post2.$id as string, {
    status: 'published',
    publishedAt: new Date().toISOString(),
  })
  console.log(`Updated post "${updatedPost.title}" status to: ${updatedPost.status}`)

  // Verify the update
  const verifyPost = await provider.get('Post', post2.$id as string)
  console.log(`Verified status: ${verifyPost?.status}`)

  // ============================================================================
  // Step 7: Search operations
  // ============================================================================

  console.log('\n--- Search Operations ---\n')

  // Full-text search
  const searchResults = await provider.search('Post', 'ParqueDB')
  console.log(`Search results for "ParqueDB": ${searchResults.length} post(s)`)
  if (searchResults.length > 0) {
    console.log(`  - ${searchResults[0].title}`)
  }

  // ============================================================================
  // Step 8: Delete operations
  // ============================================================================

  console.log('\n--- Delete Operations ---\n')

  // Create a temporary post
  const tempPost = await provider.create('Post', undefined, {
    title: 'Temporary Post',
    content: 'This will be deleted',
    status: 'draft',
  })
  console.log(`Created temporary post: ${tempPost.$id}`)

  // Delete it
  const deleted = await provider.delete('Post', tempPost.$id as string)
  console.log(`Deleted: ${deleted}`)

  // Verify deletion
  const deletedPost = await provider.get('Post', tempPost.$id as string)
  console.log(`Post exists after deletion: ${deletedPost !== null}`)

  // ============================================================================
  // Step 9: Final statistics
  // ============================================================================

  console.log('\n--- Final Statistics ---\n')

  const finalPosts = await provider.list('Post')
  const finalAuthors = await provider.list('Author')
  console.log(`Total posts: ${finalPosts.length}`)
  console.log(`Total authors: ${finalAuthors.length}`)

  // List published posts with their titles
  const published = await provider.list('Post', { where: { status: 'published' } })
  console.log('\nPublished posts:')
  for (const post of published) {
    const authors = await provider.related('Post', post.$id as string, 'author')
    const authorName = authors[0]?.name || 'Unknown'
    console.log(`  - "${post.title}" by ${authorName}`)
  }

  // ============================================================================
  // Cleanup
  // ============================================================================

  parquedb.dispose()

  console.log('\n=== Quickstart Complete ===')
}

main().catch(console.error)
