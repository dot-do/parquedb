/**
 * ParqueDB Getting Started Example
 *
 * This example demonstrates the core features of ParqueDB:
 * - Basic CRUD operations (Create, Read, Update, Delete)
 * - Filtering and querying with MongoDB-style operators
 * - Bidirectional relationships
 * - Aggregation pipelines
 * - Time-travel and event history
 *
 * Run with: npx tsx examples/getting-started/index.ts
 */

import { ParqueDB, MemoryBackend, Collection, clearGlobalStorage } from '../../src'

// =============================================================================
// Type Definitions
// =============================================================================

interface User {
  email: string
  role: 'admin' | 'author' | 'reader'
  bio?: string
}

interface Post {
  title: string
  content: string
  status: 'draft' | 'published' | 'archived'
  tags?: string[]
  viewCount?: number
}

interface Comment {
  body: string
  rating?: number
}

// =============================================================================
// Main Example
// =============================================================================

async function main() {
  console.log('='.repeat(70))
  console.log('ParqueDB Getting Started Example')
  console.log('='.repeat(70))

  // ---------------------------------------------------------------------------
  // Step 1: Initialize ParqueDB
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 1: Initialize Database ---\n')

  // Create a ParqueDB instance with in-memory storage
  // For production, use FsBackend (Node.js) or R2Backend (Cloudflare Workers)
  const db = new ParqueDB({
    storage: new MemoryBackend(),
  })

  console.log('Database initialized with MemoryBackend')

  // ---------------------------------------------------------------------------
  // Step 2: Create Entities (Users)
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 2: Create Users ---\n')

  // Create users using proxy-based collection access (db.Users)
  const alice = await db.Users.create({
    $type: 'User',
    name: 'Alice Johnson',
    email: 'alice@example.com',
    role: 'admin',
    bio: 'Platform administrator and tech lead',
  } as { $type: string; name: string } & User)

  const bob = await db.Users.create({
    $type: 'User',
    name: 'Bob Smith',
    email: 'bob@example.com',
    role: 'author',
    bio: 'Technical writer and software developer',
  } as { $type: string; name: string } & User)

  const charlie = await db.Users.create({
    $type: 'User',
    name: 'Charlie Brown',
    email: 'charlie@example.com',
    role: 'reader',
  } as { $type: string; name: string } & User)

  console.log(`Created user: ${alice.name} (${alice.$id})`)
  console.log(`Created user: ${bob.name} (${bob.$id})`)
  console.log(`Created user: ${charlie.name} (${charlie.$id})`)

  // ---------------------------------------------------------------------------
  // Step 3: Create Posts with Relationships
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 3: Create Posts ---\n')

  // Create posts - we'll establish author relationships using $link
  const post1 = await db.Posts.create({
    $type: 'Post',
    name: 'Getting Started with ParqueDB',
    title: 'Getting Started with ParqueDB',
    content: 'ParqueDB is a hybrid database built on Apache Parquet that combines the best of relational, document, and graph databases...',
    status: 'published',
    tags: ['tutorial', 'parquedb', 'database'],
    viewCount: 150,
  } as { $type: string; name: string } & Post)

  const post2 = await db.Posts.create({
    $type: 'Post',
    name: 'Advanced Query Patterns',
    title: 'Advanced Query Patterns',
    content: 'Learn how to use MongoDB-style filters, aggregation pipelines, and graph traversal in ParqueDB...',
    status: 'published',
    tags: ['advanced', 'queries', 'tutorial'],
    viewCount: 75,
  } as { $type: string; name: string } & Post)

  const post3 = await db.Posts.create({
    $type: 'Post',
    name: 'Draft: Performance Tips',
    title: 'Performance Tips for ParqueDB',
    content: 'Work in progress - covering indexes, bloom filters, and predicate pushdown...',
    status: 'draft',
    tags: ['performance', 'optimization'],
    viewCount: 0,
  } as { $type: string; name: string } & Post)

  console.log(`Created post: "${post1.title}" (${post1.$id})`)
  console.log(`Created post: "${post2.title}" (${post2.$id})`)
  console.log(`Created post: "${post3.title}" (${post3.$id})`)

  // ---------------------------------------------------------------------------
  // Step 4: Create Comments
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 4: Create Comments ---\n')

  const comment1 = await db.Comments.create({
    $type: 'Comment',
    name: 'Great introduction!',
    body: 'This tutorial helped me get started quickly. Thanks!',
    rating: 5,
  } as { $type: string; name: string } & Comment)

  const comment2 = await db.Comments.create({
    $type: 'Comment',
    name: 'Very helpful',
    body: 'Clear explanations and good examples.',
    rating: 4,
  } as { $type: string; name: string } & Comment)

  const comment3 = await db.Comments.create({
    $type: 'Comment',
    name: 'Needs more detail',
    body: 'Would love to see more advanced examples.',
    rating: 3,
  } as { $type: string; name: string } & Comment)

  console.log(`Created ${3} comments`)

  // ---------------------------------------------------------------------------
  // Step 5: Establish Relationships with $link
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 5: Establish Relationships ---\n')

  // Link posts to authors
  await db.Posts.update(post1.$id, {
    $link: { author: [alice.$id] },
  })
  console.log(`Linked "${post1.title}" to author ${alice.name}`)

  await db.Posts.update(post2.$id, {
    $link: { author: [bob.$id] },
  })
  console.log(`Linked "${post2.title}" to author ${bob.name}`)

  await db.Posts.update(post3.$id, {
    $link: { author: [bob.$id] },
  })
  console.log(`Linked "${post3.title}" to author ${bob.name}`)

  // Link comments to posts and authors
  await db.Comments.update(comment1.$id, {
    $link: {
      post: [post1.$id],
      author: [charlie.$id],
    },
  })
  console.log(`Linked comment to post and author`)

  await db.Comments.update(comment2.$id, {
    $link: {
      post: [post1.$id],
      author: [bob.$id],
    },
  })

  await db.Comments.update(comment3.$id, {
    $link: {
      post: [post2.$id],
      author: [charlie.$id],
    },
  })

  // ---------------------------------------------------------------------------
  // Step 6: Query with Filters
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 6: Query with Filters ---\n')

  // Note: ParqueDB's find() returns a PaginatedResult with { items, hasMore, nextCursor, total }
  // Use result.items to access the array of entities

  // Find all published posts
  const publishedResult = await db.Posts.find({ status: 'published' })
  console.log(`Published posts: ${publishedResult.items.length}`)
  for (const p of publishedResult.items) {
    console.log(`  - ${p.title}`)
  }

  // Find posts with high view count
  const popularResult = await db.Posts.find({
    viewCount: { $gte: 100 },
  })
  console.log(`\nPopular posts (100+ views): ${popularResult.items.length}`)

  // Find posts with specific tag using $in
  const tutorialResult = await db.Posts.find({
    tags: { $in: ['tutorial'] },
  })
  console.log(`Tutorial posts: ${tutorialResult.items.length}`)

  // Complex filter with $and
  const filteredResult = await db.Posts.find({
    $and: [{ status: 'published' }, { viewCount: { $gt: 50 } }],
  })
  console.log(`Published posts with 50+ views: ${filteredResult.items.length}`)

  // ---------------------------------------------------------------------------
  // Step 7: Sorting and Pagination
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 7: Sorting and Pagination ---\n')

  // Sort posts by view count descending
  const sortedResult = await db.Posts.find(
    {},
    {
      sort: { viewCount: -1 },
      limit: 2,
    }
  )
  console.log('Top 2 posts by views:')
  for (const p of sortedResult.items) {
    console.log(`  - ${p.title} (${(p as any).viewCount} views)`)
  }

  // Paginated results - find() already returns PaginatedResult
  const paginatedResult = await db.Posts.find({}, { limit: 2 })
  console.log(`\nPaginated results: ${paginatedResult.items.length} items, hasMore: ${paginatedResult.hasMore}, total: ${paginatedResult.total}`)

  // ---------------------------------------------------------------------------
  // Step 8: Update Operations
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 8: Update Operations ---\n')

  // Update with $set
  await db.Posts.update(post1.$id, {
    $set: { viewCount: 200 },
  })
  console.log(`Updated post1 viewCount to 200`)

  // Update with $inc
  await db.Posts.update(post2.$id, {
    $inc: { viewCount: 25 },
  })
  const updatedPost2 = await db.Posts.get(post2.$id)
  console.log(`Incremented post2 viewCount by 25, now: ${(updatedPost2 as any).viewCount}`)

  // Update with $push to add tags
  await db.Posts.update(post1.$id, {
    $push: { tags: 'featured' },
  })
  const updatedPost1 = await db.Posts.get(post1.$id)
  console.log(`Added 'featured' tag to post1, tags: ${(updatedPost1 as any).tags?.join(', ')}`)

  // Update status from draft to published
  await db.Posts.update(post3.$id, {
    $set: { status: 'published' },
  })
  console.log(`Changed post3 status from draft to published`)

  // ---------------------------------------------------------------------------
  // Step 9: Aggregation Pipeline (using standalone Collection)
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 9: Aggregation Pipeline ---\n')

  // Note: Aggregation is available on the standalone Collection class
  // For this demo, we'll show how it works with sample data

  // Create a separate collection for aggregation demo
  clearGlobalStorage()
  const aggPosts = new Collection<Post>('agg_posts')

  await aggPosts.create({ $type: 'Post', name: 'P1', title: 'P1', content: 'C1', status: 'published', viewCount: 100, tags: ['tutorial', 'featured'] })
  await aggPosts.create({ $type: 'Post', name: 'P2', title: 'P2', content: 'C2', status: 'published', viewCount: 200, tags: ['tutorial'] })
  await aggPosts.create({ $type: 'Post', name: 'P3', title: 'P3', content: 'C3', status: 'draft', viewCount: 50, tags: ['draft', 'featured'] })

  // Get view count statistics by status
  const statsResult = await aggPosts.aggregate([
    { $match: {} },
    {
      $group: {
        _id: '$status',
        totalViews: { $sum: '$viewCount' },
        avgViews: { $avg: '$viewCount' },
        count: { $count: {} },
      },
    },
    { $sort: { totalViews: -1 } },
  ])
  console.log('View statistics by status:')
  for (const stat of statsResult) {
    console.log(`  ${(stat as any)._id}: ${(stat as any).count} posts, ${(stat as any).totalViews} total views, ${((stat as any).avgViews || 0).toFixed(1)} avg`)
  }

  // Count posts per tag
  const tagStats = await aggPosts.aggregate([
    { $unwind: '$tags' },
    {
      $group: {
        _id: '$tags',
        count: { $count: {} },
      },
    },
    { $sort: { count: -1 } },
    { $limit: 5 },
  ])
  console.log('\nTop 5 tags:')
  for (const tag of tagStats) {
    console.log(`  ${(tag as any)._id}: ${(tag as any).count} posts`)
  }

  clearGlobalStorage()

  // ---------------------------------------------------------------------------
  // Step 10: Delete Operations
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 10: Delete Operations ---\n')

  // Create a temporary post
  const tempPost = await db.Posts.create({
    $type: 'Post',
    name: 'Temporary Post',
    title: 'Temporary Post',
    content: 'This will be deleted',
    status: 'draft',
  } as { $type: string; name: string } & Post)

  console.log(`Created temporary post: ${tempPost.$id}`)

  // Soft delete (default)
  const deleteResult = await db.Posts.delete(tempPost.$id)
  console.log(`Soft deleted: ${deleteResult.deletedCount} post(s)`)

  // Verify it's not found by default using findOne
  const foundPost = await db.Posts.findOne({ name: 'Temporary Post' })
  console.log(`Found after soft delete (default): ${foundPost ? 1 : 0}`)

  // But can be found with includeDeleted
  const foundWithDeleted = await db.Posts.findOne({ name: 'Temporary Post' }, { includeDeleted: true })
  console.log(`Found with includeDeleted: ${foundWithDeleted ? 1 : 0}`)

  // ---------------------------------------------------------------------------
  // Step 11: Count and Exists
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 11: Count and Check Existence ---\n')

  // Count using find().total
  const totalResult = await db.Posts.find()
  const publishedCountResult = await db.Posts.find({ status: 'published' })
  console.log(`Total posts: ${totalResult.total}`)
  console.log(`Published posts: ${publishedCountResult.total}`)

  // Check existence using get() which returns null for non-existent entities
  const existingPost = await db.Posts.get(post1.$id)
  console.log(`Post1 exists: ${existingPost !== null}`)

  const fakePost = await db.Posts.get('posts/fake-id')
  console.log(`Fake post exists: ${fakePost !== null}`)

  // ---------------------------------------------------------------------------
  // Step 12: Query Builder (Fluent API)
  // ---------------------------------------------------------------------------

  console.log('\n--- Step 12: Query Builder (Fluent API) ---\n')

  // Using the standalone Collection for query builder demo
  clearGlobalStorage()

  const posts = new Collection<Post>('posts')

  // Create some test data
  await posts.create({ $type: 'Post', name: 'Post A', title: 'Post A', content: 'Content A', status: 'published', viewCount: 100 })
  await posts.create({ $type: 'Post', name: 'Post B', title: 'Post B', content: 'Content B', status: 'draft', viewCount: 50 })
  await posts.create({ $type: 'Post', name: 'Post C', title: 'Post C', content: 'Content C', status: 'published', viewCount: 200 })

  // Use the fluent query builder
  const builderResults = await posts
    .builder()
    .where('status', 'eq', 'published')
    .andWhere('viewCount', 'gte', 100)
    .orderBy('viewCount', 'desc')
    .limit(10)
    .find()

  console.log('Query Builder results (published with 100+ views):')
  for (const p of builderResults) {
    console.log(`  - ${(p as any).title} (${(p as any).viewCount} views)`)
  }

  // Clean up
  clearGlobalStorage()

  // ---------------------------------------------------------------------------
  // Summary
  // ---------------------------------------------------------------------------

  console.log('\n' + '='.repeat(70))
  console.log('Example Complete!')
  console.log('='.repeat(70))
  console.log(`
Summary of demonstrated features:
1. Database initialization with MemoryBackend
2. Creating entities with $type and name
3. Establishing relationships with $link
4. MongoDB-style filtering ($eq, $gte, $in, $and, etc.)
5. Sorting and pagination
6. Update operators ($set, $inc, $push)
7. Aggregation pipelines ($match, $group, $sort, $unwind)
8. Soft delete and includeDeleted option
9. Count and exists operations
10. Fluent Query Builder API

For more examples, see:
- examples/ai-database/ - AI integration examples
- examples/imdb/ - Large dataset example
- examples/onet/ - Occupational database with relationships
`)

  // Clean up
  db.dispose()
}

// Run the example
main().catch(console.error)
