/**
 * Blog Dataset Query Patterns
 *
 * 10 real-world query patterns for a Blog dataset, designed for
 * benchmarking ParqueDB performance across different storage backends.
 *
 * Supports both:
 * - HTTP patterns for deployed workers (parquedb.workers.do)
 * - Local patterns for FsBackend testing
 *
 * Based on BENCHMARK-DESIGN.md specifications.
 */

// =============================================================================
// Data Types
// =============================================================================

/**
 * Author entity for blog posts
 */
export interface Author {
  $id: string
  $type: 'Author'
  name: string
  email: string
  username: string
  bio?: string
  avatarUrl?: string
  followerCount: number
  active: boolean
  createdAt: string
  updatedAt?: string
}

/**
 * Blog post entity
 */
export interface Post {
  $id: string
  $type: 'Post'
  name: string
  title: string
  slug: string
  content: string
  excerpt?: string
  status: 'draft' | 'published' | 'archived'
  authorId: string
  tags: string[]
  views: number
  likes: number
  commentCount: number
  readTimeMinutes: number
  featuredImageUrl?: string
  publishedAt?: string
  createdAt: string
  updatedAt?: string
}

/**
 * Comment entity for blog posts
 */
export interface Comment {
  $id: string
  $type: 'Comment'
  name: string
  postId: string
  authorId: string
  text: string
  likes: number
  approved: boolean
  parentCommentId?: string
  createdAt: string
  updatedAt?: string
}

/**
 * Tag entity for categorizing posts
 */
export interface Tag {
  $id: string
  $type: 'Tag'
  name: string
  slug: string
  description?: string
  postCount: number
  featured: boolean
  color?: string
  createdAt: string
}

// =============================================================================
// Query Pattern Types
// =============================================================================

/**
 * Categories of query patterns based on their characteristics
 */
export type QueryCategory =
  | 'point-lookup'
  | 'filtered'
  | 'relationship'
  | 'fts'
  | 'aggregation'
  | 'compound'
  | 'range'
  | 'top-n'
  | 'array-filter'

/**
 * Query pattern definition for benchmarking
 */
export interface QueryPattern {
  /** Human-readable pattern name */
  name: string
  /** Category for grouping and analysis */
  category: QueryCategory
  /** Target latency in milliseconds */
  targetMs: number
  /** Description of the query pattern */
  description: string
  /**
   * Execute the query via HTTP
   * @param baseUrl - Base URL (default: https://parquedb.workers.do)
   * @returns Response from the API
   */
  query: (baseUrl?: string) => Promise<Response>
}

/**
 * Local query pattern for FsBackend testing
 */
export interface LocalQueryPattern {
  /** Human-readable pattern name */
  name: string
  /** Category for grouping and analysis */
  category: QueryCategory
  /** Target latency in milliseconds */
  targetMs: number
  /** Description of the query pattern */
  description: string
  /** Collection name to query */
  collection: string
  /** MongoDB-style filter */
  filter?: Record<string, unknown>
  /** Query options (sort, limit, skip, project) */
  options?: {
    sort?: Record<string, 1 | -1>
    limit?: number
    skip?: number
    project?: Record<string, 0 | 1>
  }
  /** Aggregation pipeline (if using aggregation) */
  pipeline?: Record<string, unknown>[]
  /** Additional queries for join patterns */
  additionalQueries?: Array<{
    collection: string
    filter: Record<string, unknown>
    options?: Record<string, unknown>
  }>
}

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_BASE_URL = 'https://parquedb.workers.do'

// Sample data IDs for point lookups (would be replaced with real IDs in production)
const SAMPLE_POST_ID = 'posts/post-001'
const SAMPLE_AUTHOR_ID = 'authors/author-001'

// Date helpers for range queries
const now = new Date()
const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
const oneMonthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)

// =============================================================================
// HTTP Query Patterns (for deployed workers)
// =============================================================================

/**
 * Blog query patterns for HTTP-based benchmarking
 *
 * These patterns make HTTP requests to the deployed ParqueDB worker
 * and are suitable for production performance testing.
 */
export const blogPatterns: QueryPattern[] = [
  // ---------------------------------------------------------------------------
  // Pattern 1: Published posts (paginated) - Filtered + sorted
  // ---------------------------------------------------------------------------
  {
    name: 'Published posts (paginated)',
    category: 'filtered',
    targetMs: 20,
    description: 'Get published posts sorted by date, paginated (page 1, 20 per page)',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('filter', JSON.stringify({ status: 'published' }))
      url.searchParams.set('sort', JSON.stringify({ publishedAt: -1 }))
      url.searchParams.set('limit', '20')
      url.searchParams.set('skip', '0')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 2: Posts by author - Filtered
  // ---------------------------------------------------------------------------
  {
    name: 'Posts by author',
    category: 'filtered',
    targetMs: 20,
    description: 'Get all posts by a specific author',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('filter', JSON.stringify({ authorId: SAMPLE_AUTHOR_ID }))
      url.searchParams.set('sort', JSON.stringify({ createdAt: -1 }))
      url.searchParams.set('limit', '50')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 3: Posts by tag - Array filter
  // ---------------------------------------------------------------------------
  {
    name: 'Posts by tag',
    category: 'array-filter',
    targetMs: 30,
    description: 'Get posts containing a specific tag (array field query)',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('filter', JSON.stringify({
        tags: { $in: ['javascript'] },
        status: 'published',
      }))
      url.searchParams.set('sort', JSON.stringify({ publishedAt: -1 }))
      url.searchParams.set('limit', '20')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 4: Full-text search - FTS
  // ---------------------------------------------------------------------------
  {
    name: 'Full-text search',
    category: 'fts',
    targetMs: 50,
    description: 'Search posts by title/content using full-text search',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('q', 'typescript tutorial')
      url.searchParams.set('filter', JSON.stringify({ status: 'published' }))
      url.searchParams.set('limit', '20')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 5: Recent posts (last week) - Range
  // ---------------------------------------------------------------------------
  {
    name: 'Recent posts (last week)',
    category: 'range',
    targetMs: 20,
    description: 'Get posts published in the last 7 days',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('filter', JSON.stringify({
        status: 'published',
        publishedAt: { $gte: oneWeekAgo.toISOString() },
      }))
      url.searchParams.set('sort', JSON.stringify({ publishedAt: -1 }))
      url.searchParams.set('limit', '50')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 6: Popular posts (by views) - Top-N
  // ---------------------------------------------------------------------------
  {
    name: 'Popular posts (by views)',
    category: 'top-n',
    targetMs: 20,
    description: 'Get top 10 most viewed published posts',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('filter', JSON.stringify({ status: 'published' }))
      url.searchParams.set('sort', JSON.stringify({ views: -1 }))
      url.searchParams.set('limit', '10')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 7: Single post + author - Point + join
  // ---------------------------------------------------------------------------
  {
    name: 'Single post + author',
    category: 'point-lookup',
    targetMs: 10,
    description: 'Get a single post by ID and include author details',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      // First get the post
      const postUrl = new URL(`/api/posts/${encodeURIComponent(SAMPLE_POST_ID)}`, baseUrl)
      postUrl.searchParams.set('include', 'author')

      return fetch(postUrl.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 8: Posts with comment counts - Aggregation
  // ---------------------------------------------------------------------------
  {
    name: 'Posts with comment counts',
    category: 'aggregation',
    targetMs: 100,
    description: 'Get posts with aggregated comment counts',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts/aggregate', baseUrl)

      return fetch(url.toString(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pipeline: [
            { $match: { status: 'published' } },
            {
              $lookup: {
                from: 'comments',
                localField: '$id',
                foreignField: 'postId',
                as: 'comments',
              },
            },
            {
              $addFields: {
                commentCount: { $size: '$comments' },
              },
            },
            {
              $project: {
                comments: 0,
              },
            },
            { $sort: { commentCount: -1 } },
            { $limit: 20 },
          ],
        }),
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 9: Draft posts for author - Compound
  // ---------------------------------------------------------------------------
  {
    name: 'Draft posts for author',
    category: 'compound',
    targetMs: 20,
    description: 'Get draft posts for a specific author (compound filter)',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('filter', JSON.stringify({
        $and: [
          { authorId: SAMPLE_AUTHOR_ID },
          { status: 'draft' },
        ],
      }))
      url.searchParams.set('sort', JSON.stringify({ updatedAt: -1 }))
      url.searchParams.set('limit', '50')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 10: Posts updated since - Range
  // ---------------------------------------------------------------------------
  {
    name: 'Posts updated since',
    category: 'range',
    targetMs: 20,
    description: 'Get posts updated since a given timestamp (for sync/cache invalidation)',
    query: async (baseUrl = DEFAULT_BASE_URL) => {
      const url = new URL('/api/posts', baseUrl)
      url.searchParams.set('filter', JSON.stringify({
        updatedAt: { $gte: oneMonthAgo.toISOString() },
      }))
      url.searchParams.set('sort', JSON.stringify({ updatedAt: -1 }))
      url.searchParams.set('limit', '100')

      return fetch(url.toString(), {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
      })
    },
  },
]

// =============================================================================
// Local Query Patterns (for FsBackend testing)
// =============================================================================

/**
 * Blog query patterns for local FsBackend testing
 *
 * These patterns are designed to be executed directly against
 * FsBackend or MemoryBackend for local development and CI testing.
 */
export const blogLocalPatterns: LocalQueryPattern[] = [
  // ---------------------------------------------------------------------------
  // Pattern 1: Published posts (paginated) - Filtered + sorted
  // ---------------------------------------------------------------------------
  {
    name: 'Published posts (paginated)',
    category: 'filtered',
    targetMs: 20,
    description: 'Get published posts sorted by date, paginated (page 1, 20 per page)',
    collection: 'posts',
    filter: { status: 'published' },
    options: {
      sort: { publishedAt: -1 },
      limit: 20,
      skip: 0,
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 2: Posts by author - Filtered
  // ---------------------------------------------------------------------------
  {
    name: 'Posts by author',
    category: 'filtered',
    targetMs: 20,
    description: 'Get all posts by a specific author',
    collection: 'posts',
    filter: { authorId: SAMPLE_AUTHOR_ID },
    options: {
      sort: { createdAt: -1 },
      limit: 50,
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 3: Posts by tag - Array filter
  // ---------------------------------------------------------------------------
  {
    name: 'Posts by tag',
    category: 'array-filter',
    targetMs: 30,
    description: 'Get posts containing a specific tag (array field query)',
    collection: 'posts',
    filter: {
      tags: { $in: ['javascript'] },
      status: 'published',
    },
    options: {
      sort: { publishedAt: -1 },
      limit: 20,
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 4: Full-text search - FTS
  // ---------------------------------------------------------------------------
  {
    name: 'Full-text search',
    category: 'fts',
    targetMs: 50,
    description: 'Search posts by title/content using regex (FTS simulation)',
    collection: 'posts',
    filter: {
      $or: [
        { title: { $regex: 'typescript', $options: 'i' } },
        { content: { $regex: 'typescript', $options: 'i' } },
      ],
      status: 'published',
    },
    options: {
      limit: 20,
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 5: Recent posts (last week) - Range
  // ---------------------------------------------------------------------------
  {
    name: 'Recent posts (last week)',
    category: 'range',
    targetMs: 20,
    description: 'Get posts published in the last 7 days',
    collection: 'posts',
    filter: {
      status: 'published',
      publishedAt: { $gte: oneWeekAgo.toISOString() },
    },
    options: {
      sort: { publishedAt: -1 },
      limit: 50,
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 6: Popular posts (by views) - Top-N
  // ---------------------------------------------------------------------------
  {
    name: 'Popular posts (by views)',
    category: 'top-n',
    targetMs: 20,
    description: 'Get top 10 most viewed published posts',
    collection: 'posts',
    filter: { status: 'published' },
    options: {
      sort: { views: -1 },
      limit: 10,
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 7: Single post + author - Point + join
  // ---------------------------------------------------------------------------
  {
    name: 'Single post + author',
    category: 'point-lookup',
    targetMs: 10,
    description: 'Get a single post by ID and its author (two queries)',
    collection: 'posts',
    filter: { $id: SAMPLE_POST_ID },
    options: {
      limit: 1,
    },
    additionalQueries: [
      {
        collection: 'authors',
        filter: { $id: SAMPLE_AUTHOR_ID },
      },
    ],
  },

  // ---------------------------------------------------------------------------
  // Pattern 8: Posts with comment counts - Aggregation
  // ---------------------------------------------------------------------------
  {
    name: 'Posts with comment counts',
    category: 'aggregation',
    targetMs: 100,
    description: 'Get posts with aggregated comment counts',
    collection: 'posts',
    pipeline: [
      { $match: { status: 'published' } },
      {
        $lookup: {
          from: 'comments',
          localField: '$id',
          foreignField: 'postId',
          as: 'comments',
        },
      },
      {
        $addFields: {
          commentCount: { $size: '$comments' },
        },
      },
      {
        $project: {
          comments: 0,
        },
      },
      { $sort: { commentCount: -1 } },
      { $limit: 20 },
    ],
  },

  // ---------------------------------------------------------------------------
  // Pattern 9: Draft posts for author - Compound
  // ---------------------------------------------------------------------------
  {
    name: 'Draft posts for author',
    category: 'compound',
    targetMs: 20,
    description: 'Get draft posts for a specific author (compound filter)',
    collection: 'posts',
    filter: {
      $and: [
        { authorId: SAMPLE_AUTHOR_ID },
        { status: 'draft' },
      ],
    },
    options: {
      sort: { updatedAt: -1 },
      limit: 50,
    },
  },

  // ---------------------------------------------------------------------------
  // Pattern 10: Posts updated since - Range
  // ---------------------------------------------------------------------------
  {
    name: 'Posts updated since',
    category: 'range',
    targetMs: 20,
    description: 'Get posts updated since a given timestamp (for sync/cache invalidation)',
    collection: 'posts',
    filter: {
      updatedAt: { $gte: oneMonthAgo.toISOString() },
    },
    options: {
      sort: { updatedAt: -1 },
      limit: 100,
    },
  },
]

// =============================================================================
// Test Data Generation
// =============================================================================

/**
 * Generate sample blog test data for local benchmarking
 *
 * Creates a realistic blog dataset with:
 * - 20 authors with varying follower counts
 * - 500 posts across different statuses and tags
 * - 2000 comments distributed across posts
 * - 15 tags with varying popularity
 *
 * @param options - Configuration options
 * @returns Generated test data
 */
export function generateBlogTestData(options: {
  authorCount?: number
  postCount?: number
  commentCount?: number
  tagCount?: number
} = {}): {
  authors: Author[]
  posts: Post[]
  comments: Comment[]
  tags: Tag[]
} {
  const {
    authorCount = 20,
    postCount = 500,
    commentCount = 2000,
    tagCount = 15,
  } = options

  const authors: Author[] = []
  const posts: Post[] = []
  const comments: Comment[] = []
  const tags: Tag[] = []

  // Common tag names for realistic distribution
  const tagNames = [
    'javascript', 'typescript', 'react', 'nodejs', 'python',
    'devops', 'cloud', 'aws', 'docker', 'kubernetes',
    'database', 'api', 'testing', 'security', 'performance',
  ]

  const statuses: Post['status'][] = ['draft', 'published', 'archived']
  const statusWeights = [0.15, 0.75, 0.10] // 15% draft, 75% published, 10% archived

  // Generate tags
  for (let i = 0; i < tagCount && i < tagNames.length; i++) {
    const tagName = tagNames[i]!
    tags.push({
      $id: `tags/tag-${i.toString().padStart(3, '0')}`,
      $type: 'Tag',
      name: tagName.charAt(0).toUpperCase() + tagName.slice(1),
      slug: tagName,
      description: `Posts about ${tagName}`,
      postCount: 0, // Will be updated after posts are generated
      featured: i < 5, // First 5 tags are featured
      color: `#${Math.floor(Math.random() * 16777215).toString(16).padStart(6, '0')}`,
      createdAt: randomDate(new Date('2020-01-01'), new Date('2023-01-01')).toISOString(),
    })
  }

  // Generate authors
  for (let i = 0; i < authorCount; i++) {
    const createdAt = randomDate(new Date('2020-01-01'), new Date('2023-06-01'))
    authors.push({
      $id: `authors/author-${i.toString().padStart(3, '0')}`,
      $type: 'Author',
      name: `Author ${i + 1}`,
      email: `author${i + 1}@example.com`,
      username: `author_${i + 1}`,
      bio: `Technical writer and developer. Writing about software development since ${createdAt.getFullYear()}.`,
      avatarUrl: `https://api.dicebear.com/7.x/avatars/svg?seed=author${i}`,
      followerCount: randomInt(100, 50000),
      active: Math.random() > 0.1, // 90% active
      createdAt: createdAt.toISOString(),
      updatedAt: randomDate(createdAt, new Date()).toISOString(),
    })
  }

  // Generate posts
  const tagPostCounts = new Map<string, number>()

  for (let i = 0; i < postCount; i++) {
    const author = authors[randomInt(0, authorCount - 1)]!
    const createdAt = randomDate(new Date('2021-01-01'), new Date())

    // Determine status based on weights
    const rand = Math.random()
    let statusIndex = 0
    let cumWeight = 0
    for (let j = 0; j < statusWeights.length; j++) {
      cumWeight += statusWeights[j]!
      if (rand < cumWeight) {
        statusIndex = j
        break
      }
    }
    const status = statuses[statusIndex]!

    // Assign 1-4 random tags
    const postTagCount = randomInt(1, 4)
    const postTags = randomSubset(tagNames.slice(0, tagCount), postTagCount)

    // Update tag post counts
    for (const tag of postTags) {
      tagPostCounts.set(tag, (tagPostCounts.get(tag) || 0) + 1)
    }

    const publishedAt = status === 'published'
      ? randomDate(createdAt, new Date()).toISOString()
      : undefined

    posts.push({
      $id: `posts/post-${i.toString().padStart(3, '0')}`,
      $type: 'Post',
      name: `Post ${i + 1}`,
      title: `Blog Post Title ${i + 1}: ${generateTitle()}`,
      slug: `blog-post-${i + 1}`,
      content: generateContent(),
      excerpt: `This is the excerpt for blog post ${i + 1}. It provides a brief summary of the content.`,
      status,
      authorId: author.$id,
      tags: postTags,
      views: status === 'published' ? randomInt(10, 50000) : randomInt(0, 100),
      likes: status === 'published' ? randomInt(0, 1000) : randomInt(0, 10),
      commentCount: 0, // Will be updated after comments are generated
      readTimeMinutes: randomInt(2, 15),
      featuredImageUrl: Math.random() > 0.3
        ? `https://picsum.photos/800/400?random=${i}`
        : undefined,
      publishedAt,
      createdAt: createdAt.toISOString(),
      updatedAt: randomDate(createdAt, new Date()).toISOString(),
    })
  }

  // Update tag post counts
  for (const tag of tags) {
    tag.postCount = tagPostCounts.get(tag.slug) || 0
  }

  // Generate comments (only for published posts)
  const publishedPosts = posts.filter(p => p.status === 'published')
  const postCommentCounts = new Map<string, number>()

  for (let i = 0; i < commentCount; i++) {
    const post = publishedPosts[randomInt(0, publishedPosts.length - 1)]!
    const author = authors[randomInt(0, authorCount - 1)]!
    const createdAt = randomDate(new Date(post.createdAt), new Date())

    postCommentCounts.set(post.$id, (postCommentCounts.get(post.$id) || 0) + 1)

    comments.push({
      $id: `comments/comment-${i.toString().padStart(4, '0')}`,
      $type: 'Comment',
      name: `Comment ${i + 1}`,
      postId: post.$id,
      authorId: author.$id,
      text: generateCommentText(),
      likes: randomInt(0, 50),
      approved: Math.random() > 0.05, // 95% approved
      parentCommentId: Math.random() > 0.8 && i > 10
        ? `comments/comment-${randomInt(0, i - 1).toString().padStart(4, '0')}`
        : undefined,
      createdAt: createdAt.toISOString(),
      updatedAt: Math.random() > 0.9
        ? randomDate(createdAt, new Date()).toISOString()
        : undefined,
    })
  }

  // Update post comment counts
  for (const post of posts) {
    post.commentCount = postCommentCounts.get(post.$id) || 0
  }

  return { authors, posts, comments, tags }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate a random date within a range
 */
function randomDate(start: Date, end: Date): Date {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
}

/**
 * Generate a random integer within a range (inclusive)
 */
function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

/**
 * Pick a random subset of array elements
 */
function randomSubset<T>(arr: T[], count: number): T[] {
  const shuffled = [...arr].sort(() => Math.random() - 0.5)
  return shuffled.slice(0, Math.min(count, arr.length))
}

/**
 * Generate a realistic blog post title
 */
function generateTitle(): string {
  const topics = [
    'Getting Started with', 'Advanced Guide to', 'Understanding',
    'Best Practices for', 'How to Implement', 'Introduction to',
    'Deep Dive into', 'Mastering', 'The Complete Guide to',
    'Building Production-Ready', 'Scaling Your', 'Debugging',
  ]
  const subjects = [
    'TypeScript', 'React Hooks', 'Node.js APIs', 'GraphQL',
    'Microservices', 'Cloud Architecture', 'Docker Containers',
    'Kubernetes Deployments', 'Database Optimization', 'CI/CD Pipelines',
    'Authentication Systems', 'Error Handling', 'Performance Testing',
  ]
  return `${topics[randomInt(0, topics.length - 1)]} ${subjects[randomInt(0, subjects.length - 1)]}`
}

/**
 * Generate realistic blog post content
 */
function generateContent(): string {
  const paragraphs = randomInt(5, 15)
  const lines: string[] = []

  for (let i = 0; i < paragraphs; i++) {
    const sentences = randomInt(3, 8)
    const paragraph: string[] = []

    for (let j = 0; j < sentences; j++) {
      paragraph.push(generateSentence())
    }

    lines.push(paragraph.join(' '))
  }

  return lines.join('\n\n')
}

/**
 * Generate a random sentence
 */
function generateSentence(): string {
  const templates = [
    'This approach provides a solid foundation for building scalable applications.',
    'Understanding these concepts is essential for modern web development.',
    'The implementation follows industry best practices and coding standards.',
    'Performance improvements can be achieved through careful optimization.',
    'Error handling should always be a top priority in production systems.',
    'Testing ensures reliability and maintainability of the codebase.',
    'Documentation helps team members understand the system architecture.',
    'Code reviews are crucial for maintaining code quality over time.',
    'Monitoring and observability enable quick identification of issues.',
    'Security considerations should be built in from the start.',
  ]
  return templates[randomInt(0, templates.length - 1)]!
}

/**
 * Generate realistic comment text
 */
function generateCommentText(): string {
  const templates = [
    'Great article! This really helped me understand the topic better.',
    'Thanks for sharing this. I had the same issue and your solution worked.',
    'Could you elaborate more on the second point? I am a bit confused.',
    'I have been using this approach in production and it works great.',
    'Nice explanation. Looking forward to more content like this.',
    'This is exactly what I was looking for. Bookmarked!',
    'Have you considered using an alternative approach? Just curious.',
    'Excellent walkthrough. The code examples are very helpful.',
    'I would add that you should also consider edge cases here.',
    'Thank you! Finally someone explains this clearly.',
  ]
  return templates[randomInt(0, templates.length - 1)]!
}

// =============================================================================
// Utility Exports
// =============================================================================

/**
 * Get pattern by name
 */
export function getPatternByName(name: string): QueryPattern | undefined {
  return blogPatterns.find(p => p.name === name)
}

/**
 * Get local pattern by name
 */
export function getLocalPatternByName(name: string): LocalQueryPattern | undefined {
  return blogLocalPatterns.find(p => p.name === name)
}

/**
 * Get patterns by category
 */
export function getPatternsByCategory(category: QueryCategory): QueryPattern[] {
  return blogPatterns.filter(p => p.category === category)
}

/**
 * Get local patterns by category
 */
export function getLocalPatternsByCategory(category: QueryCategory): LocalQueryPattern[] {
  return blogLocalPatterns.filter(p => p.category === category)
}

/**
 * Summary of all blog patterns
 */
export const blogPatternSummary = {
  total: blogPatterns.length,
  byCategory: {
    'point-lookup': blogPatterns.filter(p => p.category === 'point-lookup').length,
    'filtered': blogPatterns.filter(p => p.category === 'filtered').length,
    'relationship': blogPatterns.filter(p => p.category === 'relationship').length,
    'fts': blogPatterns.filter(p => p.category === 'fts').length,
    'aggregation': blogPatterns.filter(p => p.category === 'aggregation').length,
    'compound': blogPatterns.filter(p => p.category === 'compound').length,
    'range': blogPatterns.filter(p => p.category === 'range').length,
    'top-n': blogPatterns.filter(p => p.category === 'top-n').length,
    'array-filter': blogPatterns.filter(p => p.category === 'array-filter').length,
  },
  targetLatencies: blogPatterns.map(p => ({ name: p.name, targetMs: p.targetMs })),
}
