/**
 * Realistic Workload Benchmarks for ParqueDB
 *
 * Simulates real-world usage patterns:
 * - API server simulation (mixed read/write)
 * - Batch import (bulk writes)
 * - Analytics queries (aggregations)
 * - Search workload (text matching)
 * - Graph traversal patterns
 */

import { describe, bench, beforeAll, beforeEach } from 'vitest'
import { Collection } from '../../src/Collection'
import type { Entity, EntityId, Filter } from '../../src/types'
import {
  randomElement,
  randomInt,
  randomString,
  randomSubset,
  randomDate,
  generateTestData,
  calculateStats,
  formatStats,
  getMemoryUsage,
  formatBytes,
} from './setup'

// =============================================================================
// Types for Realistic Workloads
// =============================================================================

interface User {
  email: string
  username: string
  displayName: string
  role: 'admin' | 'editor' | 'user' | 'guest'
  active: boolean
  lastLoginAt?: Date | undefined
  loginCount: number
  preferences: {
    theme: 'light' | 'dark' | 'auto'
    notifications: boolean
    language: string
  }
}

interface Article {
  title: string
  slug: string
  content: string
  excerpt: string
  status: 'draft' | 'review' | 'published' | 'archived'
  publishedAt?: Date | undefined
  views: number
  likes: number
  readTime: number
  tags: string[]
  featuredImageUrl?: string | undefined
  seoDescription?: string | undefined
}

interface Comment {
  text: string
  approved: boolean
  likes: number
  sentiment: 'positive' | 'neutral' | 'negative'
  flagged: boolean
}

interface PageView {
  path: string
  sessionId: string
  userId?: string | undefined
  userAgent: string
  referrer?: string | undefined
  duration: number
  timestamp: Date
}

interface SearchQuery {
  query: string
  results: number
  duration: number
  filters: string[]
  userId?: string | undefined
  timestamp: Date
}

// =============================================================================
// Data Generators
// =============================================================================

const roles: User['role'][] = ['admin', 'editor', 'user', 'guest']
const statuses: Article['status'][] = ['draft', 'review', 'published', 'archived']
const themes: User['preferences']['theme'][] = ['light', 'dark', 'auto']
const languages = ['en', 'es', 'fr', 'de', 'ja', 'zh', 'pt', 'ru', 'ko', 'it']
const tags = ['tech', 'science', 'business', 'health', 'sports', 'entertainment', 'politics', 'travel', 'food', 'lifestyle']
const sentiments: Comment['sentiment'][] = ['positive', 'neutral', 'negative']
const browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Mobile Safari', 'Chrome Mobile']

function generateUser(index: number): User & { $type: string; name: string } {
  return {
    $type: 'User',
    name: `User ${index}`,
    email: `user${index}@example.com`,
    username: `user_${index}`,
    displayName: `User ${randomString(8)}`,
    role: roles[index % roles.length],
    active: Math.random() > 0.1,
    lastLoginAt: Math.random() > 0.3 ? randomDate() : undefined,
    loginCount: randomInt(0, 1000),
    preferences: {
      theme: randomElement(themes),
      notifications: Math.random() > 0.3,
      language: randomElement(languages),
    },
  }
}

function generateArticle(index: number, authorId?: string): Article & { $type: string; name: string } {
  const status = statuses[index % statuses.length]
  return {
    $type: 'Article',
    name: `Article ${index}`,
    title: `Article Title ${index}: ${randomString(30)}`,
    slug: `article-${index}-${randomString(10).toLowerCase()}`,
    content: randomString(2000),
    excerpt: randomString(200),
    status,
    publishedAt: status === 'published' ? randomDate() : undefined,
    views: randomInt(0, 100000),
    likes: randomInt(0, 5000),
    readTime: randomInt(1, 30),
    tags: randomSubset(tags, randomInt(1, 5)),
    featuredImageUrl: Math.random() > 0.5 ? `https://images.example.com/${index}.jpg` : undefined,
    seoDescription: Math.random() > 0.5 ? randomString(150) : undefined,
  }
}

function generateComment(index: number): Comment & { $type: string; name: string } {
  return {
    $type: 'Comment',
    name: `Comment ${index}`,
    text: randomString(randomInt(20, 500)),
    approved: Math.random() > 0.1,
    likes: randomInt(0, 100),
    sentiment: randomElement(sentiments),
    flagged: Math.random() > 0.95,
  }
}

function generatePageView(index: number): PageView & { $type: string; name: string } {
  const paths = ['/home', '/about', '/blog', '/products', '/contact', '/pricing', '/docs', '/api', '/login', '/signup']
  return {
    $type: 'PageView',
    name: `PageView ${index}`,
    path: randomElement(paths),
    sessionId: `sess_${randomString(16)}`,
    userId: Math.random() > 0.4 ? `user_${randomInt(1, 1000)}` : undefined,
    userAgent: `${randomElement(browsers)}/100.0`,
    referrer: Math.random() > 0.5 ? `https://${randomElement(['google.com', 'twitter.com', 'facebook.com', 'reddit.com'])}` : undefined,
    duration: randomInt(5, 600),
    timestamp: randomDate(),
  }
}

// =============================================================================
// API Server Simulation (Mixed Read/Write)
// =============================================================================

describe('Realistic Workloads', () => {
  describe('API Server Simulation', () => {
    let users: Collection<User>
    let articles: Collection<Article>
    let comments: Collection<Comment>
    let userIds: string[] = []
    let articleIds: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()
      users = new Collection<User>(`api-users-${suffix}`)
      articles = new Collection<Article>(`api-articles-${suffix}`)
      comments = new Collection<Comment>(`api-comments-${suffix}`)

      // Seed data: 500 users, 2000 articles, 10000 comments
      for (let i = 0; i < 500; i++) {
        const user = await users.create(generateUser(i))
        userIds.push(user.$id as string)
      }

      for (let i = 0; i < 2000; i++) {
        const article = await articles.create(generateArticle(i, randomElement(userIds)))
        articleIds.push(article.$id as string)
      }

      for (let i = 0; i < 10000; i++) {
        await comments.create(generateComment(i))
      }
    })

    // Typical API read patterns
    bench('GET /users/:id - single user lookup', async () => {
      const userId = randomElement(userIds)
      await users.get(userId.split('/')[1])
    })

    bench('GET /articles?status=published - filtered list', async () => {
      await articles.find({ status: 'published' }, { limit: 20, sort: { publishedAt: -1 } })
    })

    bench('GET /articles?tag=tech - tag search', async () => {
      await articles.find({ tags: { $in: ['tech'] } }, { limit: 20, sort: { views: -1 } })
    })

    bench('GET /articles/popular - aggregation for top articles', async () => {
      await articles.aggregate([
        { $match: { status: 'published', views: { $gte: 1000 } } },
        { $sort: { likes: -1 } },
        { $limit: 10 },
        { $project: { title: 1, slug: 1, views: 1, likes: 1 } },
      ])
    })

    bench('GET /comments?approved=true - paginated comments', async () => {
      await comments.find({ approved: true }, { limit: 50, sort: { likes: -1 } })
    })

    // Typical API write patterns
    bench('POST /users - create user', async () => {
      await users.create(generateUser(Date.now()))
    })

    bench('PUT /articles/:id - update article', async () => {
      const articleId = randomElement(articleIds).split('/')[1]
      await articles.update(articleId, {
        $set: { title: `Updated Title ${Date.now()}` },
        $inc: { views: 1 },
      })
    })

    bench('POST /articles/:id/like - increment likes', async () => {
      const articleId = randomElement(articleIds).split('/')[1]
      await articles.update(articleId, {
        $inc: { likes: 1 },
      })
    })

    bench('PATCH /users/:id/preferences - partial update', async () => {
      const userId = randomElement(userIds).split('/')[1]
      await users.update(userId, {
        $set: { 'preferences.theme': randomElement(themes) },
      })
    })

    // Mixed read/write (realistic API mix: 80% reads, 20% writes)
    bench('mixed workload - 10 ops (80/20 read/write)', async () => {
      for (let i = 0; i < 10; i++) {
        if (Math.random() < 0.8) {
          // Read operation
          if (Math.random() < 0.5) {
            const userId = randomElement(userIds).split('/')[1]
            await users.get(userId)
          } else {
            await articles.find({ status: 'published' }, { limit: 10 })
          }
        } else {
          // Write operation
          const articleId = randomElement(articleIds).split('/')[1]
          await articles.update(articleId, { $inc: { views: 1 } })
        }
      }
    })

    bench('burst read - 50 concurrent-style reads', async () => {
      const operations = []
      for (let i = 0; i < 50; i++) {
        operations.push(articles.find({ status: 'published' }, { limit: 5 }))
      }
      await Promise.all(operations)
    })
  })

  // ===========================================================================
  // Batch Import (Bulk Writes)
  // ===========================================================================

  describe('Batch Import', () => {
    let importArticles: Collection<Article>
    let importNamespace: string

    beforeEach(() => {
      importNamespace = `import-${Date.now()}-${Math.random().toString(36).slice(2)}`
      importArticles = new Collection<Article>(importNamespace)
    })

    bench('batch import 100 entities', async () => {
      for (let i = 0; i < 100; i++) {
        await importArticles.create(generateArticle(i))
      }
    })

    bench('batch import 500 entities', async () => {
      for (let i = 0; i < 500; i++) {
        await importArticles.create(generateArticle(i))
      }
    }, { iterations: 10 })

    bench('batch import 1000 entities', async () => {
      for (let i = 0; i < 1000; i++) {
        await importArticles.create(generateArticle(i))
      }
    }, { iterations: 5 })

    bench('batch import with varied content sizes', async () => {
      for (let i = 0; i < 100; i++) {
        const article = generateArticle(i)
        // Vary content size: 500, 1000, 2000, 5000, 10000 chars
        const contentSizes = [500, 1000, 2000, 5000, 10000]
        article.content = randomString(contentSizes[i % contentSizes.length])
        await importArticles.create(article)
      }
    })

    bench('parallel batch import (5 parallel streams)', async () => {
      const streams = 5
      const perStream = 100

      const createBatch = async (startIdx: number) => {
        for (let i = 0; i < perStream; i++) {
          await importArticles.create(generateArticle(startIdx + i))
        }
      }

      await Promise.all(
        Array.from({ length: streams }, (_, i) => createBatch(i * perStream))
      )
    }, { iterations: 10 })
  })

  // ===========================================================================
  // Analytics Queries (Aggregations)
  // ===========================================================================

  describe('Analytics Queries', () => {
    let analyticsArticles: Collection<Article>
    let analyticsViews: Collection<PageView>

    beforeAll(async () => {
      const suffix = Date.now()
      analyticsArticles = new Collection<Article>(`analytics-articles-${suffix}`)
      analyticsViews = new Collection<PageView>(`analytics-views-${suffix}`)

      // Seed 5000 articles for analytics
      for (let i = 0; i < 5000; i++) {
        await analyticsArticles.create(generateArticle(i))
      }

      // Seed 20000 page views
      for (let i = 0; i < 20000; i++) {
        await analyticsViews.create(generatePageView(i))
      }
    })

    bench('count by status (group by)', async () => {
      await analyticsArticles.aggregate([
        {
          $group: {
            _id: '$status',
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
      ])
    })

    bench('average views by status', async () => {
      await analyticsArticles.aggregate([
        {
          $group: {
            _id: '$status',
            count: { $sum: 1 },
            avgViews: { $avg: '$views' },
            totalViews: { $sum: '$views' },
          },
        },
      ])
    })

    bench('top tags by article count', async () => {
      await analyticsArticles.aggregate([
        { $unwind: '$tags' },
        {
          $group: {
            _id: '$tags',
            count: { $sum: 1 },
            avgViews: { $avg: '$views' },
          },
        },
        { $sort: { count: -1 } },
        { $limit: 10 },
      ])
    })

    bench('views by path (page analytics)', async () => {
      await analyticsViews.aggregate([
        {
          $group: {
            _id: '$path',
            views: { $sum: 1 },
            avgDuration: { $avg: '$duration' },
          },
        },
        { $sort: { views: -1 } },
      ])
    })

    bench('unique sessions count', async () => {
      await analyticsViews.aggregate([
        {
          $group: {
            _id: '$sessionId',
          },
        },
        { $count: 'uniqueSessions' },
      ])
    })

    bench('complex analytics: engagement by tag', async () => {
      await analyticsArticles.aggregate([
        { $match: { status: 'published' } },
        { $unwind: '$tags' },
        {
          $group: {
            _id: '$tags',
            articleCount: { $sum: 1 },
            totalViews: { $sum: '$views' },
            totalLikes: { $sum: '$likes' },
            avgReadTime: { $avg: '$readTime' },
          },
        },
        { $sort: { totalViews: -1 } },
        { $limit: 5 },
      ])
    })

    bench('time-based aggregation (articles per day simulation)', async () => {
      await analyticsArticles.aggregate([
        { $match: { status: 'published', publishedAt: { $exists: true } } },
        {
          $group: {
            _id: null,
            count: { $sum: 1 },
            totalViews: { $sum: '$views' },
          },
        },
      ])
    })

    bench('funnel analysis simulation', async () => {
      // Simulate: views -> clicks -> conversions
      const paths = ['/home', '/products', '/checkout', '/success']
      const results: Record<string, number> = {}

      for (const path of paths) {
        const count = await analyticsViews.count({ path })
        results[path] = count
      }
    })
  })

  // ===========================================================================
  // Search Workload (Text Matching)
  // ===========================================================================

  describe('Search Workload', () => {
    let searchArticles: Collection<Article>
    let searchQueries: Collection<SearchQuery>
    const searchTerms = ['tech', 'science', 'business', 'breaking', 'update', 'review', 'guide', 'how-to']

    beforeAll(async () => {
      const suffix = Date.now()
      searchArticles = new Collection<Article>(`search-articles-${suffix}`)
      searchQueries = new Collection<SearchQuery>(`search-queries-${suffix}`)

      // Seed 3000 articles with searchable content
      for (let i = 0; i < 3000; i++) {
        const article = generateArticle(i)
        // Add search terms to title for easier testing
        if (i % 10 === 0) {
          article.title = `${randomElement(searchTerms)}: ${article.title}`
        }
        await searchArticles.create(article)
      }
    })

    bench('prefix search (autocomplete)', async () => {
      await searchArticles.find(
        { title: { $regex: '^tech', $options: 'i' } },
        { limit: 10, sort: { views: -1 } }
      )
    })

    bench('substring search (contains)', async () => {
      await searchArticles.find(
        { title: { $regex: 'update', $options: 'i' } },
        { limit: 20 }
      )
    })

    bench('multi-field search simulation', async () => {
      const term = randomElement(searchTerms)
      await searchArticles.find(
        {
          $or: [
            { title: { $regex: term, $options: 'i' } },
            { excerpt: { $regex: term, $options: 'i' } },
          ],
        },
        { limit: 20, sort: { views: -1 } }
      )
    })

    bench('faceted search (tag + status filter)', async () => {
      await searchArticles.find(
        {
          $and: [
            { tags: { $in: ['tech', 'science'] } },
            { status: 'published' },
          ],
        },
        { limit: 20, sort: { publishedAt: -1 } }
      )
    })

    bench('search with highlight fields (projection)', async () => {
      await searchArticles.find(
        { title: { $regex: 'tech', $options: 'i' } },
        {
          limit: 20,
          project: { title: 1, excerpt: 1, slug: 1, views: 1 },
        }
      )
    })

    bench('search with range filters', async () => {
      await searchArticles.find(
        {
          status: 'published',
          views: { $gte: 1000 },
          readTime: { $lte: 10 },
        },
        { limit: 20, sort: { likes: -1 } }
      )
    })

    bench('search count (for pagination)', async () => {
      await searchArticles.count({
        $and: [
          { status: 'published' },
          { tags: { $in: ['tech'] } },
        ],
      })
    })

    bench('typeahead search (very fast prefix)', async () => {
      await searchArticles.find(
        { slug: { $regex: '^article-1' } },
        { limit: 5, project: { title: 1, slug: 1 } }
      )
    })
  })

  // ===========================================================================
  // Graph Traversal Patterns
  // ===========================================================================

  describe('Graph Traversal Patterns', () => {
    let graphUsers: Collection<User>
    let graphArticles: Collection<Article>
    let graphComments: Collection<Comment>
    let graphUserIds: string[] = []
    let graphArticleIds: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()
      graphUsers = new Collection<User>(`graph-users-${suffix}`)
      graphArticles = new Collection<Article>(`graph-articles-${suffix}`)
      graphComments = new Collection<Comment>(`graph-comments-${suffix}`)

      // Create users
      for (let i = 0; i < 100; i++) {
        const user = await graphUsers.create(generateUser(i))
        graphUserIds.push(user.$id as string)
      }

      // Create articles with author relationships
      for (let i = 0; i < 500; i++) {
        const authorIdx = i % 100
        const article = await graphArticles.create({
          ...generateArticle(i),
          author: { 'Author': graphUserIds[authorIdx] as EntityId },
        })
        graphArticleIds.push(article.$id as string)

        // Create 3-10 comments per article
        const numComments = randomInt(3, 10)
        for (let c = 0; c < numComments; c++) {
          const commentAuthorIdx = randomInt(0, 99)
          await graphComments.create({
            ...generateComment(i * 10 + c),
            article: { 'Article': article.$id as EntityId },
            author: { 'Author': graphUserIds[commentAuthorIdx] as EntityId },
          })
        }
      }
    })

    bench('1-hop: get user articles', async () => {
      const userId = randomElement(graphUserIds)
      // Find articles where author matches user
      await graphArticles.find({ 'author.Author': { $eq: userId } })
    })

    bench('1-hop: get article comments', async () => {
      const articleId = randomElement(graphArticleIds)
      await graphComments.find({ 'article.Article': { $eq: articleId } })
    })

    bench('2-hop: user -> articles -> comments', async () => {
      const userId = randomElement(graphUserIds)
      const userArticles = await graphArticles.find(
        { 'author.Author': { $eq: userId } },
        { limit: 10 }
      )

      for (const article of userArticles.slice(0, 5)) {
        await graphComments.find(
          { 'article.Article': { $eq: article.$id } },
          { limit: 10 }
        )
      }
    })

    bench('fan-out: get co-commenters (users who commented on same articles)', async () => {
      const userId = randomElement(graphUserIds)

      // Get articles where user commented
      const userComments = await graphComments.find(
        { 'author.Author': { $eq: userId } },
        { limit: 20 }
      )

      // Get unique article IDs
      const articleIds = new Set<string>()
      for (const comment of userComments) {
        const articleRef = comment.article as Record<string, EntityId> | undefined
        if (articleRef) {
          for (const id of Object.values(articleRef)) {
            articleIds.add(id)
          }
        }
      }

      // Get other commenters on those articles
      const coCommenters = new Set<string>()
      for (const articleId of Array.from(articleIds).slice(0, 5)) {
        const otherComments = await graphComments.find(
          {
            'article.Article': { $eq: articleId },
            'author.Author': { $ne: userId },
          },
          { limit: 10 }
        )
        for (const c of otherComments) {
          const authorRef = c.author as Record<string, EntityId> | undefined
          if (authorRef) {
            for (const id of Object.values(authorRef)) {
              coCommenters.add(id)
            }
          }
        }
      }
    })

    bench('aggregation: top commenters', async () => {
      // Find users with most comments using aggregation
      await graphComments.aggregate([
        { $match: { approved: true } },
        { $limit: 1000 }, // Sample for performance
        {
          $group: {
            _id: null,
            totalComments: { $sum: 1 },
          },
        },
      ])
    })

    bench('path query: find connection depth', async () => {
      const user1 = graphUserIds[0]
      const user2 = graphUserIds[50]

      // Level 1: Direct articles
      const user1Articles = await graphArticles.find(
        { 'author.Author': { $eq: user1 } },
        { limit: 10 }
      )
      const user1ArticleIds = user1Articles.map(a => a.$id)

      // Level 2: Check if user2 commented on user1's articles
      for (const articleId of user1ArticleIds.slice(0, 5)) {
        const connection = await graphComments.find(
          {
            'article.Article': { $eq: articleId },
            'author.Author': { $eq: user2 },
          },
          { limit: 1 }
        )
        if (connection.length > 0) break
      }
    })

    bench('breadth-first: expand from entity', async () => {
      const startUserId = randomElement(graphUserIds)

      // Level 0: Start user
      const startUser = await graphUsers.get(startUserId.split('/')[1])

      // Level 1: User's articles
      const articles = await graphArticles.find(
        { 'author.Author': { $eq: startUserId } },
        { limit: 5 }
      )

      // Level 2: Comments on those articles
      for (const article of articles) {
        await graphComments.find(
          { 'article.Article': { $eq: article.$id } },
          { limit: 5 }
        )
      }
    })
  })

  // ===========================================================================
  // Memory Tracking Utilities
  // ===========================================================================

  describe('Memory Usage Tracking', () => {
    bench('memory baseline - 1000 entities', async () => {
      const memBefore = getMemoryUsage()
      const collection = new Collection<Article>(`mem-test-${Date.now()}`)

      for (let i = 0; i < 1000; i++) {
        await collection.create(generateArticle(i))
      }

      const memAfter = getMemoryUsage()
      if (memBefore && memAfter) {
        const heapGrowth = memAfter.heapUsed - memBefore.heapUsed
        // console.log(`Memory growth for 1000 entities: ${formatBytes(heapGrowth)}`)
      }
    }, { iterations: 3 })

    bench('memory with large content - 100 entities x 10KB', async () => {
      const memBefore = getMemoryUsage()
      const collection = new Collection<Article>(`mem-large-${Date.now()}`)

      for (let i = 0; i < 100; i++) {
        const article = generateArticle(i)
        article.content = randomString(10000)
        await collection.create(article)
      }

      const memAfter = getMemoryUsage()
      if (memBefore && memAfter) {
        const heapGrowth = memAfter.heapUsed - memBefore.heapUsed
        // console.log(`Memory growth for 100 large entities: ${formatBytes(heapGrowth)}`)
      }
    }, { iterations: 3 })
  })
})
