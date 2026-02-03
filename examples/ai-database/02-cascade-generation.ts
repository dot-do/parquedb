/**
 * Cascade Generation Example with ParqueDB + ai-database
 *
 * This example demonstrates how to use ParqueDB as a backend for
 * cascading entity generation patterns typically used with ai-database:
 * - Forward relationships for hierarchical content generation
 * - Backward references for aggregation queries
 * - Multi-level entity creation
 * - Progress tracking during generation
 *
 * Note: This example simulates the cascade pattern manually since
 * ai-database's cascade feature requires AI integration. In production,
 * you would use ai-database's DB() function with cascade: true.
 *
 * Run with: npx tsx examples/ai-database/02-cascade-generation.ts
 */

import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { createParqueDBProvider, type DBProviderExtended } from '../../src/integrations/ai-database'

// =============================================================================
// Type Definitions
// =============================================================================

interface Blog {
  $id: string
  title: string
  description: string
}

interface Topic {
  $id: string
  name: string
  description: string
}

interface PostIdea {
  $id: string
  title: string
  hook: string
  targetAudience: string
}

interface BlogPost {
  $id: string
  title: string
  slug: string
  synopsis: string
  content: string
  status: string
}

// =============================================================================
// Progress Tracking
// =============================================================================

interface CascadeProgress {
  phase: 'initializing' | 'generating' | 'linking' | 'complete'
  depth: number
  currentType: string
  totalEntitiesCreated: number
}

function logProgress(progress: CascadeProgress) {
  const indent = '  '.repeat(progress.depth)
  console.log(`${indent}[${progress.phase}] ${progress.currentType} (${progress.totalEntitiesCreated} total)`)
}

// =============================================================================
// Cascade Generator (Simulated)
// =============================================================================

/**
 * Simulates cascade generation of a blog structure.
 * In production with ai-database, AI would generate this content.
 */
async function generateBlogCascade(
  provider: DBProviderExtended,
  blogTitle: string,
  onProgress?: (p: CascadeProgress) => void
): Promise<Blog> {
  let totalCreated = 0

  // Track progress
  const progress = (phase: CascadeProgress['phase'], depth: number, type: string) => {
    onProgress?.({ phase, depth, currentType: type, totalEntitiesCreated: totalCreated })
  }

  // Step 1: Create the blog
  progress('initializing', 0, 'Blog')

  const blog = await provider.create('Blog', undefined, {
    title: blogTitle,
    description: `A professional blog about ${blogTitle.toLowerCase()}`,
  }) as unknown as Blog
  totalCreated++

  progress('generating', 0, 'Blog')

  // Step 2: Generate topics for the blog
  const topicNames = [
    { name: 'Getting Started', description: 'Introductory content for beginners' },
    { name: 'Advanced Techniques', description: 'Deep dives into complex topics' },
    { name: 'Best Practices', description: 'Industry-standard patterns and recommendations' },
    { name: 'Case Studies', description: 'Real-world examples and implementations' },
  ]

  progress('generating', 1, 'Topic')

  const topics: Topic[] = []
  for (const topicData of topicNames) {
    const topic = await provider.create('Topic', undefined, topicData) as unknown as Topic
    totalCreated++

    // Link topic to blog
    await provider.relate('Topic', topic.$id, 'blog', 'Blog', blog.$id)
    topics.push(topic)

    progress('linking', 1, 'Topic')
  }

  // Step 3: Generate post ideas for each topic
  progress('generating', 2, 'PostIdea')

  const postIdeas: PostIdea[] = []
  for (const topic of topics) {
    const ideaTitles = generatePostIdeasForTopic(topic.name)

    for (const ideaData of ideaTitles) {
      const idea = await provider.create('PostIdea', undefined, ideaData) as unknown as PostIdea
      totalCreated++

      // Link idea to topic
      await provider.relate('PostIdea', idea.$id, 'topic', 'Topic', topic.$id)
      postIdeas.push(idea)

      progress('linking', 2, 'PostIdea')
    }
  }

  progress('complete', 0, 'Blog')

  return blog
}

/**
 * Generate post ideas for a topic (simulated AI generation)
 */
function generatePostIdeasForTopic(topicName: string): Array<{ title: string; hook: string; targetAudience: string }> {
  const templates: Record<string, Array<{ title: string; hook: string; targetAudience: string }>> = {
    'Getting Started': [
      {
        title: 'Your First Steps: A Complete Beginner\'s Guide',
        hook: 'Everything you need to know to get started today',
        targetAudience: 'Complete beginners with no prior experience',
      },
      {
        title: 'Setting Up Your Development Environment',
        hook: 'The tools and configuration you need for success',
        targetAudience: 'New developers looking to set up their workspace',
      },
    ],
    'Advanced Techniques': [
      {
        title: 'Mastering Performance Optimization',
        hook: 'Take your applications to the next level',
        targetAudience: 'Experienced developers seeking optimization strategies',
      },
      {
        title: 'Architecture Patterns for Scale',
        hook: 'Design systems that grow with your needs',
        targetAudience: 'Senior engineers and architects',
      },
    ],
    'Best Practices': [
      {
        title: 'Code Review Standards That Work',
        hook: 'Improve your team\'s code quality dramatically',
        targetAudience: 'Tech leads and team managers',
      },
      {
        title: 'Testing Strategies for Reliability',
        hook: 'Ship with confidence every time',
        targetAudience: 'QA engineers and developers',
      },
    ],
    'Case Studies': [
      {
        title: 'How Company X Scaled to Millions',
        hook: 'A real-world success story with lessons learned',
        targetAudience: 'CTOs and engineering managers',
      },
      {
        title: 'Migration Story: From Legacy to Modern',
        hook: 'Practical insights from a major refactoring project',
        targetAudience: 'Teams planning modernization efforts',
      },
    ],
  }

  return templates[topicName] || [
    {
      title: `Understanding ${topicName}`,
      hook: `A comprehensive look at ${topicName.toLowerCase()}`,
      targetAudience: 'General technical audience',
    },
  ]
}

/**
 * Convert a post idea to a full blog post
 */
async function generatePostFromIdea(
  provider: DBProviderExtended,
  idea: PostIdea,
  blogId: string
): Promise<BlogPost> {
  // Generate slug from title
  const slug = idea.title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')

  // Create the full post
  const post = await provider.create('BlogPost', undefined, {
    title: idea.title,
    slug,
    synopsis: idea.hook,
    content: `# ${idea.title}\n\n${idea.hook}\n\n## Introduction\n\nThis article is written for: ${idea.targetAudience}.\n\n## Main Content\n\n[Content would be AI-generated here...]\n\n## Conclusion\n\nThank you for reading!`,
    status: 'draft',
  }) as unknown as BlogPost

  // Link post to blog
  await provider.relate('BlogPost', post.$id, 'blog', 'Blog', blogId)

  return post
}

// =============================================================================
// Main Example
// =============================================================================

async function main() {
  console.log('=== Cascade Generation Example ===\n')

  // Initialize ParqueDB
  const parquedb = new ParqueDB({ storage: new MemoryBackend() })
  const provider = createParqueDBProvider(parquedb)

  console.log('--- Generating Blog Structure ---\n')

  // Track progress
  const progressLog: CascadeProgress[] = []

  // Generate the blog cascade
  const blog = await generateBlogCascade(provider, 'Tech Engineering Blog', (p) => {
    progressLog.push(p)
    logProgress(p)
  })

  console.log('\n--- Generated Blog ---\n')
  console.log(`Title: ${blog.title}`)
  console.log(`Description: ${blog.description}`)

  // Show topics
  console.log('\n--- Topics ---\n')
  const topics = await provider.list('Topic')
  for (const topic of topics) {
    console.log(`- ${topic.name}: ${topic.description}`)

    // Show post ideas for this topic
    const ideas = await provider.related('Topic', topic.$id as string, 'postIdeas')
    // Note: We need to query post ideas that link to this topic
    // For now, show all post ideas (in a real app, we'd use backward relationships)
  }

  // Show all post ideas
  console.log('\n--- Post Ideas ---\n')
  const allIdeas = await provider.list('PostIdea')
  console.log(`Total post ideas generated: ${allIdeas.length}`)

  for (const idea of allIdeas.slice(0, 5)) {
    console.log(`\n"${idea.title}"`)
    console.log(`  Hook: ${idea.hook}`)
    console.log(`  Target: ${idea.targetAudience}`)
  }

  if (allIdeas.length > 5) {
    console.log(`\n... and ${allIdeas.length - 5} more ideas`)
  }

  // Generate a full post from one of the ideas
  console.log('\n--- Generating Full Post ---\n')

  const firstIdea = allIdeas[0] as unknown as PostIdea
  console.log(`Converting idea to full post: "${firstIdea.title}"`)

  const post = await generatePostFromIdea(provider, firstIdea, blog.$id)

  console.log('\nGenerated Post:')
  console.log(`  Title: ${post.title}`)
  console.log(`  Slug: ${post.slug}`)
  console.log(`  Status: ${post.status}`)
  console.log(`  Synopsis: ${post.synopsis}`)
  console.log('\n  Content Preview:')
  console.log(`  ${post.content.substring(0, 200)}...`)

  // Statistics
  console.log('\n--- Generation Statistics ---\n')
  console.log(`Total progress events: ${progressLog.length}`)
  console.log(`Phases: ${[...new Set(progressLog.map(p => p.phase))].join(', ')}`)
  console.log(`Max depth: ${Math.max(...progressLog.map(p => p.depth))}`)

  const finalStats = {
    blogs: (await provider.list('Blog')).length,
    topics: topics.length,
    postIdeas: allIdeas.length,
    blogPosts: (await provider.list('BlogPost')).length,
  }

  console.log('\nEntities created:')
  console.log(`  Blogs: ${finalStats.blogs}`)
  console.log(`  Topics: ${finalStats.topics}`)
  console.log(`  Post Ideas: ${finalStats.postIdeas}`)
  console.log(`  Blog Posts: ${finalStats.blogPosts}`)
  console.log(`  Total: ${Object.values(finalStats).reduce((a, b) => a + b, 0)}`)

  // Cleanup
  parquedb.dispose()

  console.log('\n=== Cascade Generation Complete ===')
}

main().catch(console.error)
