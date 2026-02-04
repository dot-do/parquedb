/**
 * REST API route - Alternative to Server Actions
 */
import { NextRequest, NextResponse } from 'next/server'
import { db } from 'parquedb'

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const status = searchParams.get('status') ?? 'published'
  const cursor = searchParams.get('cursor') ?? undefined

  const posts = await db.Post.find(
    { status },
    { limit: 10, cursor }
  )

  return NextResponse.json({
    posts: posts.map(p => ({
      slug: p.slug,
      title: p.title,
      author: p.author?.name
    })),
    total: posts.$total,
    next: posts.$next
  })
}

export async function POST(request: NextRequest) {
  const body = await request.json()

  const post = await db.Post.create({
    slug: body.slug,
    title: body.title,
    content: body.content,
    status: 'draft',
    author: body.author
  })

  return NextResponse.json(
    { id: post.$id, slug: post.slug },
    { status: 201 }
  )
}
