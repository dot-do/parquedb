import { NextRequest, NextResponse } from 'next/server'
import { db } from '@/lib/db'

export async function GET() {
  const posts = await db.Post.find({ status: 'published' })
  return NextResponse.json({ posts, total: posts.$total })
}

export async function POST(request: NextRequest) {
  const body = await request.json()
  const post = await db.Post.create({ ...body, status: 'draft' })
  return NextResponse.json({ id: post.$id }, { status: 201 })
}
