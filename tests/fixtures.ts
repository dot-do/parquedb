/**
 * Test Fixtures
 *
 * Pre-built test data and scenarios for consistent testing.
 * Use fixtures when you need predictable, reusable test data.
 */

import type { Entity, EntityId, Schema, CreateInput } from '../types'
import { createEntityId } from './factories'

// =============================================================================
// User Fixtures
// =============================================================================

export const USERS = {
  alice: {
    $id: 'users/alice' as EntityId,
    $type: 'User',
    name: 'Alice Johnson',
    email: 'alice@example.com',
    bio: 'Software engineer and open source enthusiast',
    createdAt: new Date('2024-01-01T00:00:00Z'),
    createdBy: 'system/seed' as EntityId,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: 'system/seed' as EntityId,
    version: 1,
  },
  bob: {
    $id: 'users/bob' as EntityId,
    $type: 'User',
    name: 'Bob Smith',
    email: 'bob@example.com',
    bio: 'Database administrator and Parquet fan',
    createdAt: new Date('2024-01-02T00:00:00Z'),
    createdBy: 'system/seed' as EntityId,
    updatedAt: new Date('2024-01-02T00:00:00Z'),
    updatedBy: 'system/seed' as EntityId,
    version: 1,
  },
  charlie: {
    $id: 'users/charlie' as EntityId,
    $type: 'User',
    name: 'Charlie Brown',
    email: 'charlie@example.com',
    bio: 'Frontend developer',
    createdAt: new Date('2024-01-03T00:00:00Z'),
    createdBy: 'system/seed' as EntityId,
    updatedAt: new Date('2024-01-03T00:00:00Z'),
    updatedBy: 'system/seed' as EntityId,
    version: 1,
  },
} as const

// =============================================================================
// Category Fixtures
// =============================================================================

export const CATEGORIES = {
  tech: {
    $id: 'categories/tech' as EntityId,
    $type: 'Category',
    name: 'Technology',
    slug: 'tech',
    description: 'Posts about technology and software',
    createdAt: new Date('2024-01-01T00:00:00Z'),
    createdBy: 'system/seed' as EntityId,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: 'system/seed' as EntityId,
    version: 1,
  },
  databases: {
    $id: 'categories/databases' as EntityId,
    $type: 'Category',
    name: 'Databases',
    slug: 'databases',
    description: 'Posts about database systems',
    createdAt: new Date('2024-01-01T00:00:00Z'),
    createdBy: 'system/seed' as EntityId,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: 'system/seed' as EntityId,
    version: 1,
  },
  tutorials: {
    $id: 'categories/tutorials' as EntityId,
    $type: 'Category',
    name: 'Tutorials',
    slug: 'tutorials',
    description: 'Step-by-step guides and tutorials',
    createdAt: new Date('2024-01-01T00:00:00Z'),
    createdBy: 'system/seed' as EntityId,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: 'system/seed' as EntityId,
    version: 1,
  },
} as const

// =============================================================================
// Post Fixtures
// =============================================================================

export const POSTS = {
  draft: {
    $id: 'posts/post-draft' as EntityId,
    $type: 'Post',
    name: 'Draft Post',
    title: 'My Draft Post',
    content: '# Draft\n\nThis post is still being written.',
    status: 'draft',
    publishedAt: null,
    author: { 'Alice Johnson': USERS.alice.$id },
    categories: {},
    createdAt: new Date('2024-01-10T00:00:00Z'),
    createdBy: USERS.alice.$id,
    updatedAt: new Date('2024-01-10T00:00:00Z'),
    updatedBy: USERS.alice.$id,
    version: 1,
  },
  published: {
    $id: 'posts/post-published' as EntityId,
    $type: 'Post',
    name: 'Published Post',
    title: 'Getting Started with ParqueDB',
    content: '# Getting Started\n\nParqueDB is a Parquet-based database...',
    status: 'published',
    publishedAt: new Date('2024-01-15T12:00:00Z'),
    author: { 'Alice Johnson': USERS.alice.$id },
    categories: { 'Technology': CATEGORIES.tech.$id, 'Databases': CATEGORIES.databases.$id },
    createdAt: new Date('2024-01-14T00:00:00Z'),
    createdBy: USERS.alice.$id,
    updatedAt: new Date('2024-01-15T12:00:00Z'),
    updatedBy: USERS.alice.$id,
    version: 2,
  },
  archived: {
    $id: 'posts/post-archived' as EntityId,
    $type: 'Post',
    name: 'Archived Post',
    title: 'Old Post (Archived)',
    content: '# Old Post\n\nThis post has been archived.',
    status: 'archived',
    publishedAt: new Date('2023-01-01T00:00:00Z'),
    author: { 'Bob Smith': USERS.bob.$id },
    categories: { 'Tutorials': CATEGORIES.tutorials.$id },
    createdAt: new Date('2022-12-01T00:00:00Z'),
    createdBy: USERS.bob.$id,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: USERS.bob.$id,
    deletedAt: new Date('2024-01-20T00:00:00Z'),
    deletedBy: USERS.bob.$id,
    version: 3,
  },
} as const

// =============================================================================
// Schema Fixtures
// =============================================================================

export const BLOG_SCHEMA: Schema = {
  User: {
    $type: 'schema:Person',
    $ns: 'users',
    name: 'string!',
    email: { type: 'email!', index: 'unique' },
    bio: 'text?',
    avatar: 'url?',
  },
  Post: {
    $type: 'schema:BlogPosting',
    $ns: 'posts',
    $shred: ['status', 'publishedAt'],
    name: 'string!',
    title: 'string!',
    content: 'markdown!',
    excerpt: 'text?',
    status: { type: 'string', default: 'draft', index: true },
    publishedAt: 'datetime?',
    author: '-> User.posts',
    categories: '-> Category.posts[]',
  },
  Category: {
    $type: 'schema:Category',
    $ns: 'categories',
    name: 'string!',
    slug: { type: 'string!', index: 'unique' },
    description: 'text?',
  },
  Comment: {
    $type: 'schema:Comment',
    $ns: 'comments',
    text: 'string!',
    post: '-> Post.comments',
    author: '-> User.comments',
  },
}

export const ECOMMERCE_SCHEMA: Schema = {
  Product: {
    $type: 'schema:Product',
    $ns: 'products',
    name: 'string!',
    sku: { type: 'string!', index: 'unique' },
    price: 'decimal(10,2)!',
    description: 'text?',
    inStock: { type: 'boolean', default: true },
    category: '-> Category.products',
  },
  Category: {
    $type: 'schema:Category',
    $ns: 'categories',
    name: 'string!',
    slug: { type: 'string!', index: 'unique' },
    parent: '~> Category.children',
  },
  Order: {
    $type: 'schema:Order',
    $ns: 'orders',
    orderNumber: { type: 'string!', index: 'unique' },
    status: { type: 'enum(pending,processing,shipped,delivered,cancelled)', default: 'pending' },
    customer: '-> Customer.orders',
    items: 'json!',
    total: 'decimal(10,2)!',
  },
  Customer: {
    $type: 'schema:Person',
    $ns: 'customers',
    name: 'string!',
    email: { type: 'email!', index: 'unique' },
    phone: 'string?',
  },
}

// =============================================================================
// Test Data Sets
// =============================================================================

/**
 * Get all user fixtures as an array
 */
export function getAllUsers(): Entity[] {
  return Object.values(USERS) as Entity[]
}

/**
 * Get all category fixtures as an array
 */
export function getAllCategories(): Entity[] {
  return Object.values(CATEGORIES) as Entity[]
}

/**
 * Get all post fixtures as an array
 */
export function getAllPosts(): Entity[] {
  return Object.values(POSTS) as Entity[]
}

/**
 * Get all fixtures for seeding a test database
 */
export function getAllFixtures(): { users: Entity[]; categories: Entity[]; posts: Entity[] } {
  return {
    users: getAllUsers(),
    categories: getAllCategories(),
    posts: getAllPosts(),
  }
}

// =============================================================================
// Create Input Fixtures
// =============================================================================

export const CREATE_INPUTS = {
  user: {
    $type: 'User',
    name: 'New User',
    email: 'newuser@example.com',
    bio: 'A new test user',
  } as CreateInput,

  post: {
    $type: 'Post',
    name: 'New Post',
    title: 'A New Blog Post',
    content: '# New Post\n\nThis is a new post.',
    status: 'draft',
  } as CreateInput,

  category: {
    $type: 'Category',
    name: 'New Category',
    slug: 'new-category',
    description: 'A new category for testing',
  } as CreateInput,
}

// =============================================================================
// Filter Fixtures
// =============================================================================

export const FILTERS = {
  publishedPosts: { status: 'published' },
  draftPosts: { status: 'draft' },
  archivedPosts: { status: 'archived' },
  notDeleted: { deletedAt: { $exists: false } },
  deleted: { deletedAt: { $exists: true } },
  byAlice: { 'author.Alice Johnson': USERS.alice.$id },
  recentPosts: { createdAt: { $gte: new Date('2024-01-01') } },
  techPosts: { 'categories.Technology': CATEGORIES.tech.$id },
  publishedOrDraft: { $or: [{ status: 'published' }, { status: 'draft' }] },
}

// =============================================================================
// Binary Data Fixtures
// =============================================================================

export const BINARY_DATA = {
  empty: new Uint8Array(0),
  hello: new TextEncoder().encode('Hello, World!'),
  json: new TextEncoder().encode('{"name":"test","value":42}'),
  parquet: new Uint8Array([0x50, 0x41, 0x52, 0x31]), // PAR1 magic bytes
  binary: new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]),
}
