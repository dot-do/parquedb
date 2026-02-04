/**
 * ParqueDB Configuration
 *
 * This is the single source of truth for your schema.
 * The `db` import from 'parquedb' auto-detects this file.
 */
import { defineConfig, defineSchema } from 'parquedb/config'

export const schema = defineSchema({
  User: {
    $id: 'email',
    $name: 'name',
    email: 'string!#',
    name: 'string!',
    role: 'string',
    posts: '<- Post.author[]'
  },
  Post: {
    $id: 'slug',
    $name: 'title',
    slug: 'string!#',
    title: 'string!',
    content: 'text',
    status: 'string',
    publishedAt: 'datetime',
    author: '-> User',

    // Studio layout configuration
    $layout: {
      Content: [['title', 'slug'], 'content'],
      Settings: [['status', 'publishedAt'], 'author']
    },
    $sidebar: ['$id', 'status', 'createdAt'],
    $studio: {
      label: 'Blog Posts',
      status: { options: ['draft', 'published', 'archived'] }
    }
  }
})

export default defineConfig({
  schema,
  studio: {
    port: 3001
  }
})
