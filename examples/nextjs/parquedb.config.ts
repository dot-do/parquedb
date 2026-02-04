import { defineConfig } from 'parquedb/config'

export default defineConfig({
  schema: {
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
      author: '-> User'
    }
  }
})
