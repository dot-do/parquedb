import { db } from '../lib/db'

await db.User.create({ email: 'alice@example.com', name: 'Alice', role: 'admin' })
await db.User.create({ email: 'bob@example.com', name: 'Bob', role: 'author' })

await db.Post.create({ slug: 'hello', title: 'Hello World', content: 'Welcome!', status: 'published', author: 'alice@example.com' })
await db.Post.create({ slug: 'draft', title: 'Draft', content: 'WIP', status: 'draft', author: 'bob@example.com' })

console.log('Done!')
