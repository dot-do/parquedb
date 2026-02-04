/**
 * ParqueDB SQL Integration
 *
 * SQL queries work on typed collections with native columns.
 * For flexible collections, query $data or $index_* columns.
 *
 * Run: npx tsx examples/03-sql/index.ts
 */
import { DB, FsBackend } from '../../src/index.js'

async function main() {
  const db = DB({
    User: {
      $id: 'email',
      email: 'string!#',
      name: 'string',
      age: 'int',
      role: 'string',
    },
    Post: {
      $id: 'slug',
      slug: 'string!#',
      title: 'string!',
      status: 'string',
      views: 'int',
      author: '-> User'
    }
  }, {
    storage: new FsBackend('.db')
  })

  // Seed data
  await db.User.create({ email: 'alice@example.com', name: 'Alice', age: 30, role: 'admin' })
  await db.User.create({ email: 'bob@example.com', name: 'Bob', age: 25, role: 'author' })
  await db.User.create({ email: 'charlie@example.com', name: 'Charlie', age: 35, role: 'author' })

  await db.Post.create({ slug: 'hello-world', title: 'Hello World', status: 'published', views: 100, author: 'alice@example.com' })
  await db.Post.create({ slug: 'sql-guide', title: 'SQL Guide', status: 'published', views: 250, author: 'bob@example.com' })
  await db.Post.create({ slug: 'draft-post', title: 'Draft Post', status: 'draft', views: 0, author: 'alice@example.com' })

  // SQL queries via tagged template
  const { sql } = db

  // SELECT with WHERE
  const activeUsers = await sql`SELECT * FROM user WHERE role = ${'admin'}`
  console.log('Admins:', activeUsers.rows.length)

  // SELECT with comparison operators
  const adults = await sql`SELECT name, age FROM user WHERE age >= ${30}`
  console.log('Users 30+:', adults.rows.map(u => u.name))

  // SELECT with ORDER BY and LIMIT
  const topPosts = await sql`
    SELECT title, views FROM post
    WHERE status = ${'published'}
    ORDER BY views DESC
    LIMIT ${5}
  `
  console.log('Top posts:', topPosts.rows.map(p => `${p.title} (${p.views} views)`))

  // Aggregate queries
  const stats = await sql`
    SELECT
      COUNT(*) as total,
      SUM(views) as total_views,
      AVG(views) as avg_views
    FROM post
    WHERE status = ${'published'}
  `
  console.log('Stats:', stats.rows[0])

  // INSERT via SQL
  await sql`
    INSERT INTO user (email, name, age, role)
    VALUES (${'dave@example.com'}, ${'Dave'}, ${28}, ${'reader'})
  `

  // UPDATE via SQL
  await sql`
    UPDATE post
    SET views = views + ${10}
    WHERE slug = ${'hello-world'}
  `

  // DELETE via SQL
  await sql`DELETE FROM post WHERE status = ${'draft'}`

  // Verify changes
  const allUsers = await sql`SELECT COUNT(*) as count FROM user`
  const allPosts = await sql`SELECT COUNT(*) as count FROM post`
  console.log(`Final: ${allUsers.rows[0].count} users, ${allPosts.rows[0].count} posts`)

  db.dispose()
  console.log('Done!')
}

main().catch(console.error)
