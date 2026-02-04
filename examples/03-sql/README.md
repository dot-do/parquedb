# SQL Integration

ParqueDB supports SQL queries on typed collections via a tagged template literal.

## Usage

```typescript
const { sql } = db

// SELECT with parameters
const admins = await sql`SELECT * FROM user WHERE role = ${'admin'}`

// Comparison operators
const adults = await sql`SELECT name, age FROM user WHERE age >= ${30}`

// ORDER BY and LIMIT
const topPosts = await sql`
  SELECT title, views FROM post
  WHERE status = ${'published'}
  ORDER BY views DESC
  LIMIT ${5}
`

// Aggregates
const stats = await sql`
  SELECT COUNT(*) as total, AVG(views) as avg_views
  FROM post
  WHERE status = ${'published'}
`
```

## Mutations

SQL also supports INSERT, UPDATE, and DELETE:

```typescript
// INSERT
await sql`INSERT INTO user (email, name) VALUES (${'alice@example.com'}, ${'Alice'})`

// UPDATE
await sql`UPDATE post SET views = views + ${10} WHERE slug = ${'hello-world'}`

// DELETE
await sql`DELETE FROM post WHERE status = ${'draft'}`
```

## Results

All queries return `{ rows: T[] }`:

```typescript
const result = await sql`SELECT name FROM user`
console.log(result.rows)  // [{ name: 'Alice' }, { name: 'Bob' }]
```

## Run

```bash
npx tsx examples/03-sql/index.ts
```
