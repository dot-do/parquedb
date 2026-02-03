# Hono Authentication Middleware

ParqueDB provides authentication middleware for [Hono](https://hono.dev/) that integrates with [oauth.do](https://oauth.do) for JWT verification. The middleware automatically populates user context and actor IDs for audit trails.

## Quick Start

```typescript
import { Hono } from 'hono'
import { auth, requireAuth, getUser } from 'parquedb/hono'
import { db } from 'parquedb'

const app = new Hono()

// Add auth middleware to all routes
app.use('*', auth({ jwksUri: env.JWKS_URI }))

// Public route - user may or may not be authenticated
app.get('/api/posts', async (c) => {
  const posts = await db.Posts.find({ published: true })
  return c.json(posts.items)
})

// Protected route - requires authentication
app.post('/api/posts', requireAuth(), async (c) => {
  const data = await c.req.json()
  const post = await db.Posts.create(
    { $type: 'Post', name: data.title, ...data },
    { actor: c.var.actor }  // → createdBy: "users/abc123"
  )
  return c.json(post)
})
```

## Installation

Hono is a peer dependency:

```bash
npm install hono
# or
pnpm add hono
```

## auth() Middleware

The main authentication middleware. Verifies JWT tokens and populates context variables.

```typescript
import { auth } from 'parquedb/hono'

app.use('*', auth({
  jwksUri: 'https://api.workos.com/sso/jwks/client_xxx',
  actorNamespace: 'users',  // Optional, default: 'users'
}))
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `jwksUri` | `string` | required | WorkOS JWKS endpoint for JWT verification |
| `actorNamespace` | `string` | `'users'` | Namespace prefix for actor IDs |
| `extractToken` | `(c: Context) => string \| null` | Bearer token | Custom token extractor |

### Context Variables

After `auth()` runs, these are available on `c.var`:

| Variable | Type | Description |
|----------|------|-------------|
| `c.var.user` | `AuthUser \| null` | User object from JWT |
| `c.var.actor` | `EntityId \| null` | Actor ID like `"users/abc123"` |
| `c.var.token` | `string \| null` | Raw JWT token |

### AuthUser Type

```typescript
interface AuthUser {
  id: string
  email?: string
  firstName?: string
  lastName?: string
  profilePictureUrl?: string
  organizationId?: string
  roles?: string[]
  permissions?: string[]
}
```

## requireAuth() Middleware

Enforces authentication. Returns 401 if user is not authenticated.

```typescript
import { requireAuth } from 'parquedb/hono'

// Require any authenticated user
app.use('/api/*', requireAuth())

// Require specific role
app.use('/admin/*', requireAuth({ roles: ['admin'] }))

// Custom error message
app.post('/api/posts', requireAuth({
  message: 'Please log in to create posts'
}))
```

### Options

| Option | Type | Description |
|--------|------|-------------|
| `roles` | `string[]` | Required roles (user must have at least one) |
| `message` | `string` | Custom 401 error message |

### Response

If authentication fails:
```json
{ "error": "Authentication required" }
```

If role check fails (403):
```json
{ "error": "Required role: admin or moderator" }
```

## Helper Functions

### getUser()

Get the authenticated user from context:

```typescript
import { getUser } from 'parquedb/hono'

app.get('/api/me', async (c) => {
  const user = getUser(c)
  if (!user) {
    return c.json({ error: 'Not authenticated' }, 401)
  }
  return c.json({
    id: user.id,
    email: user.email,
    roles: user.roles,
  })
})
```

### assertAuth()

Throws if not authenticated (for use in try/catch):

```typescript
import { assertAuth } from 'parquedb/hono'

app.post('/api/posts', async (c) => {
  try {
    const user = assertAuth(c)  // Throws if not authenticated
    // user is guaranteed to be non-null here
  } catch (error) {
    return c.json({ error: error.message }, 401)
  }
})
```

### assertRole()

Throws if user doesn't have required role:

```typescript
import { assertRole } from 'parquedb/hono'

app.delete('/api/posts/:id', async (c) => {
  try {
    const user = assertRole(c, 'admin')  // Throws if not admin
    await db.Posts.delete(c.req.param('id'), { actor: c.var.actor })
  } catch (error) {
    return c.json({ error: error.message }, 403)
  }
})
```

## Actor Flow to Audit Fields

The actor from authentication automatically flows to ParqueDB's audit fields:

```typescript
app.post('/api/posts', requireAuth(), async (c) => {
  // c.var.actor = "users/abc123" (from authenticated user)

  const post = await db.Posts.create(
    { $type: 'Post', name: 'My Post', title: 'Hello World' },
    { actor: c.var.actor }
  )

  // post.createdBy = "users/abc123"
  // post.updatedBy = "users/abc123"
  // post.createdAt = Date (auto-set)
  // post.updatedAt = Date (auto-set)

  return c.json(post)
})

app.patch('/api/posts/:id', requireAuth(), async (c) => {
  const data = await c.req.json()

  await db.Posts.update(
    c.req.param('id'),
    { $set: data },
    { actor: c.var.actor }  // → updatedBy: "users/abc123"
  )
})

app.delete('/api/posts/:id', requireAuth(), async (c) => {
  await db.Posts.delete(
    c.req.param('id'),
    { actor: c.var.actor }  // → deletedBy: "users/abc123"
  )
})
```

## Complete Example

```typescript
import { Hono } from 'hono'
import { auth, requireAuth, getUser } from 'parquedb/hono'
import { db } from 'parquedb'

type Env = {
  JWKS_URI: string
  R2_BUCKET: R2Bucket
}

const app = new Hono<{ Bindings: Env }>()

// Auth middleware on all routes
app.use('*', async (c, next) => {
  const authMiddleware = auth({ jwksUri: c.env.JWKS_URI })
  return authMiddleware(c, next)
})

// Public: list published posts
app.get('/api/posts', async (c) => {
  const posts = await db.Posts.find({ published: true })
  return c.json(posts.items)
})

// Public: get current user
app.get('/api/me', async (c) => {
  const user = getUser(c)
  if (!user) {
    return c.json({ authenticated: false })
  }
  return c.json({ authenticated: true, user })
})

// Protected: create post
app.post('/api/posts', requireAuth(), async (c) => {
  const data = await c.req.json()
  const post = await db.Posts.create(
    { $type: 'Post', name: data.title, ...data },
    { actor: c.var.actor }
  )
  return c.json(post, 201)
})

// Protected: update own post
app.patch('/api/posts/:id', requireAuth(), async (c) => {
  const id = c.req.param('id')
  const data = await c.req.json()

  // Check ownership
  const post = await db.Posts.get(id)
  if (post?.createdBy !== c.var.actor) {
    return c.json({ error: 'Not authorized' }, 403)
  }

  const updated = await db.Posts.update(
    id,
    { $set: data },
    { actor: c.var.actor }
  )
  return c.json(updated)
})

// Admin only: delete any post
app.delete('/api/posts/:id', requireAuth({ roles: ['admin'] }), async (c) => {
  await db.Posts.delete(c.req.param('id'), { actor: c.var.actor })
  return c.json({ deleted: true })
})

export default app
```

## Custom Token Extraction

By default, tokens are extracted from the `Authorization: Bearer <token>` header. You can customize this:

```typescript
app.use('*', auth({
  jwksUri: env.JWKS_URI,
  extractToken: (c) => {
    // Try Authorization header first
    const auth = c.req.header('Authorization')
    if (auth?.startsWith('Bearer ')) {
      return auth.slice(7)
    }
    // Fall back to cookie
    return c.req.cookie('auth_token') ?? null
  },
}))
```

## TypeScript Types

For full type safety with Hono's context:

```typescript
import type { AuthUser, AuthVariables } from 'parquedb/hono'

type Env = {
  Bindings: { JWKS_URI: string }
  Variables: AuthVariables
}

const app = new Hono<Env>()

app.get('/api/me', (c) => {
  // c.var.user is typed as AuthUser | null
  // c.var.actor is typed as EntityId | null
  const user = c.var.user
  return c.json(user)
})
```

## Error Handling

The `auth()` middleware never throws - it silently sets `user` and `actor` to `null` if verification fails. Use `requireAuth()` to enforce authentication.

For debugging, enable debug logging:

```typescript
// Set environment variable
PARQUEDB_DEBUG=true

// Or NODE_ENV
DEBUG=true
```

This logs verification failures to help diagnose issues.
