# Payload CMS with ParqueDB + OAuth.do Authentication

This example demonstrates how to use Payload CMS with ParqueDB as the database backend and **oauth.do** for authentication instead of Payload's built-in password-based auth.

Uses **OpenNext** (`@opennextjs/cloudflare`) for seamless deployment to Cloudflare Workers.

## Features

- **External OAuth Authentication**: Uses WorkOS/oauth.do for authentication instead of passwords
- **JWT Token Verification**: Verifies tokens against JWKS endpoint
- **Role-Based Access Control**: Admin, editor, and viewer roles
- **Next.js 15**: Built on Payload 3 with the latest Next.js
- **Local Development**: Use filesystem storage for quick local development
- **Cloud Deployment**: Deploy to Cloudflare Workers via OpenNext
- **E2E Tests**: Comprehensive Playwright tests with local JWKS server

## Quick Start

### Prerequisites

1. Node.js 18+
2. pnpm (recommended) or npm
3. A WorkOS account (for production) or use the test JWKS server

### Local Development

1. Install dependencies:

```bash
pnpm install
```

2. Copy environment file and configure:

```bash
cp .env.example .env.local
```

Edit `.env.local` with your WorkOS JWKS URI:

```bash
WORKOS_JWKS_URI=https://api.workos.com/sso/jwks/client_XXX
PAYLOAD_SECRET=your-secret-key-change-in-production
```

3. Start the development server:

```bash
pnpm dev
```

4. Open [http://localhost:3000/admin](http://localhost:3000/admin)

### Authentication Flow

Unlike the standard Payload example, this uses **external OAuth authentication**:

1. User authenticates with your OAuth provider (WorkOS)
2. Provider issues a JWT token containing user claims and roles
3. Token is sent to Payload via:
   - `Authorization: Bearer <token>` header, or
   - `auth` cookie
4. Payload verifies the token against the provider's JWKS endpoint
5. User's roles determine their access level in the admin panel

### Role-Based Access

The `oauthUsers()` function configures role-based access:

| Role | Description | Admin Access |
|------|-------------|--------------|
| `admin` | Full access to all collections and settings | ✅ Full |
| `editor` | Can create and edit content | ✅ Limited |
| `viewer` | Read-only, no admin panel access | ❌ None |

Configure roles in `payload.config.ts`:

```typescript
oauthUsers({
  jwksUri: process.env.WORKOS_JWKS_URI!,
  adminRoles: ['admin'],      // Roles with full admin access
  editorRoles: ['editor'],    // Roles with editor access
})
```

## Running Tests

This example includes comprehensive E2E tests that use a **local JWKS server** with pre-generated test keys. No external OAuth service needed for testing!

### Test Architecture

```
tests/
├── test-utils/
│   ├── jwt-server.ts     # Local JWKS server serving test keys
│   └── test-tokens.ts    # JWT generation utilities
├── e2e/
│   ├── oauth-auth.e2e.spec.ts  # API authentication tests
│   └── admin.e2e.spec.ts       # Admin panel tests
├── global-setup.ts       # Starts JWKS server, generates tokens
└── global-teardown.ts    # Cleanup
```

### Running Tests

```bash
# Run all tests
pnpm test

# Run with UI
pnpm test:ui

# Run in headed mode (see browser)
pnpm test:headed
```

### How Tests Work

1. **global-setup.ts** starts a local JWKS server on port 3456
2. RSA key pair is generated at test startup
3. Test JWTs are signed with the private key
4. App is configured to verify tokens against `http://localhost:3456/.well-known/jwks.json`
5. Tests use pre-generated tokens for different roles (admin, editor, viewer)

This provides **cryptographic verification** without external dependencies.

## Project Structure

```
examples/payload-oauth/
├── src/
│   ├── app/                 # Next.js App Router
│   │   ├── (payload)/       # Payload admin routes
│   │   │   ├── admin/       # Admin panel
│   │   │   └── api/         # REST & GraphQL APIs
│   │   ├── layout.tsx       # Root layout
│   │   └── page.tsx         # Home page
│   ├── collections/         # Payload collection definitions
│   │   ├── Posts.ts
│   │   ├── Categories.ts
│   │   ├── Media.ts
│   │   └── index.ts         # Note: No Users.ts - provided by oauthUsers()
│   ├── globals/
│   │   ├── SiteSettings.ts
│   │   └── index.ts
│   └── payload.config.ts    # Main config with oauthUsers()
├── tests/
│   ├── test-utils/          # JWT server & token generation
│   ├── e2e/                 # Playwright E2E tests
│   ├── global-setup.ts
│   └── global-teardown.ts
├── package.json
├── tsconfig.json
├── next.config.mjs
├── playwright.config.ts
├── wrangler.toml
└── README.md
```

## Configuration

### Database Adapter

The ParqueDB adapter is configured in `payload.config.ts`:

```typescript
import { parquedbAdapter, oauthUsers } from 'parquedb/payload'
import { FsBackend } from 'parquedb'

export default buildConfig({
  db: parquedbAdapter({
    storage: new FsBackend('./data'),
  }),
  collections: [
    oauthUsers({
      jwksUri: process.env.WORKOS_JWKS_URI!,
      adminRoles: ['admin'],
      editorRoles: ['editor'],
    }),
    Posts,
    Categories,
    Media,
  ],
})
```

### OAuth Configuration Options

```typescript
oauthUsers({
  // Required: JWKS endpoint for token verification
  jwksUri: 'https://api.workos.com/sso/jwks/client_XXX',

  // Optional: Client ID for audience claim verification
  clientId: 'your-client-id',

  // Optional: Cookie name (default: 'auth')
  cookieName: 'auth',

  // Optional: Roles that grant admin access (default: ['admin'])
  adminRoles: ['admin', 'super-admin'],

  // Optional: Roles that grant editor access (default: ['editor'])
  editorRoles: ['editor', 'content-manager'],

  // Optional: Allow all authenticated users (default: false)
  allowAllAuthenticated: false,

  // Optional: Custom access check function
  canAccessAdmin: async (user) => {
    return user.roles?.includes('content-admin') ?? false
  },

  // Optional: Clock tolerance for JWT verification in seconds (default: 60)
  clockTolerance: 60,
})
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `WORKOS_JWKS_URI` | Yes | WorkOS JWKS endpoint URL |
| `PAYLOAD_SECRET` | Yes | Secret for Payload (not used for OAuth, but required) |
| `OAUTH_CLIENT_ID` | No | Client ID for audience verification |
| `OAUTH_COOKIE_NAME` | No | Cookie name (default: 'auth') |
| `OAUTH_ADMIN_ROLES` | No | Comma-separated admin roles |
| `OAUTH_EDITOR_ROLES` | No | Comma-separated editor roles |

## Deployment to Cloudflare Workers

### Prerequisites

1. Create a Cloudflare account
2. Install Wrangler CLI: `npm install -g wrangler`
3. Login to Wrangler: `wrangler login`

### Setup R2 Bucket

```bash
wrangler r2 bucket create payload-oauth-data
wrangler r2 bucket create payload-oauth-media
```

### Set Secrets

```bash
wrangler secret put PAYLOAD_SECRET
wrangler secret put WORKOS_JWKS_URI
```

### Build and Deploy

```bash
# Build for Workers
pnpm build:workers

# Preview locally
pnpm preview

# Deploy to production
pnpm deploy
```

## Differences from Standard Payload Example

| Feature | Standard Example | OAuth Example |
|---------|-----------------|---------------|
| Users Collection | Custom `Users.ts` with password auth | `oauthUsers()` - no passwords |
| Authentication | Email/password login form | External OAuth provider |
| User Creation | Via admin panel | Via OAuth provider only |
| Token Type | Payload session tokens | External JWTs |
| JWKS Verification | Not needed | Required |

## API Usage

```bash
# Authenticated request with Bearer token
curl http://localhost:3000/api/posts \
  -H "Authorization: Bearer $TOKEN"

# Authenticated request with cookie
curl http://localhost:3000/api/posts \
  -H "Cookie: auth=$TOKEN"

# Get current user
curl http://localhost:3000/api/users/me \
  -H "Authorization: Bearer $TOKEN"
```

## Troubleshooting

### Token Verification Fails

1. Ensure `WORKOS_JWKS_URI` is correct
2. Check token hasn't expired
3. Verify audience claim matches `OAUTH_CLIENT_ID` (if set)
4. Check clock sync between servers (tolerance is 60s by default)

### Admin Panel Shows Login

If you see a login page instead of dashboard:

1. Verify your token has required roles (`admin` or `editor`)
2. Check the `auth` cookie is being sent
3. Ensure JWKS server is reachable

### Tests Fail to Start

1. Check port 3456 is available (JWKS server)
2. Check port 3000 is available (app)
3. Run `pnpm install` to ensure dependencies

## License

MIT
