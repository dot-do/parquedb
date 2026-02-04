import Link from 'next/link'

export default function Home() {
  return (
    <main style={{ padding: '2rem', maxWidth: '800px', margin: '0 auto' }}>
      <h1>Payload + ParqueDB + OAuth Example</h1>

      <p style={{ marginTop: '1rem', lineHeight: 1.6 }}>
        This example demonstrates using{' '}
        <a href="https://payloadcms.com" target="_blank" rel="noopener">
          Payload CMS
        </a>{' '}
        with{' '}
        <a href="https://github.com/dot-do/parquedb" target="_blank" rel="noopener">
          ParqueDB
        </a>{' '}
        as the database backend and{' '}
        <a href="https://oauth.do" target="_blank" rel="noopener">
          oauth.do
        </a>{' '}
        for authentication.
      </p>

      <div style={{ marginTop: '2rem', padding: '1rem', backgroundColor: '#f5f5f5', borderRadius: '8px' }}>
        <h2>OAuth Authentication</h2>
        <p style={{ marginTop: '0.5rem', lineHeight: 1.6 }}>
          This example uses external OAuth authentication instead of Payload&apos;s
          built-in password auth. Users authenticate via their OAuth provider
          (WorkOS) and receive a JWT token that grants access to the admin panel.
        </p>
      </div>

      <div style={{ marginTop: '2rem' }}>
        <h2>Getting Started</h2>
        <ul style={{ marginTop: '1rem', lineHeight: 2 }}>
          <li>
            <Link href="/admin" style={{ fontWeight: 'bold' }}>
              Go to Admin Panel →
            </Link>
            <span style={{ color: '#666', marginLeft: '0.5rem' }}>
              (requires OAuth token)
            </span>
          </li>
          <li>
            <Link href="/api/posts">View Posts API →</Link>
          </li>
          <li>
            <Link href="/api/categories">View Categories API →</Link>
          </li>
        </ul>
      </div>

      <div style={{ marginTop: '2rem' }}>
        <h2>Authentication Flow</h2>
        <ol style={{ marginTop: '1rem', lineHeight: 2, paddingLeft: '1.5rem' }}>
          <li>User authenticates with OAuth provider (WorkOS)</li>
          <li>Provider issues JWT token with user claims and roles</li>
          <li>Token is sent to Payload via Authorization header or cookie</li>
          <li>Payload verifies token against provider&apos;s JWKS endpoint</li>
          <li>User&apos;s roles determine admin panel access level</li>
        </ol>
      </div>

      <div style={{ marginTop: '2rem' }}>
        <h2>Role-Based Access</h2>
        <ul style={{ marginTop: '1rem', lineHeight: 1.8, paddingLeft: '1.5rem' }}>
          <li><strong>admin</strong> - Full access to all collections and settings</li>
          <li><strong>editor</strong> - Can create and edit content</li>
          <li><strong>user</strong> - Read-only access (no admin panel)</li>
        </ul>
      </div>

      <div style={{ marginTop: '2rem' }}>
        <h2>Data Storage</h2>
        <p style={{ marginTop: '1rem', lineHeight: 1.6 }}>
          Data is stored in Parquet files in the <code>data/</code> directory.
          Each collection has its own namespace with files like{' '}
          <code>data/posts/data.parquet</code>.
        </p>
      </div>
    </main>
  )
}
