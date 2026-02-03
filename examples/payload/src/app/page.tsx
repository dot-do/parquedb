import Link from 'next/link'

export default function Home() {
  return (
    <main style={{ padding: '2rem', maxWidth: '800px', margin: '0 auto' }}>
      <h1>Payload + ParqueDB Example</h1>

      <p style={{ marginTop: '1rem', lineHeight: 1.6 }}>
        This example demonstrates using{' '}
        <a href="https://payloadcms.com" target="_blank" rel="noopener">
          Payload CMS
        </a>{' '}
        with{' '}
        <a href="https://github.com/dot-do/parquedb" target="_blank" rel="noopener">
          ParqueDB
        </a>{' '}
        as the database backend.
      </p>

      <div style={{ marginTop: '2rem' }}>
        <h2>Getting Started</h2>
        <ul style={{ marginTop: '1rem', lineHeight: 2 }}>
          <li>
            <Link href="/admin" style={{ fontWeight: 'bold' }}>
              Go to Admin Panel →
            </Link>
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
        <h2>Features</h2>
        <ul style={{ marginTop: '1rem', lineHeight: 1.8, paddingLeft: '1.5rem' }}>
          <li>Parquet-based storage for efficient data handling</li>
          <li>Works locally with filesystem storage</li>
          <li>Deploy to Cloudflare Workers with R2 storage</li>
          <li>Full Payload CMS features (admin panel, API, auth)</li>
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
