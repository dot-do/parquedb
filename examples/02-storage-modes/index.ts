/**
 * ParqueDB Storage Modes
 *
 * Two storage formats:
 * 1. Typed Mode - native columns for schema fields
 * 2. Flexible Mode - variant-shredded ($data + $index_*)
 *
 * Run: npx tsx examples/02-storage-modes/index.ts
 */
import { DB, FsBackend } from '../../src/index.js'

async function main() {
  const db = DB({
    // Typed Mode: native columns for each field
    // File: data/user.parquet with columns: $id, email, name, role, ...
    User: {
      $id: 'email',
      $options: {
        includeDataVariant: true,  // Also include $data for fast full-row reads
      },
      email: 'string!#',
      name: 'string',
      role: 'string',
    },

    // Typed Mode optimized for append-only (no $data column)
    // File: data/auditlog.parquet with columns only (no JSON blob)
    AuditLog: {
      $options: {
        includeDataVariant: false,  // Skip $data - columnar access only
      },
      action: 'string!',
      actor: 'string!',
      timestamp: 'datetime!',
      details: 'json',
    },

    // Flexible Mode: variant-shredded storage
    // File: data/event/data.parquet with $data blob + $index_* columns
    Event: 'flexible',
  }, {
    storage: new FsBackend('.db')
  })

  // Typed collection - native columns enable efficient queries
  await db.User.create({
    email: 'alice@example.com',
    name: 'Alice',
    role: 'admin'
  })

  // Predicate pushdown works on native columns
  const admins = await db.User.find({ role: 'admin' })
  console.log('Admins:', admins.total)

  // Audit log - no $data overhead for append-only workloads
  await db.AuditLog.create({
    action: 'user.login',
    actor: 'alice@example.com',
    timestamp: new Date(),
    details: { ip: '127.0.0.1' }
  })

  // Flexible collection - any shape, no schema required
  await db.Event.create({
    type: 'page_view',
    url: '/home',
    visitor: 'anon-123',
    metadata: { referrer: 'google.com', device: 'mobile' }
  })

  // Flexible mode indexes hot fields automatically
  const events = await db.Event.find({ type: 'page_view' })
  console.log('Page views:', events.total)

  db.dispose()
  console.log('Done!')
  console.log(`
Storage layout:
  .db/data/user.parquet      - Typed: $id, email, name, role, $data
  .db/data/auditlog.parquet  - Typed: $id, action, actor, timestamp (no $data)
  .db/data/event/data.parquet - Flexible: $id, $data, $index_type, $index_url
  .db/rels/...               - Relationship indexes
  .db/events/...             - Event log (CDC)
  `)
}

main().catch(console.error)
