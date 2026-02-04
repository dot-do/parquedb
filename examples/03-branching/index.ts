/**
 * ParqueDB Branching Example
 *
 * Shows git-style version control for your database:
 * - Create branches
 * - Switch between branches
 * - Merge changes
 *
 * CLI equivalent commands shown in comments.
 * Run: npx tsx examples/03-branching/index.ts
 */
import { DB, FsBackend, BranchManager } from '../../src'
import { rm } from 'fs/promises'

async function main() {
  // Clean start
  await rm('.db', { recursive: true, force: true })

  const storage = new FsBackend('.db')
  const db = DB({
    User: {
      email: 'string!#',
      name: 'string',
      role: 'string'
    }
  }, { storage })

  const branches = new BranchManager({ storage })

  console.log('Database initialized')

  // Create initial data on main branch
  const alice = await db.User.create({
    $type: 'User',
    name: 'Alice',
    email: 'alice@example.com',
    role: 'admin'
  })
  console.log('Created Alice on main branch')

  // List branches
  // CLI: parquedb branch
  const allBranches = await branches.list()
  console.log('\nBranches:', allBranches.map(b => b.name).join(', ') || '(none yet)')

  // Create a feature branch
  // CLI: parquedb branch feature/new-users
  try {
    await branches.create('feature/new-users')
    console.log('Created branch: feature/new-users')
  } catch (e) {
    // Branch creation requires an initial commit
    console.log('Note: Branching requires initial commit (run sync first)')
  }

  // Check current branch
  // CLI: parquedb branch --show-current
  const current = await branches.current()
  console.log('Current branch:', current || 'detached HEAD')

  // Demonstrate branch workflow concept
  console.log('\n--- Branch Workflow Concept ---')
  console.log(`
  # Create and switch to feature branch
  parquedb branch feature/new-users
  parquedb checkout feature/new-users

  # Make changes
  parquedb create users --data '{"name": "Feature User"}'

  # Commit changes
  parquedb commit -m "Add feature user"

  # Switch back to main
  parquedb checkout main

  # Merge feature branch
  parquedb merge feature/new-users --strategy newest

  # Clean up
  parquedb branch -d feature/new-users
  `)

  // Show data on current state
  const users = await db.User.find()
  console.log(`Users on current branch: ${users.items.map(u => u.name).join(', ')}`)

  // Check for uncommitted changes
  const status = await branches.hasUncommittedChanges()
  console.log(`\nUncommitted changes: ${status.hasChanges}`)
  if (status.hasChanges) {
    console.log(`  ${status.summary}`)
  }

  db.dispose()
  console.log('\nDone!')
}

main().catch(console.error)
