# Testing Guide for ParqueDB

This guide covers testing best practices, TDD workflow, and benchmarking for ParqueDB contributors.

## Table of Contents

- [Mocks vs Real Storage Backends](#mocks-vs-real-storage-backends)
- [TDD Workflow](#tdd-workflow)
- [Benchmark Writing Guide](#benchmark-writing-guide)
- [Test Organization](#test-organization)
  - [Test Directory Structure](#test-directory-structure)
  - [When to Use Different Test Types](#when-to-use-different-test-types)
  - [Naming Conventions](#naming-conventions-for-test-files)
  - [Test Utilities](#test-utilities)
- [Running Tests](#running-tests)
  - [Test Coverage](#test-coverage)
  - [Troubleshooting](#troubleshooting)
- [Additional Resources](#additional-resources)

---

## Mocks vs Real Storage Backends

ParqueDB supports multiple storage backends. Choosing the right backend for your tests ensures fast, reliable test execution while maintaining proper coverage.

### When to Use MemoryBackend

Use `MemoryBackend` for **unit tests** and most **integration tests**:

```typescript
import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../src/storage/MemoryBackend'

describe('MyFeature', () => {
  let backend: MemoryBackend

  beforeEach(() => {
    backend = new MemoryBackend()
  })

  it('should perform operation', async () => {
    await backend.write('test.txt', new TextEncoder().encode('content'))
    const result = await backend.read('test.txt')
    expect(new TextDecoder().decode(result)).toBe('content')
  })
})
```

**Benefits of MemoryBackend:**
- Extremely fast (no I/O overhead)
- No cleanup required
- Isolated by default (each instance is independent)
- Works in all environments (Node.js, browser, Workers)

### When to Use Real Storage Backends

Use real backends (`FsBackend`, `R2Backend`, `S3Backend`) for:

- **Integration tests** that verify actual storage behavior
- **E2E tests** that test the complete system
- **Performance tests** where real I/O matters

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { FsBackend } from '../../src/storage/FsBackend'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rm, mkdir } from 'node:fs/promises'

describe('FsBackend Integration', () => {
  let backend: FsBackend
  let testDir: string

  beforeEach(async () => {
    testDir = join(tmpdir(), `parquedb-test-${Date.now()}`)
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
  })

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true })
  })

  it('should persist data to filesystem', async () => {
    // Test with real filesystem
  })
})
```

### Test Isolation and Cleanup

**For MemoryBackend:** Each `new MemoryBackend()` instance is isolated - no cleanup needed.

**For global state (Collection):** Use `clearGlobalStorage()` in `beforeEach`:

```typescript
import { Collection, clearGlobalStorage } from '../../src/StandaloneCollection'

beforeEach(() => {
  clearGlobalStorage()
})
```

**For filesystem-based tests:** Always clean up temp directories in `afterEach`:

```typescript
import { cleanupTempDir } from '../setup'

afterEach(async () => {
  await cleanupTempDir(testDir)
})
```

**Key isolation principles:**
- Create fresh backend instances in `beforeEach`
- Use unique namespaces/directories per test
- Clean up resources in `afterEach`
- Never share mutable state between tests

---

## TDD Workflow

ParqueDB follows **Test-Driven Development (TDD)** with the Red-Green-Refactor cycle.

### Red-Green-Refactor Cycle

1. **Red**: Write a failing test that defines desired behavior
2. **Green**: Write minimal code to make the test pass
3. **Refactor**: Improve code quality while keeping tests green

### Example: Writing a Failing Test First

**Step 1: Red Phase** - Write the test before the implementation:

```typescript
import { describe, it, expect } from 'vitest'
import { Collection } from '../../src/StandaloneCollection'

describe('Collection.findOne', () => {
  it('should return first matching entity', async () => {
    const posts = new Collection('posts')

    await posts.create({ $type: 'Post', name: 'First', status: 'draft' })
    await posts.create({ $type: 'Post', name: 'Second', status: 'published' })

    // This test will fail until findOne is implemented
    const result = await posts.findOne({ status: 'published' })

    expect(result).not.toBeNull()
    expect(result?.name).toBe('Second')
  })

  it('should return null when no match found', async () => {
    const posts = new Collection('posts')

    const result = await posts.findOne({ status: 'archived' })

    expect(result).toBeNull()
  })
})
```

**Step 2: Green Phase** - Implement just enough to pass:

```typescript
async findOne(filter: Filter): Promise<Entity | null> {
  const results = await this.find(filter, { limit: 1 })
  return results[0] ?? null
}
```

**Step 3: Refactor** - Improve without changing behavior:

```typescript
async findOne(filter: Filter): Promise<Entity | null> {
  const [first = null] = await this.find(filter, { limit: 1 })
  return first
}
```

### TDD Best Practices

- **One assertion per test** (when practical)
- **Descriptive test names** that explain the expected behavior
- **Test edge cases**: null inputs, empty arrays, boundary values
- **Don't test implementation details** - test behavior

---

## Benchmark Writing Guide

ParqueDB uses Vitest's built-in benchmarking via `vitest bench`.

### How to Write Performance Tests

Create benchmark files in `tests/benchmarks/` with the `.bench.ts` extension:

```typescript
import { describe, bench, beforeAll } from 'vitest'
import { Collection } from '../../src/StandaloneCollection'

describe('Query Benchmarks', () => {
  let posts: Collection

  beforeAll(async () => {
    posts = new Collection('bench-posts')
    // Seed test data
    for (let i = 0; i < 1000; i++) {
      await posts.create({
        $type: 'Post',
        name: `Post ${i}`,
        views: Math.floor(Math.random() * 100000),
      })
    }
  })

  bench('find all', async () => {
    await posts.find()
  })

  bench('find with filter', async () => {
    await posts.find({ views: { $gt: 50000 } })
  })

  bench('find with sort and limit', async () => {
    await posts.find({}, { sort: { views: -1 }, limit: 10 })
  })
})
```

### Performance Targets and Thresholds

ParqueDB has the following performance targets (p50/p99 in milliseconds):

| Operation | Target (p50) | Target (p99) |
|-----------|-------------|-------------|
| Get by ID | 5ms | 20ms |
| Find (indexed) | 20ms | 100ms |
| Find (scan) | 100ms | 500ms |
| Create | 10ms | 50ms |
| Update | 15ms | 75ms |
| Relationship traverse | 50ms | 200ms |

### Benchmark Code Examples

**Measuring operation time manually:**

```typescript
import { bench, expect } from 'vitest'

bench('create operation meets target', async () => {
  const start = performance.now()

  await collection.create({
    $type: 'Entity',
    name: 'Test',
  })

  const elapsed = performance.now() - start
  // Implicit: vitest bench tracks this automatically
})
```

**Using bench options for iterations:**

```typescript
bench('batch create 1000 entities', async () => {
  for (let i = 0; i < 1000; i++) {
    await collection.create({ $type: 'Item', name: `Item ${i}` })
  }
}, { iterations: 10 })
```

**Running benchmarks:**

```bash
pnpm bench                              # Run all benchmarks
pnpm bench tests/benchmarks/crud.bench.ts  # Run specific benchmark
```

---

## Test Organization

### Test Directory Structure

```
tests/
├── unit/                    # Fast, isolated tests
│   ├── filter.test.ts       # Filter evaluation
│   ├── Collection.test.ts   # Collection CRUD operations
│   ├── entity.test.ts       # Entity validation
│   ├── backends/            # Storage backend unit tests
│   ├── cli/                 # CLI command tests
│   ├── codegen/             # Code generation tests
│   └── ...
├── integration/             # Tests with real dependencies
│   ├── storage.test.ts      # FsBackend integration
│   ├── crud-workflow.test.ts
│   ├── backends/            # Backend integration tests
│   ├── sql/                 # SQL query integration
│   └── ...
├── e2e/                     # End-to-end workflows
│   ├── compaction-workflow.test.ts
│   ├── relationships.test.ts
│   ├── worker/              # Cloudflare Workers E2E tests
│   ├── sql/                 # SQL E2E tests
│   └── ...
├── benchmarks/              # Performance tests
│   ├── crud.bench.ts
│   ├── queries.bench.ts
│   ├── relationships.bench.ts
│   ├── vector-search.bench.ts
│   └── ...
├── __mocks__/               # Manual mocks for Vitest
├── helpers/                 # Shared test utilities
├── mocks/                   # Mock implementations
├── setup.ts                 # Global test setup
├── factories.ts             # Test data factories
├── fixtures.ts              # Static test data
└── matchers.ts              # Custom Vitest matchers
```

### When to Use Different Test Types

**Unit tests (`tests/unit/`):**
- Pure functions (filter evaluation, update operators)
- Classes with no external dependencies
- Utility functions
- Use `MemoryBackend` for storage tests

**Integration tests (`tests/integration/`):**
- Multiple components working together
- Real storage backend interactions
- Database operations with `FsBackend`
- API integrations

**E2E tests (`tests/e2e/`):**
- Complete user workflows
- Full system tests with real infrastructure
- Workers tests with Cloudflare bindings
- Time-travel and compaction scenarios

### Naming Conventions for Test Files

- **Unit tests**: `{module}.test.ts` (e.g., `filter.test.ts`)
- **Integration tests**: `{feature}-workflow.test.ts` or `{integration}.test.ts`
- **E2E tests**: `{workflow}.test.ts`
- **Benchmarks**: `{area}.bench.ts` (e.g., `crud.bench.ts`)
- **Workers tests**: `{feature}.workers.test.ts`
- **Browser tests**: `{feature}.browser.test.ts`

### Test Utilities

ParqueDB provides several test utilities to help write consistent, maintainable tests.

**Test Data Factories (`tests/factories.ts`):**

```typescript
import {
  createTestEntity,
  createPostInput,
  createUserInput,
  createEntityId,
  createAuditFields,
  generateTestDirName,
} from '../factories'

// Create a test entity with sensible defaults
const entity = createTestEntity({ name: 'My Test' })

// Create input for specific entity types
const post = createPostInput({ title: 'Test Post', status: 'published' })
const user = createUserInput({ email: 'test@example.com' })

// Generate unique test directory names for parallel test isolation
const testDir = generateTestDirName('my-feature')
```

**Custom Vitest Matchers (`tests/matchers.ts`):**

```typescript
import { expect } from 'vitest'

// Assert entity structure
expect(entity).toBeValidEntity()
expect(entity).toHaveAuditFields()
expect(entity.$id).toBeEntityId()

// Assert filter matching
expect(entity).toMatchFilter({ status: 'published' })

// Assert relationships
expect(entity).toHaveRelationship('author', /users\//)

// Assert Parquet file properties
expect(buffer).toBeValidParquetFile()
expect(buffer).toHaveRowGroups(3)
expect(buffer).toBeCompressedWith('SNAPPY')
```

**Static Test Fixtures (`tests/fixtures.ts`):**

Use fixtures for complex, reusable test data that doesn't change between tests.

**Environment Helpers (`tests/setup.ts`):**

```typescript
import {
  isNode,
  isBrowser,
  isWorkers,
  getEnvironment,
  shouldSkipInEnvironment,
  cleanupTempDir,
} from '../setup'

// Skip tests in specific environments
it.skipIf(shouldSkipInEnvironment('workers'))('uses fs module', () => {
  // This test won't run in Workers
})

// Clean up temp directories robustly
afterEach(async () => {
  await cleanupTempDir(testDir)
})
```

---

## Running Tests

### Basic Commands

```bash
# Run all tests
pnpm test

# Run tests in watch mode
pnpm test -- --watch

# Run specific test file
pnpm test tests/unit/filter.test.ts

# Run tests matching a pattern
pnpm test -- filter

# Run tests with coverage
pnpm test -- --coverage
```

### Running Specific Test Types

```bash
# Unit tests only
pnpm test:unit

# Integration tests only
pnpm test:integration

# E2E tests only
pnpm test:e2e

# Cloudflare Workers tests
pnpm test:e2e:workers

# Browser tests
pnpm test:browser
```

### Running Benchmarks

```bash
# Run all benchmarks
pnpm bench

# Run specific benchmark
pnpm bench:crud
pnpm bench:queries
pnpm bench:relationships
```

### Test Configuration

ParqueDB uses Vitest with the following key settings:

- **Pool**: `forks` for parallel test execution
- **File parallelism**: Enabled for performance
- **Setup file**: `tests/setup.ts` runs before all tests
- **Timeout**: 30 seconds default

See `vitest.config.ts` for full configuration details.

### Tips for Faster Test Runs

1. **Run specific tests** during development: `pnpm test -- filter`
2. **Use watch mode**: `pnpm test -- --watch`
3. **Prefer MemoryBackend** for unit tests
4. **Use `it.skip`** to temporarily skip slow tests
5. **Run full suite** before committing

### Test Coverage

ParqueDB maintains coverage thresholds to ensure code quality:

```bash
# Run tests with coverage report
pnpm test -- --coverage
```

Coverage thresholds (configured in `vitest.config.ts`):
- Lines: 80%
- Branches: 70%
- Functions: 80%
- Statements: 80%

Coverage reports are generated in the `coverage/` directory in multiple formats (text, HTML, JSON, LCOV).

### Troubleshooting

**Tests timing out:**
- Default timeout is 30 seconds
- Increase for specific tests: `it('slow test', async () => {...}, 60000)`
- Check for unresolved promises or infinite loops

**Test isolation failures:**
- Ensure `clearGlobalStorage()` is called in `beforeEach`
- Use unique namespaces per test file
- Check for shared mutable state

**Parallel test conflicts:**
- Use `generateTestDirName()` for unique temp directories
- Avoid hard-coded port numbers
- Use `maxConcurrency: 1` in test file if needed

**Workers test failures:**
- Ensure Miniflare is properly configured
- Check that bindings are available in test environment
- Use `vitest --project 'workers:*'` for Workers-specific tests

---

## Additional Resources

### ParqueDB Documentation

- [CLAUDE.md](./CLAUDE.md) - Project overview and development conventions
- [Architecture docs](./docs/architecture/) - Design decisions and technical details
- [API Documentation](./docs/api/) - Collection and Query API reference
- [Benchmarks Documentation](./docs/BENCHMARKS.md) - Performance analysis and results

### External Resources

- [Vitest Documentation](https://vitest.dev/) - Test framework documentation
- [Vitest Benchmarking](https://vitest.dev/guide/features.html#benchmarking) - Performance testing guide
- [vitest-pool-workers](https://developers.cloudflare.com/workers/testing/vitest-integration/) - Cloudflare Workers testing

### Example Test Files

For real-world examples, see these test files in the repository:

- `tests/unit/filter.test.ts` - Comprehensive filter operator tests
- `tests/unit/Collection.test.ts` - Collection CRUD operations
- `tests/integration/crud-workflow.test.ts` - Integration test patterns
- `tests/e2e/relationships.test.ts` - End-to-end relationship tests
- `tests/benchmarks/crud.bench.ts` - CRUD operation benchmarks
