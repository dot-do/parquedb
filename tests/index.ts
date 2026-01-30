/**
 * Test Utilities Index
 *
 * Re-exports all test utilities for convenient importing.
 *
 * @example
 * ```ts
 * import { createTestEntity, USERS, toBeValidEntity } from '../test'
 * ```
 */

// Factories - Functions to create test data
export {
  generateTestId,
  resetIdCounter,
  createEntityId,
  createAuditFields,
  createEntityRef,
  createTestEntity,
  createTestEntities,
  createEntityRecord,
  createCreateInput,
  createPostInput,
  createUserInput,
  createTestSchema,
  createBlogSchema,
  createTypeDefinition,
  createEqualityFilter,
  createComparisonFilter,
  createAndFilter,
  createOrFilter,
  createTestData,
  decodeData,
  createRandomData,
  createRelativeDate,
  createPastDate,
  createFutureDate,
} from './factories'

// Fixtures - Pre-built test data
export {
  USERS,
  CATEGORIES,
  POSTS,
  BLOG_SCHEMA,
  ECOMMERCE_SCHEMA,
  getAllUsers,
  getAllCategories,
  getAllPosts,
  getAllFixtures,
  CREATE_INPUTS,
  FILTERS,
  BINARY_DATA,
} from './fixtures'

// Matchers - Custom Vitest assertions
export { parquedbMatchers } from './matchers'

// Setup utilities - Environment detection and helpers
export {
  cleanupAfterTest,
  isNode,
  isBrowser,
  isWorkers,
  getEnvironment,
  shouldSkipInEnvironment,
  shouldRunInEnvironment,
  describeForEnvironment,
  assertNode,
  assertBrowser,
  assertWorkers,
  delay,
  randomBytes,
  uniqueTestId,
  useTestContext,
} from './setup'

// Mock storage implementations
export {
  type MockStorage,
  MemoryStorageMock,
  MockR2Bucket,
  MockKVNamespace,
  MockIndexedDBStorage,
  MockFileSystemStorage,
  createMockStorage,
  useMockStorage,
} from './mocks/storage'
