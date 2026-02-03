/**
 * Webhook Handler Tests
 *
 * Tests for GitHub webhook handling including:
 * - Webhook registration (signature verification)
 * - Webhook triggering (event handling flow)
 * - Error handling (invalid payloads, missing headers, etc.)
 *
 * These tests complement the handlers.test.ts tests in the github/ directory
 * by focusing on the full request/response flow and edge cases.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  verifyWebhookSignature,
  handleInstallationCreated,
  handleInstallationDeleted,
  handleCreate,
  handleDelete,
  handlePullRequestOpened,
  handlePullRequestSynchronize,
  handlePullRequestClosed,
  type DatabaseIndexService,
  type ParqueDBService,
  type OctokitClient,
  type InstallationCreatedPayload,
  type InstallationDeletedPayload,
  type CreatePayload,
  type DeletePayload,
  type PullRequestPayload,
} from '../../../src/worker/github/handlers'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Compute HMAC-SHA256 signature for testing webhook verification
 */
async function computeSignature(payload: string, secret: string): Promise<string> {
  const encoder = new TextEncoder()
  const keyData = encoder.encode(secret)
  const messageData = encoder.encode(payload)

  const key = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  )

  const signatureBuffer = await crypto.subtle.sign('HMAC', key, messageData)
  const signatureArray = new Uint8Array(signatureBuffer)

  const hex = Array.from(signatureArray)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('')

  return `sha256=${hex}`
}

/**
 * Create mock DatabaseIndexService
 */
function createMockDatabaseIndex(): DatabaseIndexService {
  return {
    getUserByInstallation: vi.fn(),
    linkGitHubInstallation: vi.fn(),
    unlinkGitHubInstallation: vi.fn(),
    getDatabaseByRepo: vi.fn(),
  }
}

/**
 * Create mock ParqueDBService
 */
function createMockParqueDB(): ParqueDBService {
  return {
    branch: {
      create: vi.fn().mockResolvedValue({ name: 'branch' }),
      delete: vi.fn().mockResolvedValue(true),
      exists: vi.fn().mockResolvedValue(true),
    },
    merge: vi.fn().mockResolvedValue({ conflicts: [] }),
    diff: vi.fn().mockResolvedValue({ entities: {}, relationships: {} }),
  }
}

/**
 * Create mock OctokitClient
 */
function createMockOctokit(): OctokitClient {
  return {
    rest: {
      checks: {
        create: vi.fn().mockResolvedValue({ data: { id: 1 } }),
        update: vi.fn().mockResolvedValue({ data: { id: 1 } }),
      },
      issues: {
        createComment: vi.fn().mockResolvedValue({ data: { id: 1 } }),
        updateComment: vi.fn().mockResolvedValue({ data: { id: 1 } }),
        listComments: vi.fn().mockResolvedValue({ data: [] }),
      },
      reactions: {
        createForIssueComment: vi.fn().mockResolvedValue({ data: { id: 1 } }),
      },
    },
  }
}

// =============================================================================
// Webhook Registration Tests (Signature Verification)
// =============================================================================

describe('Webhook Registration', () => {
  describe('verifyWebhookSignature', () => {
    const SECRET = 'webhook-secret-key-12345'

    it('should verify valid signature', async () => {
      const payload = JSON.stringify({ action: 'test', data: 'value' })
      const signature = await computeSignature(payload, SECRET)

      const result = await verifyWebhookSignature(payload, signature, SECRET)

      expect(result).toBe(true)
    })

    it('should reject invalid signature', async () => {
      const payload = JSON.stringify({ action: 'test' })
      const signature = 'sha256=0000000000000000000000000000000000000000000000000000000000000000'

      const result = await verifyWebhookSignature(payload, signature, SECRET)

      expect(result).toBe(false)
    })

    it('should reject signature with wrong secret', async () => {
      const payload = JSON.stringify({ action: 'test' })
      const signature = await computeSignature(payload, 'wrong-secret')

      const result = await verifyWebhookSignature(payload, signature, SECRET)

      expect(result).toBe(false)
    })

    it('should throw when signature header is missing', async () => {
      const payload = JSON.stringify({ action: 'test' })

      await expect(
        verifyWebhookSignature(payload, null, SECRET)
      ).rejects.toThrow('Missing signature header')
    })

    it('should reject signature without sha256= prefix', async () => {
      const payload = JSON.stringify({ action: 'test' })
      const signatureWithoutPrefix = 'invalid-format'

      const result = await verifyWebhookSignature(payload, signatureWithoutPrefix, SECRET)

      expect(result).toBe(false)
    })

    it('should reject empty signature', async () => {
      const payload = JSON.stringify({ action: 'test' })

      await expect(
        verifyWebhookSignature(payload, '', SECRET)
      ).rejects.toThrow('Missing signature header')
    })

    it('should verify signature for large payloads', async () => {
      const largePayload = JSON.stringify({
        action: 'test',
        data: 'x'.repeat(100000),
        nested: { deep: { value: 'y'.repeat(50000) } },
      })
      const signature = await computeSignature(largePayload, SECRET)

      const result = await verifyWebhookSignature(largePayload, signature, SECRET)

      expect(result).toBe(true)
    })

    it('should verify signature for payloads with special characters', async () => {
      const specialPayload = JSON.stringify({
        action: 'test',
        message: 'Hello\nWorld\t"Quoted"\\Escaped',
        unicode: '\u{1F5C4}\uFE0F Database Changes',
      })
      const signature = await computeSignature(specialPayload, SECRET)

      const result = await verifyWebhookSignature(specialPayload, signature, SECRET)

      expect(result).toBe(true)
    })

    it('should be timing-safe against attacks', async () => {
      const payload = JSON.stringify({ action: 'test' })
      const validSignature = await computeSignature(payload, SECRET)
      const invalidSignature = validSignature.slice(0, -1) + '0'

      // Both should complete without significant timing difference
      const startValid = performance.now()
      await verifyWebhookSignature(payload, validSignature, SECRET)
      const validTime = performance.now() - startValid

      const startInvalid = performance.now()
      await verifyWebhookSignature(payload, invalidSignature, SECRET)
      const invalidTime = performance.now() - startInvalid

      // Timing should be similar (within reasonable margin)
      // This is a basic check - real timing attacks require more sophisticated analysis
      expect(Math.abs(validTime - invalidTime)).toBeLessThan(50)
    })
  })
})

// =============================================================================
// Webhook Triggering Tests (Event Handling)
// =============================================================================

describe('Webhook Triggering', () => {
  let mockDatabaseIndex: ReturnType<typeof createMockDatabaseIndex>
  let mockParqueDB: ReturnType<typeof createMockParqueDB>
  let mockOctokit: ReturnType<typeof createMockOctokit>

  beforeEach(() => {
    vi.clearAllMocks()
    mockDatabaseIndex = createMockDatabaseIndex()
    mockParqueDB = createMockParqueDB()
    mockOctokit = createMockOctokit()
  })

  // ===========================================================================
  // Installation Events
  // ===========================================================================

  describe('Installation Events', () => {
    describe('installation.created', () => {
      const basePayload: InstallationCreatedPayload = {
        action: 'created',
        installation: {
          id: 12345,
          account: { login: 'testuser', type: 'User' },
        },
        repositories: [
          { full_name: 'testuser/repo1' },
          { full_name: 'testuser/repo2' },
        ],
      }

      it('should link installation when user exists', async () => {
        mockDatabaseIndex.getUserByInstallation = vi.fn().mockResolvedValue({ id: 'user-123' })

        await handleInstallationCreated(basePayload, mockDatabaseIndex)

        expect(mockDatabaseIndex.linkGitHubInstallation).toHaveBeenCalledWith(
          'user-123',
          12345,
          expect.objectContaining({
            login: 'testuser',
            type: 'User',
            repositories: ['testuser/repo1', 'testuser/repo2'],
          })
        )
      })

      it('should handle installation without repositories', async () => {
        const payloadWithoutRepos: InstallationCreatedPayload = {
          ...basePayload,
          repositories: undefined,
        }
        mockDatabaseIndex.getUserByInstallation = vi.fn().mockResolvedValue({ id: 'user-123' })

        await handleInstallationCreated(payloadWithoutRepos, mockDatabaseIndex)

        expect(mockDatabaseIndex.linkGitHubInstallation).toHaveBeenCalledWith(
          'user-123',
          12345,
          expect.objectContaining({
            repositories: undefined,
          })
        )
      })

      it('should not throw when user not found', async () => {
        mockDatabaseIndex.getUserByInstallation = vi.fn().mockResolvedValue(null)

        await expect(
          handleInstallationCreated(basePayload, mockDatabaseIndex)
        ).resolves.not.toThrow()

        expect(mockDatabaseIndex.linkGitHubInstallation).not.toHaveBeenCalled()
      })

      it('should handle organization installations', async () => {
        const orgPayload: InstallationCreatedPayload = {
          ...basePayload,
          installation: {
            id: 99999,
            account: { login: 'myorg', type: 'Organization' },
          },
        }
        mockDatabaseIndex.getUserByInstallation = vi.fn().mockResolvedValue({ id: 'org-456' })

        await handleInstallationCreated(orgPayload, mockDatabaseIndex)

        expect(mockDatabaseIndex.linkGitHubInstallation).toHaveBeenCalledWith(
          'org-456',
          99999,
          expect.objectContaining({ type: 'Organization' })
        )
      })
    })

    describe('installation.deleted', () => {
      const payload: InstallationDeletedPayload = {
        action: 'deleted',
        installation: { id: 12345 },
      }

      it('should unlink installation', async () => {
        mockDatabaseIndex.unlinkGitHubInstallation = vi.fn().mockResolvedValue(true)

        await handleInstallationDeleted(payload, mockDatabaseIndex)

        expect(mockDatabaseIndex.unlinkGitHubInstallation).toHaveBeenCalledWith(12345)
      })

      it('should handle already unlinked installation gracefully', async () => {
        mockDatabaseIndex.unlinkGitHubInstallation = vi.fn().mockResolvedValue(false)

        await expect(
          handleInstallationDeleted(payload, mockDatabaseIndex)
        ).resolves.not.toThrow()
      })
    })
  })

  // ===========================================================================
  // Branch Events
  // ===========================================================================

  describe('Branch Events', () => {
    describe('create (branch)', () => {
      const basePayload: CreatePayload = {
        ref: 'feature/new-feature',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      }

      it('should create database branch for git branch', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })

        await handleCreate(basePayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.create).toHaveBeenCalledWith('feature/new-feature', {
          from: 'main',
        })
      })

      it('should use custom default branch', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({
          id: 'db-123',
          defaultBranch: 'develop',
        })

        await handleCreate(basePayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.create).toHaveBeenCalledWith('feature/new-feature', {
          from: 'develop',
        })
      })

      it('should ignore tag creation', async () => {
        const tagPayload: CreatePayload = {
          ...basePayload,
          ref: 'v1.0.0',
          ref_type: 'tag',
        }

        await handleCreate(tagPayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
      })

      it('should ignore dependabot branches by default', async () => {
        const dependabotPayload: CreatePayload = {
          ...basePayload,
          ref: 'dependabot/npm/lodash-4.17.21',
        }
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })

        await handleCreate(dependabotPayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
      })

      it('should ignore renovate branches by default', async () => {
        const renovatePayload: CreatePayload = {
          ...basePayload,
          ref: 'renovate/typescript-5.x',
        }
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })

        await handleCreate(renovatePayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
      })

      it('should respect auto_create patterns', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({
          id: 'db-123',
          config: {
            branches: {
              auto_create: ['feature/*', 'fix/*'],
            },
          },
        })

        // Feature branch should be created
        await handleCreate(basePayload, mockDatabaseIndex, mockParqueDB)
        expect(mockParqueDB.branch.create).toHaveBeenCalled()

        vi.clearAllMocks()

        // Random branch should not be created
        const randomPayload: CreatePayload = {
          ...basePayload,
          ref: 'random-branch',
        }
        await handleCreate(randomPayload, mockDatabaseIndex, mockParqueDB)
        expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
      })

      it('should handle database not found', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue(null)

        await expect(
          handleCreate(basePayload, mockDatabaseIndex, mockParqueDB)
        ).resolves.not.toThrow()

        expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
      })
    })

    describe('delete (branch)', () => {
      const basePayload: DeletePayload = {
        ref: 'feature/old-feature',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      }

      it('should delete database branch for git branch', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockParqueDB.branch.exists = vi.fn().mockResolvedValue(true)

        await handleDelete(basePayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('feature/old-feature')
      })

      it('should not delete protected branches', async () => {
        const mainPayload: DeletePayload = { ...basePayload, ref: 'main' }
        const masterPayload: DeletePayload = { ...basePayload, ref: 'master' }

        await handleDelete(mainPayload, mockDatabaseIndex, mockParqueDB)
        await handleDelete(masterPayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.delete).not.toHaveBeenCalled()
      })

      it('should not delete non-existent branch', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockParqueDB.branch.exists = vi.fn().mockResolvedValue(false)

        await handleDelete(basePayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.delete).not.toHaveBeenCalled()
      })

      it('should ignore tag deletion', async () => {
        const tagPayload: DeletePayload = {
          ...basePayload,
          ref: 'v1.0.0',
          ref_type: 'tag',
        }

        await handleDelete(tagPayload, mockDatabaseIndex, mockParqueDB)

        expect(mockParqueDB.branch.delete).not.toHaveBeenCalled()
      })
    })
  })

  // ===========================================================================
  // Pull Request Events
  // ===========================================================================

  describe('Pull Request Events', () => {
    describe('pull_request.opened', () => {
      const basePayload: PullRequestPayload = {
        action: 'opened',
        number: 42,
        pull_request: {
          head: { ref: 'feature/test', sha: 'abc123' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      }

      it('should create preview branch and post diff comment', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockParqueDB.diff = vi.fn().mockResolvedValue({
          entities: { added: 5, modified: 3 },
          relationships: { added: 2 },
        })

        await handlePullRequestOpened(basePayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockParqueDB.branch.create).toHaveBeenCalledWith('pr-42', {
          from: 'feature/test',
        })
        expect(mockOctokit.rest.issues.createComment).toHaveBeenCalled()
        expect(mockOctokit.rest.checks.create).toHaveBeenCalled()
      })

      it('should skip when preview is disabled', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({
          id: 'db-123',
          config: { preview: { enabled: false } },
        })

        await handlePullRequestOpened(basePayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
        expect(mockOctokit.rest.issues.createComment).not.toHaveBeenCalled()
      })

      it('should include preview URL in comment', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({
          id: 'db-123',
          previewUrl: 'https://preview.example.com',
        })
        mockParqueDB.diff = vi.fn().mockResolvedValue({ entities: {}, relationships: {} })

        await handlePullRequestOpened(basePayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
          expect.objectContaining({
            body: expect.stringContaining('https://preview.example.com/pr-42'),
          })
        )
      })
    })

    describe('pull_request.synchronize', () => {
      const basePayload: PullRequestPayload = {
        action: 'synchronize',
        number: 42,
        pull_request: {
          head: { ref: 'feature/test', sha: 'def456' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      }

      it('should update existing comment', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockOctokit.rest.issues.listComments = vi.fn().mockResolvedValue({
          data: [{ id: 999, body: '## \u{1F5C4}\uFE0F Database Changes\nOld content' }],
        })
        mockParqueDB.diff = vi.fn().mockResolvedValue({ entities: {}, relationships: {} })

        await handlePullRequestSynchronize(basePayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockOctokit.rest.issues.updateComment).toHaveBeenCalledWith(
          expect.objectContaining({ comment_id: 999 })
        )
      })

      it('should create new comment if none exists', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockOctokit.rest.issues.listComments = vi.fn().mockResolvedValue({ data: [] })
        mockParqueDB.diff = vi.fn().mockResolvedValue({ entities: {}, relationships: {} })

        await handlePullRequestSynchronize(basePayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockOctokit.rest.issues.createComment).toHaveBeenCalled()
        expect(mockOctokit.rest.issues.updateComment).not.toHaveBeenCalled()
      })
    })

    describe('pull_request.closed (merged)', () => {
      const mergedPayload: PullRequestPayload = {
        action: 'closed',
        number: 42,
        pull_request: {
          merged: true,
          head: { ref: 'feature/test' },
          base: { ref: 'main' },
          merge_commit_sha: 'merge123',
        },
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      }

      it('should merge database branch and clean up', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockParqueDB.merge = vi.fn().mockResolvedValue({ conflicts: [] })
        mockParqueDB.branch.exists = vi.fn().mockResolvedValue(true)

        await handlePullRequestClosed(mergedPayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockParqueDB.merge).toHaveBeenCalledWith('feature/test', 'main', undefined)
        expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('pr-42')
      })

      it('should use configured merge strategy', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({
          id: 'db-123',
          config: { merge: { strategy: 'theirs' } },
        })
        mockParqueDB.merge = vi.fn().mockResolvedValue({ conflicts: [] })

        await handlePullRequestClosed(mergedPayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockParqueDB.merge).toHaveBeenCalledWith(
          'feature/test',
          'main',
          { strategy: 'theirs' }
        )
      })

      it('should report merge conflicts', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockParqueDB.merge = vi.fn().mockResolvedValue({
          conflicts: [
            { entityId: 'entity-1', field: 'name' },
            { entityId: 'entity-2', field: 'status' },
          ],
        })

        await handlePullRequestClosed(mergedPayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
          expect.objectContaining({
            body: expect.stringContaining('Merge conflicts detected'),
          })
        )
      })

      it('should auto-delete feature branch when configured', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({
          id: 'db-123',
          config: { branches: { auto_delete: true } },
        })
        mockParqueDB.merge = vi.fn().mockResolvedValue({ conflicts: [] })

        await handlePullRequestClosed(mergedPayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('feature/test')
      })
    })

    describe('pull_request.closed (not merged)', () => {
      const closedPayload: PullRequestPayload = {
        action: 'closed',
        number: 42,
        pull_request: {
          merged: false,
          head: { ref: 'feature/test' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      }

      it('should clean up preview branch without merging', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
        mockParqueDB.branch.exists = vi.fn().mockResolvedValue(true)

        await handlePullRequestClosed(closedPayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockParqueDB.merge).not.toHaveBeenCalled()
        expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('pr-42')
      })

      it('should not delete feature branch even with auto_delete', async () => {
        mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({
          id: 'db-123',
          config: { branches: { auto_delete: true } },
        })
        mockParqueDB.branch.exists = vi.fn().mockResolvedValue(true)

        await handlePullRequestClosed(closedPayload, mockDatabaseIndex, mockParqueDB, mockOctokit)

        expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('pr-42')
        expect(mockParqueDB.branch.delete).not.toHaveBeenCalledWith('feature/test')
      })
    })
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Webhook Error Handling', () => {
  let mockDatabaseIndex: ReturnType<typeof createMockDatabaseIndex>
  let mockParqueDB: ReturnType<typeof createMockParqueDB>
  let mockOctokit: ReturnType<typeof createMockOctokit>

  beforeEach(() => {
    vi.clearAllMocks()
    mockDatabaseIndex = createMockDatabaseIndex()
    mockParqueDB = createMockParqueDB()
    mockOctokit = createMockOctokit()
  })

  describe('Database Service Errors', () => {
    it('should handle getDatabaseByRepo failure gracefully', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockRejectedValue(new Error('Database unavailable'))

      const payload: CreatePayload = {
        ref: 'feature/test',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
      }

      await expect(
        handleCreate(payload, mockDatabaseIndex, mockParqueDB)
      ).rejects.toThrow('Database unavailable')
    })

    it('should handle getUserByInstallation failure gracefully', async () => {
      mockDatabaseIndex.getUserByInstallation = vi.fn().mockRejectedValue(new Error('Service error'))

      const payload: InstallationCreatedPayload = {
        action: 'created',
        installation: {
          id: 12345,
          account: { login: 'testuser', type: 'User' },
        },
      }

      await expect(
        handleInstallationCreated(payload, mockDatabaseIndex)
      ).rejects.toThrow('Service error')
    })
  })

  describe('Branch Service Errors', () => {
    it('should propagate branch.create errors', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.create = vi.fn().mockRejectedValue(new Error('Branch exists'))

      const payload: CreatePayload = {
        ref: 'feature/test',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
      }

      await expect(
        handleCreate(payload, mockDatabaseIndex, mockParqueDB)
      ).rejects.toThrow('Branch exists')
    })

    it('should propagate branch.delete errors', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.exists = vi.fn().mockResolvedValue(true)
      mockParqueDB.branch.delete = vi.fn().mockRejectedValue(new Error('Delete failed'))

      const payload: DeletePayload = {
        ref: 'feature/test',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
      }

      await expect(
        handleDelete(payload, mockDatabaseIndex, mockParqueDB)
      ).rejects.toThrow('Delete failed')
    })

    it('should propagate merge errors', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
      mockParqueDB.merge = vi.fn().mockRejectedValue(new Error('Merge conflict'))

      const payload: PullRequestPayload = {
        action: 'closed',
        number: 42,
        pull_request: {
          merged: true,
          head: { ref: 'feature/test' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
      }

      await expect(
        handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)
      ).rejects.toThrow('Merge conflict')
    })
  })

  describe('GitHub API Errors', () => {
    it('should propagate comment creation errors', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
      mockParqueDB.diff = vi.fn().mockResolvedValue({ entities: {}, relationships: {} })
      mockOctokit.rest.issues.createComment = vi.fn().mockRejectedValue(new Error('Rate limited'))

      const payload: PullRequestPayload = {
        action: 'opened',
        number: 42,
        pull_request: {
          head: { ref: 'feature/test' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
      }

      await expect(
        handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)
      ).rejects.toThrow('Rate limited')
    })

    it('should propagate check run creation errors', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
      mockParqueDB.diff = vi.fn().mockResolvedValue({ entities: {}, relationships: {} })
      mockOctokit.rest.issues.createComment = vi.fn().mockResolvedValue({ data: { id: 1 } })
      mockOctokit.rest.checks.create = vi.fn().mockRejectedValue(new Error('Insufficient permissions'))

      const payload: PullRequestPayload = {
        action: 'opened',
        number: 42,
        pull_request: {
          head: { ref: 'feature/test', sha: 'abc123' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
      }

      await expect(
        handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)
      ).rejects.toThrow('Insufficient permissions')
    })
  })

  describe('Edge Cases', () => {
    it('should handle empty repository name', async () => {
      const payload: CreatePayload = {
        ref: 'feature/test',
        ref_type: 'branch',
        repository: { full_name: '' },
      }

      // Should call getDatabaseByRepo with empty string
      await handleCreate(payload, mockDatabaseIndex, mockParqueDB)

      expect(mockDatabaseIndex.getDatabaseByRepo).toHaveBeenCalledWith('')
    })

    it('should handle branch names with special characters', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })

      const payload: CreatePayload = {
        ref: 'feature/test-branch_v2.0/with.dots',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
      }

      await handleCreate(payload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith(
        'feature/test-branch_v2.0/with.dots',
        expect.anything()
      )
    })

    it('should handle very long branch names', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })

      const longBranchName = 'feature/' + 'a'.repeat(200)
      const payload: CreatePayload = {
        ref: longBranchName,
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
      }

      await handleCreate(payload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith(
        longBranchName,
        expect.anything()
      )
    })

    it('should handle PR number 0', async () => {
      mockDatabaseIndex.getDatabaseByRepo = vi.fn().mockResolvedValue({ id: 'db-123' })
      mockParqueDB.diff = vi.fn().mockResolvedValue({ entities: {}, relationships: {} })

      const payload: PullRequestPayload = {
        action: 'opened',
        number: 0,
        pull_request: {
          head: { ref: 'feature/test' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
      }

      await handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith('pr-0', expect.anything())
    })
  })
})
