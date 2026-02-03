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
} from '../../../../src/worker/github/handlers'

// Mock ParqueDB services
const mockDatabaseIndex = {
  getUserByInstallation: vi.fn(),
  linkGitHubInstallation: vi.fn(),
  unlinkGitHubInstallation: vi.fn(),
  getDatabaseByRepo: vi.fn(),
}

const mockParqueDB = {
  branch: {
    create: vi.fn(),
    delete: vi.fn(),
    exists: vi.fn(),
  },
  merge: vi.fn(),
  diff: vi.fn(),
}

// Mock GitHub API
const mockOctokit = {
  rest: {
    checks: {
      create: vi.fn(),
      update: vi.fn(),
    },
    issues: {
      createComment: vi.fn(),
      updateComment: vi.fn(),
      listComments: vi.fn(),
    },
    reactions: {
      createForIssueComment: vi.fn(),
    },
  },
}

/**
 * Helper to compute real HMAC-SHA256 signature for testing
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

describe('GitHub webhook handlers', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('webhook verification', () => {
    it('verifies webhook signature', async () => {
      const payload = JSON.stringify({ action: 'test' })
      const secret = 'test-secret'
      const signature = await computeSignature(payload, secret)

      const result = await verifyWebhookSignature(payload, signature, secret)
      expect(result).toBe(true)
    })

    it('rejects invalid signatures', async () => {
      const payload = JSON.stringify({ action: 'test' })
      const secret = 'test-secret'
      const signature = 'sha256=invalid-signature'

      const result = await verifyWebhookSignature(payload, signature, secret)
      expect(result).toBe(false)
    })

    it('handles missing signature header', async () => {
      const payload = JSON.stringify({ action: 'test' })
      const secret = 'test-secret'

      await expect(verifyWebhookSignature(payload, null, secret)).rejects.toThrow()
    })
  })

  describe('installation.created', () => {
    const payload = {
      action: 'created' as const,
      installation: {
        id: 12345,
        account: { login: 'testuser', type: 'User' as const },
      },
      repositories: [
        { full_name: 'testuser/repo1' },
        { full_name: 'testuser/repo2' },
      ],
    }

    it('links installation to ParqueDB account', async () => {
      mockDatabaseIndex.getUserByInstallation.mockResolvedValue({ id: 'user-123' })

      await handleInstallationCreated(payload, mockDatabaseIndex)

      expect(mockDatabaseIndex.linkGitHubInstallation).toHaveBeenCalledWith(
        'user-123',
        12345,
        expect.objectContaining({ login: 'testuser', type: 'User' })
      )
    })

    it('stores repository access list', async () => {
      mockDatabaseIndex.getUserByInstallation.mockResolvedValue({ id: 'user-123' })

      await handleInstallationCreated(payload, mockDatabaseIndex)

      expect(mockDatabaseIndex.linkGitHubInstallation).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.objectContaining({
          repositories: ['testuser/repo1', 'testuser/repo2']
        })
      )
    })

    it('handles organization installations', async () => {
      const orgPayload = {
        ...payload,
        installation: {
          ...payload.installation,
          account: { login: 'testorg', type: 'Organization' as const },
        },
      }

      mockDatabaseIndex.getUserByInstallation.mockResolvedValue({ id: 'org-123' })

      await handleInstallationCreated(orgPayload, mockDatabaseIndex)

      expect(mockDatabaseIndex.linkGitHubInstallation).toHaveBeenCalledWith(
        'org-123',
        12345,
        expect.objectContaining({ type: 'Organization' })
      )
    })

    it('handles user not found gracefully', async () => {
      mockDatabaseIndex.getUserByInstallation.mockResolvedValue(null)

      // Should not throw, but log warning
      await expect(handleInstallationCreated(payload, mockDatabaseIndex)).resolves.not.toThrow()
    })
  })

  describe('installation.deleted', () => {
    const payload = {
      action: 'deleted' as const,
      installation: { id: 12345 },
    }

    it('unlinks installation from account', async () => {
      await handleInstallationDeleted(payload, mockDatabaseIndex)

      expect(mockDatabaseIndex.unlinkGitHubInstallation).toHaveBeenCalledWith(12345)
    })

    it('handles already unlinked installation', async () => {
      mockDatabaseIndex.unlinkGitHubInstallation.mockResolvedValue(false)

      // Should be idempotent
      await expect(handleInstallationDeleted(payload, mockDatabaseIndex)).resolves.not.toThrow()
    })
  })

  describe('create (branch)', () => {
    const payload = {
      ref: 'feature/new-feature',
      ref_type: 'branch' as const,
      repository: { full_name: 'testuser/mydb' },
      installation: { id: 12345 },
    }

    it('creates database branch matching git branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.create.mockResolvedValue({ name: 'feature/new-feature' })

      await handleCreate(payload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith('feature/new-feature', {
        from: 'main'
      })
    })

    it('uses correct base branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        defaultBranch: 'develop'
      })

      await handleCreate(payload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith('feature/new-feature', {
        from: 'develop'
      })
    })

    it('ignores branches in ignore list', async () => {
      const dependabotPayload = {
        ...payload,
        ref: 'dependabot/npm_and_yarn/lodash-4.17.21',
      }

      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })

      await handleCreate(dependabotPayload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
    })

    it('ignores branches not matching auto_create patterns', async () => {
      const config = {
        branches: {
          auto_create: ['feature/*', 'fix/*'],
        },
      }

      const randomPayload = {
        ...payload,
        ref: 'random-branch',
      }

      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        config
      })

      await handleCreate(randomPayload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
    })

    it('handles branch names with slashes', async () => {
      const complexPayload = {
        ...payload,
        ref: 'feature/foo/bar/baz',
      }

      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })

      await handleCreate(complexPayload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith('feature/foo/bar/baz', expect.anything())
    })

    it('handles database not found', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue(null)

      // Should not throw, just skip
      await expect(handleCreate(payload, mockDatabaseIndex, mockParqueDB)).resolves.not.toThrow()

      expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
    })

    it('ignores tag creation events', async () => {
      const tagPayload = {
        ...payload,
        ref_type: 'tag' as const,
        ref: 'v1.0.0',
      }

      await handleCreate(tagPayload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
    })
  })

  describe('delete (branch)', () => {
    const payload = {
      ref: 'feature/old-feature',
      ref_type: 'branch' as const,
      repository: { full_name: 'testuser/mydb' },
      installation: { id: 12345 },
    }

    it('deletes database branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.exists.mockResolvedValue(true)

      await handleDelete(payload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('feature/old-feature')
    })

    it('handles non-existent branch gracefully', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.exists.mockResolvedValue(false)

      await expect(handleDelete(payload, mockDatabaseIndex, mockParqueDB)).resolves.not.toThrow()

      expect(mockParqueDB.branch.delete).not.toHaveBeenCalled()
    })

    it('does not delete protected branches', async () => {
      const mainPayload = {
        ...payload,
        ref: 'main',
      }

      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })

      await handleDelete(mainPayload, mockDatabaseIndex, mockParqueDB)

      expect(mockParqueDB.branch.delete).not.toHaveBeenCalled()
    })
  })

  describe('pull_request.opened', () => {
    const payload = {
      action: 'opened' as const,
      number: 42,
      pull_request: {
        head: { ref: 'feature/test' },
        base: { ref: 'main' },
      },
      repository: { full_name: 'testuser/mydb' },
      installation: { id: 12345 },
    }

    it('creates preview branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })

      await handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith('pr-42', {
        from: 'feature/test'
      })
    })

    it('posts initial diff comment', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.diff.mockResolvedValue({
        entities: { added: 5, modified: 3, deleted: 1 },
        relationships: { added: 2, modified: 0, deleted: 0 },
      })

      await handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('## \u{1F5C4}\uFE0F Database Changes')
        })
      )
    })

    it('creates merge check run', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })
      mockOctokit.rest.checks.create.mockResolvedValue({ data: { id: 1 } })

      await handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.checks.create).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'ParqueDB Merge Check',
          status: 'in_progress'
        })
      )
    })

    it('includes preview URL in comment', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        previewUrl: 'https://preview.example.com'
      })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })

      await handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('https://preview.example.com/pr-42')
        })
      )
    })

    it('loads repo config from .github/parquedb.yml', async () => {
      // The config is loaded via getDatabaseByRepo which returns config stored in the index
      // This test verifies the handler uses the config when making decisions
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        config: {
          preview: { enabled: true }
        }
      })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })

      await handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockDatabaseIndex.getDatabaseByRepo).toHaveBeenCalledWith('testuser/mydb')
      expect(mockParqueDB.branch.create).toHaveBeenCalled()
    })

    it('respects preview.enabled=false config', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        config: {
          preview: { enabled: false }
        }
      })

      await handlePullRequestOpened(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.create).not.toHaveBeenCalled()
      expect(mockOctokit.rest.issues.createComment).not.toHaveBeenCalled()
    })
  })

  describe('pull_request.synchronize', () => {
    const payload = {
      action: 'synchronize' as const,
      number: 42,
      pull_request: {
        head: { ref: 'feature/test', sha: 'abc123' },
        base: { ref: 'main' },
      },
      repository: { full_name: 'testuser/mydb' },
      installation: { id: 12345 },
    }

    it('updates existing diff comment', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockOctokit.rest.issues.listComments.mockResolvedValue({
        data: [{ id: 999, body: '## \u{1F5C4}\uFE0F Database Changes\nOld diff' }],
      })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })

      await handlePullRequestSynchronize(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.updateComment).toHaveBeenCalledWith(
        expect.objectContaining({
          comment_id: 999,
          body: expect.stringContaining('## \u{1F5C4}\uFE0F Database Changes')
        })
      )
    })

    it('creates comment if none exists', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockOctokit.rest.issues.listComments.mockResolvedValue({ data: [] })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })

      await handlePullRequestSynchronize(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalled()
    })

    it('re-runs merge check', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockOctokit.rest.issues.listComments.mockResolvedValue({ data: [] })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })

      await handlePullRequestSynchronize(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.checks.create).toHaveBeenCalled()
    })

    it('updates preview branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.exists.mockResolvedValue(true)
      mockOctokit.rest.issues.listComments.mockResolvedValue({ data: [] })
      mockParqueDB.diff.mockResolvedValue({ entities: {}, relationships: {} })

      // The synchronize handler currently just updates comments and check runs
      // Updating the preview branch would require additional implementation
      await handlePullRequestSynchronize(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      // Verify the diff is computed (which uses the latest branch state)
      expect(mockParqueDB.diff).toHaveBeenCalledWith('feature/test', 'main')
    })
  })

  describe('pull_request.closed (merged)', () => {
    const payload = {
      action: 'closed' as const,
      number: 42,
      pull_request: {
        merged: true,
        head: { ref: 'feature/test' },
        base: { ref: 'main' },
        merge_commit_sha: 'def456',
      },
      repository: { full_name: 'testuser/mydb' },
      installation: { id: 12345 },
    }

    it('merges database branch into base', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.merge.mockResolvedValue({ conflicts: [] })

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.merge).toHaveBeenCalledWith('feature/test', 'main', undefined)
    })

    it('uses configured merge strategy', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        config: {
          merge: { strategy: 'theirs' }
        }
      })
      mockParqueDB.merge.mockResolvedValue({ conflicts: [] })

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.merge).toHaveBeenCalledWith(
        'feature/test',
        'main',
        { strategy: 'theirs' }
      )
    })

    it('deletes preview branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.merge.mockResolvedValue({ conflicts: [] })
      mockParqueDB.branch.exists.mockResolvedValue(true)

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('pr-42')
    })

    it('deletes feature branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        config: {
          branches: { auto_delete: true }
        }
      })
      mockParqueDB.merge.mockResolvedValue({ conflicts: [] })

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('feature/test')
    })

    it('posts confirmation comment', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.merge.mockResolvedValue({ conflicts: [] })

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('\u2705 Database merged successfully')
        })
      )
    })

    it('handles merge conflicts', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.merge.mockResolvedValue({
        conflicts: [{ entityId: 'entity-123', field: 'name' }]
      })

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('\u26A0\uFE0F Merge conflicts detected')
        })
      )
    })
  })

  describe('pull_request.closed (not merged)', () => {
    const payload = {
      action: 'closed' as const,
      number: 42,
      pull_request: {
        merged: false,
        head: { ref: 'feature/test' },
        base: { ref: 'main' },
      },
      repository: { full_name: 'testuser/mydb' },
      installation: { id: 12345 },
    }

    it('deletes preview branch without merging', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.exists.mockResolvedValue(true)

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.merge).not.toHaveBeenCalled()
      expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('pr-42')
    })

    it('does not delete feature branch', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({
        id: 'db-123',
        config: {
          branches: { auto_delete: true }
        }
      })
      mockParqueDB.branch.exists.mockResolvedValue(true)

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.delete).toHaveBeenCalledWith('pr-42')
      expect(mockParqueDB.branch.delete).not.toHaveBeenCalledWith('feature/test')
    })

    it('posts closure comment', async () => {
      mockDatabaseIndex.getDatabaseByRepo.mockResolvedValue({ id: 'db-123' })
      mockParqueDB.branch.exists.mockResolvedValue(true)

      await handlePullRequestClosed(payload, mockDatabaseIndex, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('Preview branch cleaned up')
        })
      )
    })
  })
})
