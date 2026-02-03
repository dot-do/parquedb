import { describe, it, expect, vi, beforeEach } from 'vitest'

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

const mockEnv = {
  GITHUB_APP_ID: 'test-app-id',
  GITHUB_APP_PRIVATE_KEY: 'test-private-key',
  GITHUB_WEBHOOK_SECRET: 'test-webhook-secret',
  PARQUEDB: {} as DurableObjectNamespace,
  BUCKET: {} as R2Bucket,
}

describe('GitHub Webhooks Router', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('request validation', () => {
    it('rejects requests without event header', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: JSON.stringify({ action: 'test' }),
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(400)
      const body = await response.json()
      expect(body.error).toContain('X-GitHub-Event')
    })

    it('rejects requests with invalid signature', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({ action: 'test' })
      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'ping',
          'X-Hub-Signature-256': 'sha256=invalid',
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(401)
      const body = await response.json()
      expect(body.error).toContain('Invalid')
    })

    it('accepts requests with valid signature', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({ action: 'test' })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'ping',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('rejects requests with invalid JSON', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = 'invalid json'
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'ping',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(400)
      const body = await response.json()
      expect(body.error).toContain('Invalid JSON')
    })
  })

  describe('event routing', () => {
    it('handles ping events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({ zen: 'Keep it simple' })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'ping',
          'X-GitHub-Delivery': 'test-delivery-id',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
      const body = await response.json()
      expect(body.success).toBe(true)
      expect(body.event).toBe('ping')
    })

    it('handles installation.created events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        action: 'created',
        installation: {
          id: 12345,
          account: { login: 'testuser', type: 'User' },
        },
        repositories: [{ full_name: 'testuser/repo1' }],
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'installation',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
      const body = await response.json()
      expect(body.event).toBe('installation')
    })

    it('handles installation.deleted events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        action: 'deleted',
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'installation',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('handles create (branch) events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        ref: 'feature/test',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'create',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('handles delete (branch) events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        ref: 'feature/old',
        ref_type: 'branch',
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'delete',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('handles pull_request.opened events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        action: 'opened',
        number: 42,
        pull_request: {
          head: { ref: 'feature/test', sha: 'abc123' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'pull_request',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('handles pull_request.synchronize events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        action: 'synchronize',
        number: 42,
        pull_request: {
          head: { ref: 'feature/test', sha: 'def456' },
          base: { ref: 'main' },
        },
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'pull_request',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('handles pull_request.closed events', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        action: 'closed',
        number: 42,
        pull_request: {
          merged: true,
          head: { ref: 'feature/test' },
          base: { ref: 'main' },
          merge_commit_sha: 'ghi789',
        },
        repository: { full_name: 'testuser/mydb' },
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'pull_request',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('handles issue_comment.created events on PRs', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        action: 'created',
        issue: {
          number: 42,
          pull_request: { url: 'https://api.github.com/repos/testuser/mydb/pulls/42' },
        },
        comment: {
          id: 123,
          body: '/parquedb help',
          user: { login: 'contributor' },
        },
        repository: {
          full_name: 'testuser/mydb',
          owner: { login: 'testuser' },
          name: 'mydb',
        },
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'issue_comment',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })

    it('ignores issue_comment events on issues (not PRs)', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({
        action: 'created',
        issue: {
          number: 42,
          // No pull_request field - this is a regular issue
        },
        comment: {
          id: 123,
          body: '/parquedb help',
          user: { login: 'contributor' },
        },
        repository: {
          full_name: 'testuser/mydb',
          owner: { login: 'testuser' },
          name: 'mydb',
        },
        installation: { id: 12345 },
      })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'issue_comment',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
      // Should succeed but not process the command
    })

    it('handles unknown events gracefully', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({ action: 'test' })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'unknown_event',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.status).toBe(200)
    })
  })

  describe('response format', () => {
    it('returns JSON content type', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({ zen: 'Test' })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'ping',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)

      expect(response.headers.get('Content-Type')).toBe('application/json')
    })

    it('includes event type in success response', async () => {
      const { handleGitHubWebhook } = await import('../../../../src/worker/github/webhooks')

      const payload = JSON.stringify({ zen: 'Test' })
      const signature = await computeSignature(payload, mockEnv.GITHUB_WEBHOOK_SECRET)

      const request = new Request('https://api.parque.db/webhooks/github', {
        method: 'POST',
        body: payload,
        headers: {
          'X-GitHub-Event': 'ping',
          'X-Hub-Signature-256': signature,
        },
      })

      const response = await handleGitHubWebhook(request, mockEnv as any)
      const body = await response.json()

      expect(body.success).toBe(true)
      expect(body.event).toBe('ping')
    })
  })
})
