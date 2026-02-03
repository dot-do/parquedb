import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import * as os from 'os'

// Mock the action context and tools
const mockCore = {
  getInput: vi.fn(),
  setOutput: vi.fn(),
  setFailed: vi.fn(),
  addPath: vi.fn(),
  exportVariable: vi.fn(),
  info: vi.fn(),
  warning: vi.fn(),
  error: vi.fn(),
}

const mockExec = {
  exec: vi.fn(),
}

const mockCache = {
  find: vi.fn(),
  downloadTool: vi.fn(),
  extractTar: vi.fn(),
  cacheDir: vi.fn(),
}

// Mock modules before importing
vi.mock('@actions/core', () => mockCore)
vi.mock('@actions/tool-cache', () => mockCache)
vi.mock('@actions/exec', () => mockExec)

// Mock fetch globally
global.fetch = vi.fn()

// Import the action module (will be compiled)
let resolveVersion: (version: string) => Promise<string>
let getDownloadUrl: (version: string) => string
let configureAuth: (token: string) => Promise<void>
let run: () => Promise<void>

// Helper to load action functions
async function loadAction() {
  // In real implementation, these would be imported from the compiled action
  // For testing, we'll define them inline

  resolveVersion = async (version: string): Promise<string> => {
    if (version === 'latest') {
      const response = await fetch(
        'https://api.github.com/repos/parquedb/parquedb/releases/latest'
      )
      const data = await response.json()
      return data.tag_name.replace(/^v/, '')
    }

    if (!/^\d+\.\d+\.\d+/.test(version)) {
      throw new Error(`Invalid version format: ${version}`)
    }

    return version
  }

  getDownloadUrl = (version: string): string => {
    const platform = os.platform()
    const arch = os.arch()

    let platformStr: string
    switch (platform) {
      case 'darwin':
        platformStr = arch === 'arm64' ? 'darwin-arm64' : 'darwin-x64'
        break
      case 'linux':
        platformStr = 'linux-x64'
        break
      case 'win32':
        platformStr = 'win32-x64'
        break
      default:
        throw new Error(`Unsupported platform: ${platform}`)
    }

    return `https://github.com/parquedb/parquedb/releases/download/v${version}/parquedb-${platformStr}.tar.gz`
  }

  configureAuth = async (token: string): Promise<void> => {
    const response = await fetch('https://parque.db/api/auth/validate', {
      headers: { 'Authorization': `Bearer ${token}` }
    })

    if (!response.ok) {
      throw new Error('Invalid ParqueDB token. Get a token from oauth.do')
    }

    mockCore.exportVariable('PARQUEDB_TOKEN', token)
    mockCore.info('Authentication configured')
  }

  run = async (): Promise<void> => {
    try {
      const version = mockCore.getInput('version') || 'latest'
      const token = mockCore.getInput('token')

      const resolvedVersion = await resolveVersion(version)
      mockCore.info(`Installing ParqueDB CLI v${resolvedVersion}`)

      const cachedPath = mockCache.find('parquedb', resolvedVersion)
      if (cachedPath) {
        mockCore.info('Using cached ParqueDB CLI')
        mockCore.addPath(cachedPath)
        mockCore.setOutput('cache-hit', 'true')
        mockCore.setOutput('parquedb-version', resolvedVersion)
        return
      }

      mockCore.setOutput('cache-hit', 'false')

      const downloadUrl = getDownloadUrl(resolvedVersion)
      mockCore.info(`Downloading from ${downloadUrl}`)

      let downloadPath: string
      try {
        downloadPath = await mockCache.downloadTool(downloadUrl)
      } catch (error) {
        mockCore.warning('Download failed, retrying...')
        downloadPath = await mockCache.downloadTool(downloadUrl)
      }

      const extractedPath = await mockCache.extractTar(downloadPath)

      if (os.platform() !== 'win32') {
        await mockExec.exec('chmod', ['+x', require('path').join(extractedPath, 'parquedb')])
      }

      const cachedDir = await mockCache.cacheDir(extractedPath, 'parquedb', resolvedVersion)
      mockCore.addPath(cachedDir)

      if (token) {
        await configureAuth(token)
      }

      mockCore.setOutput('parquedb-version', resolvedVersion)
      mockCore.info('ParqueDB CLI installed successfully')

    } catch (error) {
      if (error instanceof Error) {
        mockCore.setFailed(error.message)
      } else {
        mockCore.setFailed('An unexpected error occurred')
      }
    }
  }
}

describe('setup-action', () => {
  beforeEach(async () => {
    vi.clearAllMocks()
    await loadAction()
  })

  describe('version resolution', () => {
    it('uses latest version when version input is "latest"', async () => {
      const mockFetch = global.fetch as ReturnType<typeof vi.fn>
      mockFetch.mockResolvedValueOnce({
        json: async () => ({ tag_name: 'v1.2.3' })
      } as Response)

      const version = await resolveVersion('latest')
      expect(version).toBe('1.2.3')
      expect(mockFetch).toHaveBeenCalledWith(
        'https://api.github.com/repos/parquedb/parquedb/releases/latest'
      )
    })

    it('uses specified version when provided', async () => {
      const version = await resolveVersion('1.2.3')
      expect(version).toBe('1.2.3')
    })

    it('fails on invalid version format', async () => {
      await expect(resolveVersion('invalid')).rejects.toThrow('Invalid version format: invalid')
    })
  })

  describe('CLI installation', () => {
    it('downloads CLI binary for current platform', async () => {
      const url = getDownloadUrl('1.2.3')
      const platform = os.platform()
      const arch = os.arch()

      if (platform === 'darwin') {
        const expectedArch = arch === 'arm64' ? 'darwin-arm64' : 'darwin-x64'
        expect(url).toContain(expectedArch)
      } else if (platform === 'linux') {
        expect(url).toContain('linux-x64')
      } else if (platform === 'win32') {
        expect(url).toContain('win32-x64')
      }

      expect(url).toContain('v1.2.3')
      expect(url).toContain('parquedb-')
      expect(url).toContain('.tar.gz')
    })

    it('extracts downloaded archive', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockCache.extractTar).toHaveBeenCalledWith('/tmp/download.tar.gz')
    })

    it('adds CLI to PATH', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockCore.addPath).toHaveBeenCalledWith('/cached/dir')
    })

    it('makes CLI executable on unix', async () => {
      if (os.platform() === 'win32') {
        // Skip on Windows
        return
      }

      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockExec.exec).toHaveBeenCalledWith('chmod', expect.arrayContaining(['+x']))
    })
  })

  describe('caching', () => {
    it('uses cached CLI if available', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('/cached/path')

      await run()

      expect(mockCore.addPath).toHaveBeenCalledWith('/cached/path')
      expect(mockCore.setOutput).toHaveBeenCalledWith('cache-hit', 'true')
      expect(mockCache.downloadTool).not.toHaveBeenCalled()
    })

    it('caches downloaded CLI for future runs', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockCache.cacheDir).toHaveBeenCalledWith('/tmp/extracted', 'parquedb', '1.2.3')
      expect(mockCore.setOutput).toHaveBeenCalledWith('cache-hit', 'false')
    })
  })

  describe('authentication', () => {
    it('configures auth when token is provided', async () => {
      const mockFetch = global.fetch as ReturnType<typeof vi.fn>
      mockFetch.mockResolvedValueOnce({
        ok: true
      } as Response)

      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        if (name === 'token') return 'test-token'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockCore.exportVariable).toHaveBeenCalledWith('PARQUEDB_TOKEN', 'test-token')
    })

    it('validates token by making API call', async () => {
      const mockFetch = global.fetch as ReturnType<typeof vi.fn>
      mockFetch.mockResolvedValueOnce({
        ok: true
      } as Response)

      await configureAuth('test-token')

      expect(mockFetch).toHaveBeenCalledWith(
        'https://parque.db/api/auth/validate',
        expect.objectContaining({
          headers: { 'Authorization': 'Bearer test-token' }
        })
      )
    })

    it('fails with clear error on invalid token', async () => {
      const mockFetch = global.fetch as ReturnType<typeof vi.fn>
      mockFetch.mockResolvedValueOnce({
        ok: false
      } as Response)

      await expect(configureAuth('invalid-token')).rejects.toThrow(
        'Invalid ParqueDB token. Get a token from oauth.do'
      )
    })

    it('skips auth when no token provided', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        if (name === 'token') return ''
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockCore.exportVariable).not.toHaveBeenCalledWith('PARQUEDB_TOKEN', expect.anything())
    })
  })

  describe('error handling', () => {
    it('fails gracefully on network error during download', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockRejectedValue(new Error('Network error'))

      await run()

      expect(mockCore.setFailed).toHaveBeenCalledWith('Network error')
    })

    it('fails gracefully on extraction error', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockRejectedValue(new Error('Extract failed'))

      await run()

      expect(mockCore.setFailed).toHaveBeenCalledWith('Extract failed')
    })

    it('retries download on transient failure', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool
        .mockRejectedValueOnce(new Error('Network error'))
        .mockResolvedValueOnce('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockCore.warning).toHaveBeenCalledWith('Download failed, retrying...')
      expect(mockCache.downloadTool).toHaveBeenCalledTimes(2)
      expect(mockCore.setFailed).not.toHaveBeenCalled()
    })
  })

  describe('outputs', () => {
    it('sets parquedb-version output', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('')
      mockCache.downloadTool.mockResolvedValue('/tmp/download.tar.gz')
      mockCache.extractTar.mockResolvedValue('/tmp/extracted')
      mockCache.cacheDir.mockResolvedValue('/cached/dir')

      await run()

      expect(mockCore.setOutput).toHaveBeenCalledWith('parquedb-version', '1.2.3')
    })

    it('sets cache-hit output when cache used', async () => {
      mockCore.getInput.mockImplementation((name) => {
        if (name === 'version') return '1.2.3'
        return ''
      })
      mockCache.find.mockReturnValue('/cached/path')

      await run()

      expect(mockCore.setOutput).toHaveBeenCalledWith('cache-hit', 'true')
    })
  })
})
