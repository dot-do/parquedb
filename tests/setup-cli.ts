/**
 * CLI Test Setup
 *
 * This file sets up mocks for CLI tests that import from @dotdo/cli,
 * which has dependencies on oauth.do that don't work in Node.js test environment.
 */

import { vi } from 'vitest'

// Mock oauth.do before any imports
vi.mock('oauth.do', () => ({
  FileTokenStorage: class FileTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  SecureFileTokenStorage: class SecureFileTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  KeychainTokenStorage: class KeychainTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  MemoryTokenStorage: class MemoryTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  LocalStorageTokenStorage: class LocalStorageTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  CompositeTokenStorage: class CompositeTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  auth: async () => null,
  getUser: async () => null,
  login: async () => {},
  logout: async () => {},
  getToken: async () => null,
  isAuthenticated: async () => false,
  buildAuthUrl: () => 'https://example.com/auth',
  configure: () => {},
  getConfig: () => ({}),
  authorizeDevice: async () => {},
  pollForTokens: async () => {},
  startGitHubDeviceFlow: async () => {},
  pollGitHubDeviceFlow: async () => {},
  getGitHubUser: async () => null,
  createSecureStorage: () => ({ getToken: async () => null, setToken: async () => {}, removeToken: async () => {} }),
}))

vi.mock('oauth.do/node', () => ({
  ensureLoggedIn: async () => {},
  ensureLoggedOut: async () => {},
  forceLogin: async () => {},
  getToken: async () => null,
  isAuthenticated: async () => false,
  FileTokenStorage: class FileTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  SecureFileTokenStorage: class SecureFileTokenStorage {
    private storage = new Map<string, string>()
    async getToken() { return this.storage.get('token') ?? null }
    async setToken(token: string) { this.storage.set('token', token) }
    async removeToken() { this.storage.delete('token') }
  },
  createSecureStorage: () => ({ getToken: async () => null, setToken: async () => {}, removeToken: async () => {} }),
}))

// Import the main setup after mocks are defined
import './setup'
