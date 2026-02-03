/**
 * Mock for oauth.do module
 *
 * This mock provides stub implementations of oauth.do APIs
 * for running unit tests in Node.js environment.
 */

// Mock TokenStorage classes
export class FileTokenStorage {
  private storage = new Map<string, string>()

  async getToken(): Promise<string | null> {
    return this.storage.get('token') ?? null
  }

  async setToken(token: string): Promise<void> {
    this.storage.set('token', token)
  }

  async removeToken(): Promise<void> {
    this.storage.delete('token')
  }
}

export class SecureFileTokenStorage extends FileTokenStorage {}
export class KeychainTokenStorage extends FileTokenStorage {}
export class MemoryTokenStorage extends FileTokenStorage {}
export class LocalStorageTokenStorage extends FileTokenStorage {}
export class CompositeTokenStorage extends FileTokenStorage {}

// Mock auth functions
export async function auth(): Promise<{ token: string } | null> {
  return null
}

export async function getUser(): Promise<{ id: string; email: string } | null> {
  return null
}

export async function login(): Promise<void> {
  // Mock login
}

export async function logout(): Promise<void> {
  // Mock logout
}

export async function getToken(): Promise<string | null> {
  return null
}

export async function isAuthenticated(): Promise<boolean> {
  return false
}

export function buildAuthUrl(): string {
  return 'https://example.com/auth'
}

export function configure(): void {
  // Mock configure
}

export function getConfig(): Record<string, unknown> {
  return {}
}

export async function authorizeDevice(): Promise<void> {
  // Mock device authorization
}

export async function pollForTokens(): Promise<void> {
  // Mock token polling
}

export async function startGitHubDeviceFlow(): Promise<void> {
  // Mock GitHub device flow
}

export async function pollGitHubDeviceFlow(): Promise<void> {
  // Mock GitHub device flow polling
}

export async function getGitHubUser(): Promise<{ login: string } | null> {
  return null
}

export function createSecureStorage(): FileTokenStorage {
  return new FileTokenStorage()
}

// Node.js specific exports
export async function ensureLoggedIn(): Promise<void> {
  // Mock ensure logged in
}

export async function ensureLoggedOut(): Promise<void> {
  // Mock ensure logged out
}

export async function forceLogin(): Promise<void> {
  // Mock force login
}

// Export types (empty interfaces for mocking)
export interface AuthProvider {
  type: string
}

export interface OAuthConfig {
  clientId: string
}

export interface User {
  id: string
  email?: string | undefined
}

export interface AuthResult {
  token: string
  user?: User | undefined
}

export interface DeviceAuthorizationResponse {
  device_code: string
  user_code: string
  verification_uri: string
}

export interface TokenResponse {
  access_token: string
  token_type: string
}

export interface TokenError {
  error: string
  error_description?: string | undefined
}

export interface TokenStorage {
  getToken(): Promise<string | null>
  setToken(token: string): Promise<void>
  removeToken(): Promise<void>
}

export interface LoginOptions {
  provider?: string | undefined
}

export interface LoginResult {
  success: boolean
}

export interface OAuthProvider {
  name: string
}

export interface GitHubDeviceFlowOptions {
  clientId: string
}

export interface GitHubDeviceAuthResponse {
  device_code: string
  user_code: string
}

export interface GitHubTokenResponse {
  access_token: string
}

export interface GitHubUser {
  login: string
  id: number
}
