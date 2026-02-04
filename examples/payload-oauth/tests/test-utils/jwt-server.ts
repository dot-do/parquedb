/**
 * Local JWKS Server for E2E Testing
 *
 * This module provides a local JWKS server that serves test keys for JWT verification.
 * It generates an RSA key pair at startup and exposes the public key via a standard
 * JWKS endpoint at /.well-known/jwks.json
 *
 * Usage:
 *   const { server, privateKey, publicKey, kid } = await startJWKSServer(3456)
 *   // Use privateKey to sign test tokens
 *   // Configure app to use http://localhost:3456/.well-known/jwks.json as JWKS URI
 *   await stopJWKSServer(server)
 */

import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'http'
import { generateKeyPair, exportJWK, type KeyLike, type JWK } from 'jose'

export interface JWKSServerConfig {
  port: number
  kid?: string
}

export interface JWKSServerResult {
  server: Server
  privateKey: KeyLike
  publicKey: KeyLike
  kid: string
  jwksUri: string
}

/**
 * Generate an RSA key pair for signing test JWTs
 */
export async function generateTestKeyPair() {
  const { publicKey, privateKey } = await generateKeyPair('RS256', {
    modulusLength: 2048,
  })
  return { publicKey, privateKey }
}

/**
 * Start a local JWKS server that serves test keys
 *
 * @param port - Port to listen on (default: 3456)
 * @param kid - Key ID for the JWKS (default: 'test-key-1')
 * @returns Server instance, keys, and JWKS URI
 */
export async function startJWKSServer(
  port: number = 3456,
  kid: string = 'test-key-1'
): Promise<JWKSServerResult> {
  // Generate RSA key pair
  const { publicKey, privateKey } = await generateTestKeyPair()

  // Export public key to JWK format
  const jwk = await exportJWK(publicKey)
  const jwks = {
    keys: [
      {
        ...jwk,
        kid,
        use: 'sig',
        alg: 'RS256',
      } as JWK,
    ],
  }

  // Create HTTP server
  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    // CORS headers for cross-origin requests
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS')
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type')

    // Handle preflight
    if (req.method === 'OPTIONS') {
      res.writeHead(204)
      res.end()
      return
    }

    // Serve JWKS at standard endpoint
    if (req.url === '/.well-known/jwks.json' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(jwks))
      return
    }

    // Health check endpoint
    if (req.url === '/health' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ status: 'ok', kid }))
      return
    }

    // 404 for everything else
    res.writeHead(404, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: 'Not found' }))
  })

  // Start server
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(port, () => {
      server.removeListener('error', reject)
      resolve()
    })
  })

  const jwksUri = `http://localhost:${port}/.well-known/jwks.json`

  console.log(`JWKS server started at ${jwksUri}`)

  return {
    server,
    privateKey,
    publicKey,
    kid,
    jwksUri,
  }
}

/**
 * Stop the JWKS server
 */
export async function stopJWKSServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((err) => {
      if (err) reject(err)
      else resolve()
    })
  })
  console.log('JWKS server stopped')
}

/**
 * Wait for the JWKS server to be ready
 */
export async function waitForJWKSServer(
  jwksUri: string,
  maxAttempts: number = 30,
  delayMs: number = 100
): Promise<boolean> {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const response = await fetch(jwksUri)
      if (response.ok) {
        const jwks = await response.json()
        if (jwks.keys && jwks.keys.length > 0) {
          return true
        }
      }
    } catch {
      // Server not ready yet
    }
    await new Promise(resolve => setTimeout(resolve, delayMs))
  }
  return false
}
