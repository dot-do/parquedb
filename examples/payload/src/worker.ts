/**
 * Cloudflare Worker for Payload CMS with ParqueDB
 *
 * This Worker serves Payload CMS using R2 for storage.
 * It demonstrates how to run Payload on Cloudflare Workers
 * with ParqueDB as the database.
 */

import { buildConfig } from 'payload'
import { parquedbAdapter } from 'parquedb/payload'
import { R2Backend } from 'parquedb'
import { Posts, Categories, Media, Users } from './collections'
import { SiteSettings } from './globals'

interface Env {
  DATA: R2Bucket
  PAYLOAD_SECRET: string
}

/**
 * Create Payload configuration for Workers environment
 */
function createPayloadConfig(env: Env) {
  return buildConfig({
    serverURL: 'https://your-worker.your-subdomain.workers.dev',

    admin: {
      user: Users.slug,
    },

    // Use R2Backend for Workers environment
    db: parquedbAdapter({
      storage: new R2Backend(env.DATA),
    }),

    collections: [Users, Posts, Categories, Media],
    globals: [SiteSettings],

    secret: env.PAYLOAD_SECRET,
  })
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)

    // Health check endpoint
    if (url.pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok' }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // For a full Payload implementation on Workers, you would need:
    // 1. Initialize Payload with the config
    // 2. Handle the request through Payload's handler
    //
    // Note: Full Payload 3.0 on Workers requires additional setup.
    // This example shows the database adapter configuration pattern.

    // Placeholder response
    return new Response(
      JSON.stringify({
        message: 'Payload CMS with ParqueDB on Cloudflare Workers',
        docs: 'https://payloadcms.com/docs',
        note: 'This is a configuration example. Full Payload on Workers requires additional setup.',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    )
  },
}
