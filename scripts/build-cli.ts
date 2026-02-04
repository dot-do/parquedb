#!/usr/bin/env tsx
/**
 * Build CLI Bundle
 *
 * Bundles the ParqueDB CLI into a single JavaScript file using esbuild.
 * This is necessary because:
 * 1. The project uses moduleResolution: "bundler" in tsconfig.json
 * 2. Node.js ESM requires explicit .js extensions for relative imports
 * 3. Bundling into a single file avoids this issue entirely
 *
 * Usage:
 *   pnpm build:cli
 *   # or
 *   tsx scripts/build-cli.ts
 */

import * as esbuild from 'esbuild'
import { existsSync, mkdirSync } from 'fs'
import { dirname, resolve } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const projectRoot = resolve(__dirname, '..')

async function buildCli() {
  const outfile = resolve(projectRoot, 'dist/cli-bundle.js')

  // Ensure dist directory exists
  const distDir = dirname(outfile)
  if (!existsSync(distDir)) {
    mkdirSync(distDir, { recursive: true })
  }

  console.log('Building CLI bundle...')

  try {
    const result = await esbuild.build({
      entryPoints: [resolve(projectRoot, 'src/cli/index.ts')],
      bundle: true,
      platform: 'node',
      target: 'node18',
      format: 'esm',
      outfile,
      // Mark external packages and Node.js built-ins
      external: [
        // Node.js built-ins (with and without node: prefix)
        'fs',
        'fs/promises',
        'path',
        'os',
        'crypto',
        'stream',
        'util',
        'events',
        'buffer',
        'url',
        'http',
        'https',
        'net',
        'tls',
        'zlib',
        'child_process',
        'readline',
        'assert',
        'worker_threads',
        'perf_hooks',
        'inspector',
        'node:fs',
        'node:fs/promises',
        'node:path',
        'node:os',
        'node:crypto',
        'node:stream',
        'node:util',
        'node:events',
        'node:buffer',
        'node:url',
        'node:http',
        'node:https',
        'node:net',
        'node:tls',
        'node:zlib',
        'node:child_process',
        'node:readline',
        'node:assert',
        'node:worker_threads',
        'node:perf_hooks',
        'node:inspector',
        // Native modules that cannot be bundled
        'keytar',
        // Note: cloudflare: imports are handled by the cloudflare-shim plugin
        // AWS SDK (optional dependency, should be external)
        '@aws-sdk/client-s3',
        // Optional peer dependencies
        'payload',
        'hono',
        '@modelcontextprotocol/sdk',
        'express',
        'fastify',
      ],
      // Enable source maps for debugging
      sourcemap: true,
      // Minify for smaller bundle size
      minify: false, // Keep readable for debugging
      // Tree shaking
      treeShaking: true,
      // Keep names for better error messages
      keepNames: true,
      // Define process.env.NODE_ENV for tree shaking
      define: {
        'process.env.NODE_ENV': '"production"',
      },
      // Log level
      logLevel: 'info',
      // Plugins for handling Node.js-incompatible modules
      plugins: [
        {
          name: 'externalize-native-modules',
          setup(build) {
            // Externalize any .node files (native addons)
            build.onResolve({ filter: /\.node$/ }, (args) => {
              return { path: args.path, external: true }
            })
          },
        },
        {
          name: 'cloudflare-shim',
          setup(build) {
            // Shim cloudflare:workers module for Node.js
            // The runtime detection code will handle returning early,
            // but we need to prevent the import from failing
            build.onResolve({ filter: /^cloudflare:/ }, (args) => {
              return {
                path: args.path,
                namespace: 'cloudflare-shim',
              }
            })

            build.onLoad({ filter: /.*/, namespace: 'cloudflare-shim' }, () => {
              return {
                contents: `
                  // Cloudflare Workers shim for Node.js CLI
                  // These exports will throw if actually used in Node.js
                  export const DurableObject = class DurableObject {
                    constructor() {
                      throw new Error('DurableObject is only available in Cloudflare Workers')
                    }
                  }
                  export const WorkerEntrypoint = class WorkerEntrypoint {
                    constructor() {
                      throw new Error('WorkerEntrypoint is only available in Cloudflare Workers')
                    }
                  }
                  export const Workflow = class Workflow {
                    constructor() {
                      throw new Error('Workflow is only available in Cloudflare Workers')
                    }
                  }
                  export const WorkflowEntrypoint = class WorkflowEntrypoint {
                    constructor() {
                      throw new Error('WorkflowEntrypoint is only available in Cloudflare Workers')
                    }
                  }
                  export const WorkflowEvent = class WorkflowEvent {}
                  export const WorkflowStep = class WorkflowStep {}
                  // Return undefined for env - runtime detection handles this
                  export const env = undefined
                `,
                loader: 'js',
              }
            })
          },
        },
      ],
    })

    if (result.errors.length > 0) {
      console.error('Build failed with errors:')
      for (const error of result.errors) {
        console.error(error)
      }
      process.exit(1)
    }

    if (result.warnings.length > 0) {
      console.warn('Build completed with warnings:')
      for (const warning of result.warnings) {
        console.warn(warning)
      }
    }

    console.log(`CLI bundle built successfully: ${outfile}`)

    // Make the bundle executable
    const { chmodSync } = await import('fs')
    chmodSync(outfile, 0o755)
    console.log('Made bundle executable')
  } catch (error) {
    console.error('Build failed:', error)
    process.exit(1)
  }
}

buildCli()
