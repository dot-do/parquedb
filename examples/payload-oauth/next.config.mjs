import { withPayload } from '@payloadcms/next/withPayload'

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Payload requires this for proper module resolution
  experimental: {
    reactCompiler: false,
  },
  // OpenNext compatibility
  output: 'standalone',
  // Exclude native modules and Cloudflare-specific code from webpack bundling
  webpack: (config, { isServer, webpack }) => {
    if (!isServer) {
      // Client-side: exclude native modules entirely
      config.resolve.fallback = {
        ...config.resolve.fallback,
        keytar: false,
        'oauth.do': false,
        bson: false,
      }
    }

    // Mark problematic modules as external (won't be bundled)
    config.externals = config.externals || []
    config.externals.push({
      keytar: 'commonjs keytar',
      'cloudflare:workers': 'commonjs cloudflare:workers',
    })

    // Replace problematic parquedb modules with browser-safe versions
    config.plugins.push(
      new webpack.NormalModuleReplacementPlugin(
        /parquedb[\/\\]dist[\/\\]migration[\/\\]index\.js$/,
        (resource) => {
          resource.request = resource.request.replace(
            /migration[\/\\]index\.js$/,
            'migration/index.browser.js'
          )
        }
      ),
      new webpack.NormalModuleReplacementPlugin(
        /parquedb[\/\\]dist[\/\\]config[\/\\]env\.js$/,
        (resource) => {
          resource.request = resource.request.replace(
            /config[\/\\]env\.js$/,
            'config/env.browser.js'
          )
        }
      ),
      new webpack.NormalModuleReplacementPlugin(
        /parquedb[\/\\]dist[\/\\]config[\/\\]auth\.js$/,
        (resource) => {
          resource.request = resource.request.replace(
            /config[\/\\]auth\.js$/,
            'config/auth.browser.js'
          )
        }
      ),
      new webpack.NormalModuleReplacementPlugin(
        /parquedb[\/\\]dist[\/\\]index\.js$/,
        (resource) => {
          resource.request = resource.request.replace(
            /dist[\/\\]index\.js$/,
            'dist/index.browser.js'
          )
        }
      ),
      // Ignore bson - it's only needed for MongoDB migration
      new webpack.IgnorePlugin({
        resourceRegExp: /^bson$/,
      }),
    )

    // Ignore cloudflare: scheme
    config.plugins.push({
      apply: (compiler) => {
        compiler.hooks.normalModuleFactory.tap('IgnoreCloudflare', (nmf) => {
          nmf.hooks.beforeResolve.tap('IgnoreCloudflare', (result) => {
            if (result?.request?.startsWith('cloudflare:')) {
              return false // Skip this module
            }
          })
        })
      }
    })

    return config
  },
}

export default withPayload(nextConfig)
