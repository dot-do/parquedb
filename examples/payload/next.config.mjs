import { withPayload } from '@payloadcms/next/withPayload'

/** @type {import('next').NextConfig} */
const nextConfig = {
  // OpenNext compatibility
  output: 'standalone',
  webpack: (config, { isServer, webpack }) => {
    // Extension alias required by Payload
    config.resolve.extensionAlias = {
      '.cjs': ['.cts', '.cjs'],
      '.js': ['.ts', '.tsx', '.js', '.jsx'],
      '.mjs': ['.mts', '.mjs'],
    }

    if (!isServer) {
      config.resolve.fallback = {
        ...config.resolve.fallback,
        keytar: false,
        'oauth.do': false,
        bson: false,
      }
    }

    config.externals = config.externals || []
    config.externals.push({
      keytar: 'commonjs keytar',
      'cloudflare:workers': 'commonjs cloudflare:workers',
    })

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
      new webpack.IgnorePlugin({
        resourceRegExp: /^bson$/,
      }),
    )

    config.plugins.push({
      apply: (compiler) => {
        compiler.hooks.normalModuleFactory.tap('IgnoreCloudflare', (nmf) => {
          nmf.hooks.beforeResolve.tap('IgnoreCloudflare', (result) => {
            if (result?.request?.startsWith('cloudflare:')) {
              return false
            }
          })
        })
      }
    })

    return config
  },
}

export default withPayload(nextConfig, { devBundleServerPackages: false })
