import { withPayload } from '@payloadcms/next/withPayload'

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Payload requires this for proper module resolution
  experimental: {
    reactCompiler: false,
  },
  // OpenNext compatibility
  output: 'standalone',
}

export default withPayload(nextConfig)
