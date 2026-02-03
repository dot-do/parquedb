import * as core from '@actions/core'
import * as tc from '@actions/tool-cache'
import * as exec from '@actions/exec'
import * as os from 'os'
import * as path from 'path'

async function run(): Promise<void> {
  try {
    const version = core.getInput('version') || 'latest'
    const token = core.getInput('token')

    // Resolve version
    const resolvedVersion = await resolveVersion(version)
    core.info(`Installing ParqueDB CLI v${resolvedVersion}`)

    // Check cache
    const cachedPath = tc.find('parquedb', resolvedVersion)
    if (cachedPath) {
      core.info('Using cached ParqueDB CLI')
      core.addPath(cachedPath)
      core.setOutput('cache-hit', 'true')
      core.setOutput('parquedb-version', resolvedVersion)
      return
    }

    core.setOutput('cache-hit', 'false')

    // Download
    const downloadUrl = getDownloadUrl(resolvedVersion)
    core.info(`Downloading from ${downloadUrl}`)

    let downloadPath: string
    try {
      downloadPath = await tc.downloadTool(downloadUrl)
    } catch (error) {
      // Retry once on transient failure
      core.warning('Download failed, retrying...')
      downloadPath = await tc.downloadTool(downloadUrl)
    }

    // Extract
    const extractedPath = await tc.extractTar(downloadPath)

    // Make executable on unix
    if (os.platform() !== 'win32') {
      await exec.exec('chmod', ['+x', path.join(extractedPath, 'parquedb')])
    }

    // Cache
    const cachedDir = await tc.cacheDir(extractedPath, 'parquedb', resolvedVersion)
    core.addPath(cachedDir)

    // Configure auth if token provided
    if (token) {
      await configureAuth(token)
    }

    core.setOutput('parquedb-version', resolvedVersion)
    core.info('ParqueDB CLI installed successfully')

  } catch (error) {
    if (error instanceof Error) {
      core.setFailed(error.message)
    } else {
      core.setFailed('An unexpected error occurred')
    }
  }
}

async function resolveVersion(version: string): Promise<string> {
  if (version === 'latest') {
    // Fetch latest release from GitHub API
    const response = await fetch(
      'https://api.github.com/repos/parquedb/parquedb/releases/latest'
    )
    const data = await response.json()
    return data.tag_name.replace(/^v/, '')
  }

  // Validate version format
  if (!/^\d+\.\d+\.\d+/.test(version)) {
    throw new Error(`Invalid version format: ${version}`)
  }

  return version
}

function getDownloadUrl(version: string): string {
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

async function configureAuth(token: string): Promise<void> {
  // Validate token
  const response = await fetch('https://parque.db/api/auth/validate', {
    headers: { 'Authorization': `Bearer ${token}` }
  })

  if (!response.ok) {
    throw new Error('Invalid ParqueDB token. Get a token from oauth.do')
  }

  // Set environment variable for CLI
  core.exportVariable('PARQUEDB_TOKEN', token)
  core.info('Authentication configured')
}

run()
