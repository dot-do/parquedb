/**
 * Authentication Commands
 *
 * Login, logout, and whoami commands using oauth.do
 */

import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'

/**
 * Login command - authenticate with oauth.do
 */
export async function loginCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Dynamic import to avoid loading oauth.do in non-auth contexts
    const { ensureLoggedIn, getUser } = await import('oauth.do/node')

    print('Authenticating with oauth.do...')
    print('')

    const { token, isNewLogin } = await ensureLoggedIn({
      openBrowser: true,
      print: (msg: string) => print(msg),
    })

    // Get user info with the token
    const authResult = await getUser(token)
    const user = authResult.user

    if (isNewLogin) {
      printSuccess(`Logged in as ${user?.email ?? 'unknown'}`)
    } else {
      print(`Already logged in as ${user?.email ?? 'unknown'}`)
    }

    if (parsed.options.pretty) {
      print('')
      print('User details:')
      print(JSON.stringify(user, null, 2))
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Login failed: ${message}`)
    return 1
  }
}

/**
 * Logout command - clear authentication tokens
 */
export async function logoutCommand(_parsed: ParsedArgs): Promise<number> {
  try {
    const { ensureLoggedOut } = await import('oauth.do/node')

    await ensureLoggedOut()
    printSuccess('Logged out successfully')

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Logout failed: ${message}`)
    return 1
  }
}

/**
 * Whoami command - show current user info
 */
export async function whoamiCommand(parsed: ParsedArgs): Promise<number> {
  try {
    const { getToken, getUser } = await import('oauth.do')

    const token = await getToken()
    if (!token) {
      print('Not logged in')
      print('')
      print('Run "parquedb login" to authenticate')
      return 1
    }

    const authResult = await getUser(token)
    const user = authResult.user
    if (!user) {
      print('Token exists but user info unavailable')
      return 1
    }

    if (parsed.options.pretty) {
      print(JSON.stringify(user, null, 2))
    } else {
      print(`Logged in as: ${user.email ?? user.id}`)
      // Check for extended properties via index signature
      const orgId = (user as Record<string, unknown>).organizationId
      if (orgId) {
        print(`Organization: ${orgId}`)
      }
      const roles = (user as Record<string, unknown>).roles as string[] | undefined
      if (roles && roles.length > 0) {
        print(`Roles: ${roles.join(', ')}`)
      }
    }

    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to get user info: ${message}`)
    return 1
  }
}

/**
 * Auth status command - check authentication status
 */
export async function authStatusCommand(parsed: ParsedArgs): Promise<number> {
  try {
    const { getToken, isAuthenticated } = await import('oauth.do')

    const token = await getToken()
    if (!token) {
      print('Status: Not authenticated')
      return 0
    }

    const authenticated = await isAuthenticated(token)

    if (parsed.options.pretty) {
      print(JSON.stringify({
        authenticated,
        hasToken: true,
      }, null, 2))
    } else {
      print(`Status: ${authenticated ? 'Authenticated' : 'Token invalid/expired'}`)
    }

    return authenticated ? 0 : 1
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Failed to check status: ${message}`)
    return 1
  }
}
