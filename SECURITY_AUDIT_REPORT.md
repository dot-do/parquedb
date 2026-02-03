# ParqueDB Security Audit Report

**Date:** 2026-02-03
**Auditor:** Claude Code (Security Review)
**Scope:** Authentication, Authorization, Input Validation, Injection Prevention, Path Traversal, Token Handling, CSRF Protection

---

## Executive Summary

ParqueDB demonstrates **strong security posture** across most reviewed areas. The codebase implements defense-in-depth strategies with multiple layers of protection for common web application vulnerabilities. A few areas warrant attention for improvement.

### Overall Risk Assessment: **LOW to MEDIUM**

| Area | Status | Risk Level |
|------|--------|------------|
| Authentication/Authorization | Strong | Low |
| Input Validation | Strong | Low |
| SQL/NoSQL Injection | Strong | Low |
| Prototype Pollution | Strong | Low |
| Path Traversal | Strong | Low |
| Token Handling | Strong | Low |
| CSRF Protection | Strong | Low |
| Rate Limiting | Present | Low |
| XSS Prevention | Strong | Low |

---

## 1. Authentication/Authorization

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/integrations/payload/auth.ts`
- `/Users/nathanclevenger/projects/parquedb/src/worker/jwt-utils.ts`
- `/Users/nathanclevenger/projects/parquedb/src/integrations/hono/auth.ts`

### Findings:

#### Strengths:
1. **JWT Verification with JWKS**: Uses `jose` library for cryptographic JWT verification against JWKS endpoints
2. **Timeout Protection**: Implements `withAbortTimeout` to prevent hung JWKS fetch connections (JWKSFetchTimeoutError)
3. **Clock Tolerance**: 60-second clock skew tolerance for JWT verification prevents timing issues
4. **Role-based Access Control**: Implements proper RBAC with admin/editor/user roles
5. **JWKS Caching**: Caches JWKS with configurable TTL to avoid fetching on every request
6. **User ID Validation**: Requires valid `sub` claim in tokens, rejects tokens without user ID

#### Potential Improvements:
1. **Audience Validation**: The `clientId` (audience) check is optional. Consider making it mandatory for production deployments.
2. **Issuer Validation**: No explicit issuer validation in JWT verification options.

### Recommendation:
Consider adding issuer validation to the JWT verification options:
```typescript
if (config.issuer) {
  verifyOptions.issuer = config.issuer
}
```

---

## 2. Input Validation

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/storage/validation.ts`
- `/Users/nathanclevenger/projects/parquedb/src/integrations/mcp/validation.ts`

### Findings:

#### Strengths:
1. **Comprehensive Validation Functions**: Validates collection names, entity IDs, filters, updates, pipelines, pagination parameters
2. **Prototype Pollution Protection**: Blocks dangerous keys (`__proto__`, `constructor`, `prototype`, `__defineGetter__`, etc.)
3. **Operator Allowlisting**: Only allows known filter operators (`$eq`, `$gt`, etc.) and update operators (`$set`, `$inc`, etc.)
4. **Nesting Depth Limits**: MAX_NESTING_DEPTH = 10 prevents stack overflow from deeply nested objects
5. **String Length Limits**: MAX_MCP_STRING_LENGTH prevents memory exhaustion
6. **Path Traversal Prevention in Entity IDs**: Rejects `..`, `/`, `\\` in entity IDs
7. **Pattern Validation**: Collection names must match `/^[a-zA-Z][a-zA-Z0-9_]*$/`

#### Code Quality:
The `sanitizeObject` function properly uses `Object.create(null)` to create prototype-free objects:
```typescript
const result: Record<string, unknown> = Object.create(null)
```

### Recommendation:
None - validation is comprehensive and well-implemented.

---

## 3. SQL/NoSQL Injection Prevention

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/utils/sql-security.ts`
- `/Users/nathanclevenger/projects/parquedb/src/integrations/sql/translator.ts`
- `/Users/nathanclevenger/projects/parquedb/src/storage/DOSqliteBackend.ts`

### Findings:

#### Strengths:
1. **Parameterized Queries**: All DOSqliteBackend queries use parameterized statements via `.prepare().bind()`
2. **LIKE Pattern Escaping**: `escapeLikePattern()` escapes `%`, `_`, and `\` to prevent wildcard injection
3. **Table Name Validation**: `isValidTableName()` validates identifiers against safe patterns and blocks SQL keywords
4. **WHERE Clause Validation**: `validateWhereClause()` detects and blocks:
   - Multiple statement attempts (`;SELECT`, etc.)
   - SQL comments (`--`, `/*`)
   - UNION injection
   - Dangerous keywords (DROP, TRUNCATE, ALTER, etc.)
   - Excessive nesting (max 10 parentheses depth)

#### Example of Safe Query Pattern (DOSqliteBackend):
```typescript
const row = this.sql
  .prepare('SELECT data FROM parquet_blocks WHERE path = ?')
  .bind(key)
  .first<Pick<ParquetBlockRow, 'data'>>()
```

### Recommendation:
None - SQL security is properly implemented with parameterized queries throughout.

---

## 4. Prototype Pollution Protection

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/utils/path-safety.ts`
- `/Users/nathanclevenger/projects/parquedb/src/integrations/mcp/validation.ts`

### Findings:

#### Strengths:
1. **UNSAFE_PATH_SEGMENTS Set**: Defines dangerous segments (`__proto__`, `constructor`, `prototype`)
2. **Path Validation**: `isUnsafePath()` checks dot-notation paths for dangerous segments
3. **Key Validation**: Both shallow (`validateObjectKeys`) and deep (`validateObjectKeysDeep`) validation
4. **Sanitization**: `sanitizeObject()` removes dangerous keys while preserving safe content
5. **MCP Validation**: Recursive `sanitizeObject()` in MCP validation with depth limits

### Recommendation:
None - prototype pollution protection is comprehensive.

---

## 5. Path Traversal Protection

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/storage/RemoteBackend.ts`
- `/Users/nathanclevenger/projects/parquedb/src/storage/FsBackend.ts`
- `/Users/nathanclevenger/projects/parquedb/src/worker/sync-routes.ts`
- `/Users/nathanclevenger/projects/parquedb/tests/unit/storage/RemoteBackend-pathTraversal.test.ts`

### Findings:

#### Strengths:

**RemoteBackend:**
- Rejects paths containing `..` (parent directory traversal)
- Rejects paths with `//` (double slash bypass attempts)
- Rejects absolute paths starting with `/`
- Throws `PathTraversalError` for violations
- Has comprehensive test coverage

**FsBackend:**
- Checks for null bytes (`\x00`)
- URL-decodes and checks for `..` traversal
- Uses `resolve()` to get absolute path, then verifies it starts with root path
- Critical security check: `if (!fullPath.startsWith(this.resolvedRootPath + '/') && fullPath !== this.resolvedRootPath)`

**Sync Routes:**
- `validateUrlParameter()`: Checks for dangerous chars (null byte, line breaks)
- `hasEncodedDangerousChars()`: Detects URL-encoded dangerous patterns
- `hasPathTraversal()`: Detects traversal in both raw and decoded values
- `fullyDecode()`: Recursively decodes up to 3 levels to catch double/triple encoding
- `validateDatabaseId()`: Allows only alphanumeric, hyphens, underscores
- `validateFilePath()`: Ensures relative paths, validates each segment

### Recommendation:
None - path traversal protection is thorough with multiple defense layers.

---

## 6. Token Handling

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/worker/sync-token.ts`

### Findings:

#### Strengths:
1. **HMAC-SHA256 Signing**: Tokens are signed with HMAC-SHA256 using `SYNC_SECRET`
2. **Token Type Field**: Separates upload/download tokens to prevent confusion attacks
3. **Expiration Checking**: Tokens have `expiresAt` field with 5-second clock skew tolerance
4. **Replay Protection**:
   - Single-use nonces (jti) for upload tokens
   - In-memory tracking with cleanup
   - Optional KV-based cross-isolate tracking via `USED_TOKENS` binding
5. **Base64url Encoding**: URL-safe encoding prevents issues in URLs
6. **Secret Validation**: Throws error if `SYNC_SECRET` is not configured

#### Nonce Cache Management:
- MAX_NONCE_CACHE_SIZE with forced cleanup
- Automatic cleanup of expired nonces
- Falls back to in-memory when KV unavailable

### Recommendation:
None - token handling is secure with proper replay protection.

---

## 7. CSRF Protection

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/security/csrf.ts`

### Findings:

#### Strengths:
1. **Multiple Defense Layers**:
   - Origin/Referer header validation
   - Custom header requirement (X-Requested-With)
   - Token-based protection for forms
2. **Safe Methods Bypass**: GET, HEAD, OPTIONS skip CSRF validation
3. **Same-Origin Default**: Without explicit allowed origins, only same-origin is permitted
4. **Flexible Configuration**: Supports custom headers, excluded paths, error handlers
5. **Token-Based CSRF**: HMAC-SHA256 signed tokens for form submissions
6. **CORS Integration**: `buildSecureCorsHeaders()` ensures CORS doesn't bypass CSRF

#### Token Features:
- Subject binding (user/session ID)
- Nonce for uniqueness
- Configurable TTL (default 1 hour)

### Recommendation:
None - CSRF protection is comprehensive and well-designed.

---

## 8. Rate Limiting

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/worker/RateLimitDO.ts`

### Findings:

#### Strengths:
1. **Sliding Window Algorithm**: More accurate than fixed window
2. **Durable Object Storage**: Consistent state across edge locations
3. **Per-Endpoint Configuration**: Different limits for public, database, query, file endpoints
4. **Status Headers**: Returns remaining requests and reset time
5. **SQLite-Based**: Efficient request tracking with indexed timestamps

### Recommendation:
None - rate limiting implementation is solid.

---

## 9. XSS Prevention

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/indexes/fts/highlight.ts`
- `/Users/nathanclevenger/projects/parquedb/tests/unit/studio/xss-prevention.test.ts`

### Findings:

#### Strengths:
1. **HTML Escaping by Default**: `escapeHtml: true` is default in highlighting functions
2. **Comprehensive Escaping**: Escapes `&`, `<`, `>`, `"`, `'`
3. **Test Coverage**: XSS prevention tests verify escaping in database name, description, IDs
4. **Studio Components**: HTML generation functions properly escape user content

### Recommendation:
None - XSS prevention is properly implemented.

---

## 10. Timing Attack Prevention

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/integrations/mcp/auth.ts`
- `/Users/nathanclevenger/projects/parquedb/src/worker/github/handlers.ts`

### Findings:

#### Strengths:
1. **Constant-Time Comparison**: `constantTimeCompare()` and `timingSafeEqual()` implementations
2. **SHA256 Hashing Option**: API keys can be compared via hashed values for additional security
3. **Webhook Signature Verification**: GitHub webhook signatures use timing-safe comparison
4. **Sync Token Verification**: Uses `crypto.subtle.verify` which is inherently constant-time

### Recommendation:
None - timing attack prevention is implemented where needed.

---

## 11. Code Execution Safety

### Files Reviewed:
- `/Users/nathanclevenger/projects/parquedb/src/client/rpc-promise.ts`
- `/Users/nathanclevenger/projects/parquedb/src/embeddings/ai-sdk.ts`

### Findings:

#### Strengths (RPC Promise):
1. **No eval/Function for User Code**: `deserializeFunction()` only supports:
   - `path`: Safe property traversal
   - `registered`: Lookup of pre-registered server-side mappers
2. **Rejects Legacy Format**: Old `{ type: 'sync', body: '...' }` format (which used `new Function()`) is rejected
3. **Property Path Extraction**: `extractPropertyPath()` only allows simple property access

#### Dynamic Import Usage:
- `ai-sdk.ts` and `iceberg-native.ts` use `new Function('specifier', 'return import(specifier)')` for dynamic imports
- This is controlled internal code, not user input

### Recommendation:
The dynamic import pattern is acceptable for internal use but should be documented as a known pattern.

---

## Security Testing Coverage

### Verified Test Files:
- `tests/unit/storage/RemoteBackend-pathTraversal.test.ts` - Path traversal tests
- `tests/unit/studio/xss-prevention.test.ts` - XSS prevention tests
- `tests/unit/worker/webhook.test.ts` - Timing-safe signature verification
- `tests/unit/worker/sync-routes-token.test.ts` - Token verification tests
- `tests/unit/worker/RateLimitDO.test.ts` - Rate limiting tests

---

## Summary of Recommendations

### No Critical Issues Found

### Minor Improvements (Optional):
1. **JWT Issuer Validation**: Consider adding optional issuer validation to JWT verification
2. **Documentation**: Document the dynamic import pattern in ai-sdk.ts and iceberg-native.ts
3. **Monitoring**: Consider adding security event logging for:
   - Failed authentication attempts
   - Path traversal attempts
   - Rate limit exceeded events

---

## Conclusion

ParqueDB demonstrates mature security practices with:
- Defense-in-depth approach
- Proper use of cryptographic libraries (jose, Web Crypto API)
- Comprehensive input validation and sanitization
- Strong protection against OWASP Top 10 vulnerabilities
- Good test coverage for security-critical code paths

The codebase is well-structured for security maintainability and follows established best practices.
