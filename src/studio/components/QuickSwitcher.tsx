/**
 * QuickSwitcher Component
 *
 * A cmd+k / ctrl+k quick database switcher modal.
 * Provides fuzzy search with keyboard navigation for rapidly
 * switching between databases.
 *
 * Features:
 * - Global keyboard shortcut (Cmd+K / Ctrl+K) to open
 * - Fuzzy search by database name, description, or slug
 * - Full keyboard navigation (arrow keys, Enter to select, Escape to close)
 * - Recent databases shown first
 * - Visual keyboard shortcut hints
 *
 * @example
 * ```typescript
 * <QuickSwitcher
 *   databases={databases}
 *   basePath="/admin"
 *   onSelect={(db) => navigate(`/admin/${db.id}`)}
 * />
 * ```
 */

'use client'

import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import type { DatabaseInfo } from '../../worker/DatabaseIndexDO'
import { useKeyboardNavigation } from './hooks/useKeyboardNavigation'

export interface QuickSwitcherProps {
  /** Available databases to search through */
  databases: DatabaseInfo[]
  /** Base path for admin routes (default: '/admin') */
  basePath?: string
  /** Callback when a database is selected */
  onSelect?: (database: DatabaseInfo) => void
  /** Whether the quick switcher is currently open */
  isOpen?: boolean
  /** Callback when the quick switcher should close */
  onClose?: () => void
  /** Whether to register global keyboard shortcut (default: true) */
  registerShortcut?: boolean
  /** Currently active database ID (will be highlighted) */
  activeDatabaseId?: string
  /** Placeholder text for the search input */
  placeholder?: string
  /** Maximum number of results to display (default: 10) */
  maxResults?: number
}

/**
 * Fuzzy match score for a query against a string.
 * Returns -1 for no match, higher score = better match.
 */
function fuzzyScore(query: string, target: string): number {
  const queryLower = query.toLowerCase()
  const targetLower = target.toLowerCase()

  // Exact match gets highest score
  if (targetLower === queryLower) return 1000

  // Starts with query
  if (targetLower.startsWith(queryLower)) return 500 + (query.length / target.length) * 100

  // Contains query as substring
  const index = targetLower.indexOf(queryLower)
  if (index >= 0) return 200 + (query.length / target.length) * 100 - index

  // Character-by-character fuzzy match
  let qi = 0
  let score = 0
  let prevMatch = -2

  for (let ti = 0; ti < targetLower.length && qi < queryLower.length; ti++) {
    if (targetLower[ti] === queryLower[qi]) {
      // Consecutive matches score higher
      if (ti === prevMatch + 1) {
        score += 10
      }
      // Match at word boundary scores higher
      if (ti === 0 || target[ti - 1] === ' ' || target[ti - 1] === '-' || target[ti - 1] === '_') {
        score += 5
      }
      score += 1
      prevMatch = ti
      qi++
    }
  }

  // All query characters must match
  if (qi < queryLower.length) return -1

  return score
}

/**
 * Search and rank databases by relevance
 */
function searchDatabases(
  databases: DatabaseInfo[],
  query: string,
  maxResults: number
): DatabaseInfo[] {
  if (!query.trim()) {
    // No query: show recent databases first (by lastAccessedAt)
    return [...databases]
      .sort((a, b) => {
        const aTime = a.lastAccessedAt?.getTime() ?? 0
        const bTime = b.lastAccessedAt?.getTime() ?? 0
        return bTime - aTime
      })
      .slice(0, maxResults)
  }

  // Score each database
  const scored = databases
    .map((db) => {
      const nameScore = fuzzyScore(query, db.name)
      const slugScore = db.slug ? fuzzyScore(query, db.slug) : -1
      const descScore = db.description ? fuzzyScore(query, db.description) * 0.5 : -1
      const bestScore = Math.max(nameScore, slugScore, descScore)
      return { db, score: bestScore }
    })
    .filter(({ score }) => score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, maxResults)

  return scored.map(({ db }) => db)
}

/**
 * Format keyboard shortcut for display
 */
function getModifierKey(): string {
  if (typeof navigator === 'undefined') return 'Ctrl'
  return navigator.platform?.includes('Mac') || navigator.userAgent?.includes('Mac')
    ? 'Cmd' // macOS: Command key
    : 'Ctrl'
}

/**
 * QuickSwitcher - Cmd+K database switcher modal
 */
export function QuickSwitcher({
  databases,
  basePath = '/admin',
  onSelect,
  isOpen: controlledIsOpen,
  onClose: controlledOnClose,
  registerShortcut = true,
  activeDatabaseId,
  placeholder,
  maxResults = 10,
}: QuickSwitcherProps) {
  // Internal open state (used when not controlled)
  const [internalIsOpen, setInternalIsOpen] = useState(false)
  const [query, setQuery] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const modKey = useMemo(() => getModifierKey(), [])

  // Determine if controlled or uncontrolled
  const isOpen = controlledIsOpen !== undefined ? controlledIsOpen : internalIsOpen

  const handleClose = useCallback(() => {
    if (controlledOnClose) {
      controlledOnClose()
    } else {
      setInternalIsOpen(false)
    }
    setQuery('')
  }, [controlledOnClose])

  const handleOpen = useCallback(() => {
    if (controlledIsOpen !== undefined) {
      // Controlled: let parent handle
      return
    }
    setInternalIsOpen(true)
    setQuery('')
  }, [controlledIsOpen])

  // Filter databases based on query
  const filteredDatabases = useMemo(
    () => searchDatabases(databases, query, maxResults),
    [databases, query, maxResults]
  )

  // Keyboard navigation for the results list
  const {
    focusedIndex,
    setFocusedIndex,
    handleKeyDown: navKeyDown,
  } = useKeyboardNavigation({
    itemCount: filteredDatabases.length,
    onSelect: (index) => {
      const db = filteredDatabases[index]
      if (db) {
        handleSelectDatabase(db)
      }
    },
    onEscape: handleClose,
    enabled: isOpen,
  })

  // Reset focused index when query or results change
  useEffect(() => {
    if (filteredDatabases.length > 0) {
      setFocusedIndex(0)
    }
  }, [query, filteredDatabases.length, setFocusedIndex])

  // Handle database selection
  const handleSelectDatabase = useCallback((db: DatabaseInfo) => {
    if (onSelect) {
      onSelect(db)
    } else {
      // Default: navigate to the database
      window.location.href = `${basePath}/${db.id}`
    }
    handleClose()
  }, [onSelect, basePath, handleClose])

  // Register global keyboard shortcut (Cmd+K / Ctrl+K)
  useEffect(() => {
    if (!registerShortcut) return

    const handleGlobalKeyDown = (event: KeyboardEvent) => {
      // Cmd+K (Mac) or Ctrl+K (Windows/Linux)
      if ((event.metaKey || event.ctrlKey) && event.key === 'k') {
        event.preventDefault()
        event.stopPropagation()

        if (isOpen) {
          handleClose()
        } else {
          handleOpen()
        }
      }
    }

    document.addEventListener('keydown', handleGlobalKeyDown)
    return () => document.removeEventListener('keydown', handleGlobalKeyDown)
  }, [registerShortcut, isOpen, handleClose, handleOpen])

  // Focus input when modal opens
  useEffect(() => {
    if (isOpen) {
      // Small delay to ensure the input is rendered
      requestAnimationFrame(() => {
        inputRef.current?.focus()
      })
    }
  }, [isOpen])

  // Handle key events on the input
  const handleInputKeyDown = useCallback((event: React.KeyboardEvent) => {
    // Let navigation handler process arrow keys and Enter
    navKeyDown(event)
  }, [navKeyDown])

  if (!isOpen) {
    return null
  }

  return (
    <div
      className="quick-switcher-backdrop"
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(0, 0, 0, 0.5)',
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'center',
        paddingTop: '15vh',
        zIndex: 2000,
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget) {
          handleClose()
        }
      }}
      role="dialog"
      aria-modal="true"
      aria-label="Quick database switcher"
    >
      <div
        className="quick-switcher-content"
        style={{
          background: 'var(--theme-elevation-0, #fff)',
          borderRadius: '12px',
          width: '100%',
          maxWidth: '560px',
          maxHeight: '70vh',
          overflow: 'hidden',
          boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* Search input */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            padding: '0 1rem',
            borderBottom: '1px solid var(--theme-elevation-100, #e5e5e5)',
          }}
        >
          <svg
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            style={{
              color: 'var(--theme-elevation-400, #999)',
              flexShrink: 0,
            }}
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleInputKeyDown}
            placeholder={placeholder || 'Search databases...'}
            style={{
              flex: 1,
              padding: '1rem 0.75rem',
              border: 'none',
              outline: 'none',
              fontSize: '1rem',
              background: 'transparent',
              color: 'var(--theme-text, #333)',
            }}
            aria-label="Search databases"
            autoComplete="off"
            spellCheck={false}
          />
          <kbd
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              padding: '0.125rem 0.375rem',
              background: 'var(--theme-elevation-50, #f5f5f5)',
              border: '1px solid var(--theme-elevation-150, #ddd)',
              borderRadius: '4px',
              fontSize: '0.6875rem',
              fontFamily: 'inherit',
              color: 'var(--theme-elevation-500, #888)',
            }}
          >
            ESC
          </kbd>
        </div>

        {/* Results */}
        <div
          style={{
            overflow: 'auto',
            padding: '0.5rem 0',
          }}
          role="listbox"
          aria-label="Database results"
        >
          {filteredDatabases.length > 0 ? (
            filteredDatabases.map((db, index) => (
              <div
                key={db.id}
                role="option"
                aria-selected={index === focusedIndex}
                onClick={() => handleSelectDatabase(db)}
                onMouseEnter={() => setFocusedIndex(index)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.75rem',
                  padding: '0.625rem 1rem',
                  cursor: 'pointer',
                  background: index === focusedIndex
                    ? 'var(--theme-elevation-50, #f5f5f5)'
                    : 'transparent',
                  transition: 'background 0.1s',
                }}
              >
                {/* Database icon */}
                <div
                  style={{
                    width: '32px',
                    height: '32px',
                    borderRadius: '6px',
                    background: db.id === activeDatabaseId
                      ? 'var(--theme-elevation-800, #333)'
                      : 'var(--theme-elevation-100, #f0f0f0)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    flexShrink: 0,
                  }}
                >
                  <svg
                    width="16"
                    height="16"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke={db.id === activeDatabaseId ? 'var(--theme-elevation-0, #fff)' : 'var(--theme-elevation-500, #888)'}
                    strokeWidth="2"
                  >
                    <ellipse cx="12" cy="5" rx="9" ry="3" />
                    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
                  </svg>
                </div>

                {/* Database info */}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                  }}>
                    <span style={{
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      color: 'var(--theme-text, #333)',
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                    }}>
                      {db.name}
                    </span>
                    {db.id === activeDatabaseId && (
                      <span style={{
                        fontSize: '0.6875rem',
                        padding: '0.0625rem 0.375rem',
                        borderRadius: '3px',
                        background: 'var(--theme-success-100, #dcfce7)',
                        color: 'var(--theme-success-600, #16a34a)',
                        fontWeight: 500,
                        flexShrink: 0,
                      }}>
                        Active
                      </span>
                    )}
                  </div>
                  {(db.slug || db.description) && (
                    <span style={{
                      fontSize: '0.75rem',
                      color: 'var(--theme-elevation-500, #888)',
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      display: 'block',
                    }}>
                      {db.slug ? `/${db.slug}` : db.description}
                    </span>
                  )}
                </div>

                {/* Stats */}
                <div style={{
                  fontSize: '0.75rem',
                  color: 'var(--theme-elevation-400, #999)',
                  whiteSpace: 'nowrap',
                  flexShrink: 0,
                }}>
                  {db.entityCount !== undefined && db.entityCount > 0
                    ? `${db.entityCount.toLocaleString()} entities`
                    : db.visibility
                  }
                </div>

                {/* Enter hint on focused item */}
                {index === focusedIndex && (
                  <kbd
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      padding: '0.0625rem 0.25rem',
                      background: 'var(--theme-elevation-100, #f5f5f5)',
                      border: '1px solid var(--theme-elevation-200, #e5e5e5)',
                      borderRadius: '3px',
                      fontSize: '0.625rem',
                      fontFamily: 'inherit',
                      color: 'var(--theme-elevation-500, #888)',
                      flexShrink: 0,
                    }}
                  >
                    ENTER
                  </kbd>
                )}
              </div>
            ))
          ) : (
            <div
              style={{
                padding: '2rem 1rem',
                textAlign: 'center',
                color: 'var(--theme-elevation-500, #888)',
                fontSize: '0.875rem',
              }}
            >
              {query
                ? `No databases found matching "${query}"`
                : 'No databases available'
              }
            </div>
          )}
        </div>

        {/* Footer with keyboard hints */}
        <div
          style={{
            display: 'flex',
            gap: '1rem',
            padding: '0.5rem 1rem',
            borderTop: '1px solid var(--theme-elevation-100, #e5e5e5)',
            fontSize: '0.6875rem',
            color: 'var(--theme-elevation-400, #999)',
          }}
        >
          <span style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <kbd style={kbdStyle}>
              <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="18,15 12,9 6,15" />
              </svg>
            </kbd>
            <kbd style={kbdStyle}>
              <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="6,9 12,15 18,9" />
              </svg>
            </kbd>
            navigate
          </span>
          <span style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <kbd style={kbdStyle}>ENTER</kbd>
            select
          </span>
          <span style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <kbd style={kbdStyle}>ESC</kbd>
            close
          </span>
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <kbd style={kbdStyle}>{modKey}+K</kbd>
            toggle
          </span>
        </div>
      </div>
    </div>
  )
}

const kbdStyle: React.CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  padding: '0.0625rem 0.25rem',
  minWidth: '18px',
  background: 'var(--theme-elevation-50, #f5f5f5)',
  border: '1px solid var(--theme-elevation-200, #e5e5e5)',
  borderRadius: '3px',
  fontSize: '0.625rem',
  fontFamily: 'inherit',
  lineHeight: 1,
}

export default QuickSwitcher
