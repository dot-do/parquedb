/**
 * useKeyboardNavigation Hook
 *
 * Provides full keyboard navigation for lists and grids.
 * Supports arrow keys, Enter for selection, Escape for deselection,
 * Home/End for jumping to first/last item, and Tab for focus management.
 *
 * @example
 * ```typescript
 * const { focusedIndex, setFocusedIndex, handleKeyDown, containerRef } = useKeyboardNavigation({
 *   itemCount: databases.length,
 *   onSelect: (index) => navigateToDatabase(databases[index]),
 *   onEscape: () => setSearchQuery(''),
 *   columns: 3, // for grid navigation
 * })
 *
 * return (
 *   <div ref={containerRef} onKeyDown={handleKeyDown} tabIndex={0}>
 *     {databases.map((db, i) => (
 *       <div
 *         key={db.id}
 *         data-focused={i === focusedIndex}
 *         onMouseEnter={() => setFocusedIndex(i)}
 *       >
 *         {db.name}
 *       </div>
 *     ))}
 *   </div>
 * )
 * ```
 */

import { useState, useCallback, useRef, useEffect } from 'react'

/**
 * Configuration for keyboard navigation
 */
export interface UseKeyboardNavigationOptions {
  /** Total number of navigable items */
  itemCount: number
  /** Callback when an item is selected (Enter key) */
  onSelect?: ((index: number) => void) | undefined
  /** Callback when Escape is pressed */
  onEscape?: (() => void) | undefined
  /** Number of columns for grid navigation (default: 1 for list) */
  columns?: number | undefined
  /** Whether to wrap around at boundaries (default: true) */
  wrap?: boolean | undefined
  /** Whether navigation is enabled (default: true) */
  enabled?: boolean | undefined
  /** Initial focused index (default: -1, no focus) */
  initialIndex?: number | undefined
  /** Callback when focus changes */
  onFocusChange?: ((index: number) => void) | undefined
  /** Callback when Delete/Backspace is pressed on a focused item */
  onDelete?: ((index: number) => void) | undefined
}

/**
 * Return type for useKeyboardNavigation hook
 */
export interface UseKeyboardNavigationResult {
  /** Currently focused item index (-1 for none) */
  focusedIndex: number
  /** Set the focused index directly */
  setFocusedIndex: (index: number) => void
  /** Key down handler to attach to the container */
  handleKeyDown: (event: React.KeyboardEvent) => void
  /** Ref to attach to the container element for focus management */
  containerRef: React.RefObject<HTMLDivElement | null>
  /** Focus the container element */
  focusContainer: () => void
  /** Reset focus to no selection */
  resetFocus: () => void
}

/**
 * useKeyboardNavigation - Hook for keyboard navigation in lists and grids
 *
 * Supports:
 * - Arrow keys: Navigate between items
 * - Enter/Space: Select focused item
 * - Escape: Clear focus / trigger escape callback
 * - Home: Jump to first item
 * - End: Jump to last item
 * - Page Up/Down: Jump by page (10 items or one screen of columns)
 * - Delete/Backspace: Trigger delete callback on focused item
 */
export function useKeyboardNavigation({
  itemCount,
  onSelect,
  onEscape,
  columns = 1,
  wrap = true,
  enabled = true,
  initialIndex = -1,
  onFocusChange,
  onDelete,
}: UseKeyboardNavigationOptions): UseKeyboardNavigationResult {
  const [focusedIndex, setFocusedIndexState] = useState(initialIndex)
  const containerRef = useRef<HTMLDivElement | null>(null)

  // Clamp focused index when item count changes
  useEffect(() => {
    if (focusedIndex >= itemCount) {
      const newIndex = itemCount > 0 ? itemCount - 1 : -1
      setFocusedIndexState(newIndex)
      onFocusChange?.(newIndex)
    }
  }, [itemCount]) // eslint-disable-line react-hooks/exhaustive-deps

  const setFocusedIndex = useCallback((index: number) => {
    const clamped = index < 0 ? -1 : Math.min(index, itemCount - 1)
    setFocusedIndexState(clamped)
    onFocusChange?.(clamped)
  }, [itemCount, onFocusChange])

  const focusContainer = useCallback(() => {
    containerRef.current?.focus()
  }, [])

  const resetFocus = useCallback(() => {
    setFocusedIndexState(-1)
    onFocusChange?.(-1)
  }, [onFocusChange])

  const handleKeyDown = useCallback((event: React.KeyboardEvent) => {
    if (!enabled || itemCount === 0) return

    const { key } = event

    switch (key) {
      case 'ArrowDown': {
        event.preventDefault()
        if (focusedIndex === -1) {
          // No item focused, focus first
          setFocusedIndex(0)
        } else {
          const next = focusedIndex + columns
          if (next < itemCount) {
            setFocusedIndex(next)
          } else if (wrap) {
            // Wrap to top, same column position
            const col = focusedIndex % columns
            setFocusedIndex(col < itemCount ? col : 0)
          }
        }
        break
      }

      case 'ArrowUp': {
        event.preventDefault()
        if (focusedIndex === -1) {
          // No item focused, focus last
          setFocusedIndex(itemCount - 1)
        } else {
          const prev = focusedIndex - columns
          if (prev >= 0) {
            setFocusedIndex(prev)
          } else if (wrap) {
            // Wrap to bottom, same column position
            const col = focusedIndex % columns
            const lastRowStart = Math.floor((itemCount - 1) / columns) * columns
            const target = lastRowStart + col
            setFocusedIndex(target < itemCount ? target : itemCount - 1)
          }
        }
        break
      }

      case 'ArrowRight': {
        if (columns <= 1) return // Only for grids
        event.preventDefault()
        if (focusedIndex === -1) {
          setFocusedIndex(0)
        } else {
          const next = focusedIndex + 1
          if (next < itemCount) {
            setFocusedIndex(next)
          } else if (wrap) {
            setFocusedIndex(0)
          }
        }
        break
      }

      case 'ArrowLeft': {
        if (columns <= 1) return // Only for grids
        event.preventDefault()
        if (focusedIndex === -1) {
          setFocusedIndex(itemCount - 1)
        } else {
          const prev = focusedIndex - 1
          if (prev >= 0) {
            setFocusedIndex(prev)
          } else if (wrap) {
            setFocusedIndex(itemCount - 1)
          }
        }
        break
      }

      case 'Enter':
      case ' ': {
        if (focusedIndex >= 0 && focusedIndex < itemCount) {
          event.preventDefault()
          onSelect?.(focusedIndex)
        }
        break
      }

      case 'Escape': {
        event.preventDefault()
        if (focusedIndex >= 0) {
          resetFocus()
        }
        onEscape?.()
        break
      }

      case 'Home': {
        event.preventDefault()
        setFocusedIndex(0)
        break
      }

      case 'End': {
        event.preventDefault()
        setFocusedIndex(itemCount - 1)
        break
      }

      case 'PageDown': {
        event.preventDefault()
        const pageSize = Math.max(columns * 3, 10)
        const next = Math.min(focusedIndex + pageSize, itemCount - 1)
        setFocusedIndex(next)
        break
      }

      case 'PageUp': {
        event.preventDefault()
        const pageSize = Math.max(columns * 3, 10)
        const prev = Math.max(focusedIndex - pageSize, 0)
        setFocusedIndex(prev)
        break
      }

      case 'Delete':
      case 'Backspace': {
        if (focusedIndex >= 0 && focusedIndex < itemCount && onDelete) {
          event.preventDefault()
          onDelete(focusedIndex)
        }
        break
      }

      default:
        // Don't prevent default for other keys (allows typing in search, etc.)
        break
    }
  }, [enabled, itemCount, focusedIndex, columns, wrap, onSelect, onEscape, onDelete, setFocusedIndex, resetFocus])

  return {
    focusedIndex,
    setFocusedIndex,
    handleKeyDown,
    containerRef,
    focusContainer,
    resetFocus,
  }
}

export default useKeyboardNavigation
