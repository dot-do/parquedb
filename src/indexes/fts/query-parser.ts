/**
 * Query Parser for Full-Text Search
 *
 * Parses FTS query strings into structured query objects.
 * Supports:
 * - Phrase queries: "exact phrase"
 * - Required terms: +term (must contain)
 * - Excluded terms: -term (must not contain)
 * - Regular terms: term (should contain, for ranking)
 *
 * @example
 * // Simple query
 * parseQuery("database programming")
 * // => { terms: [{ term: "database", required: false, excluded: false }, ...] }
 *
 * @example
 * // Phrase query
 * parseQuery('"exact phrase" other')
 * // => { phrases: ["exact phrase"], terms: [{ term: "other", ... }] }
 *
 * @example
 * // Boolean query
 * parseQuery("+required -excluded optional")
 * // => { terms: [{ term: "required", required: true }, { term: "excluded", excluded: true }, ...] }
 */

/**
 * Parsed term with modifiers
 */
export interface ParsedTerm {
  /** The term text (normalized) */
  term: string
  /** Whether this term is required (+) */
  required: boolean
  /** Whether this term is excluded (-) */
  excluded: boolean
}

/**
 * Parsed query structure
 */
export interface ParsedQuery {
  /** Individual terms with modifiers */
  terms: ParsedTerm[]
  /** Phrase queries (exact sequences) */
  phrases: string[]
  /** Required phrases (+") */
  requiredPhrases: string[]
  /** Excluded phrases (-") */
  excludedPhrases: string[]
}

/**
 * Parse an FTS query string into structured components
 *
 * @param query - Raw query string
 * @returns Parsed query structure
 */
export function parseQuery(query: string): ParsedQuery {
  const result: ParsedQuery = {
    terms: [],
    phrases: [],
    requiredPhrases: [],
    excludedPhrases: [],
  }

  if (!query || query.trim().length === 0) {
    return result
  }

  // Track position in the string
  let pos = 0
  const len = query.length

  while (pos < len) {
    // Skip whitespace
    while (pos < len && /\s/.test(query[pos]!)) {
      pos++
    }

    if (pos >= len) break

    const char = query[pos]!

    // Check for phrase (starts with quote, optionally preceded by + or -)
    if (char === '"' || ((char === '+' || char === '-') && pos + 1 < len && query[pos + 1] === '"')) {
      const modifier = char === '+' || char === '-' ? char : null
      const phraseStart = modifier ? pos + 2 : pos + 1
      pos = phraseStart

      // Find closing quote
      let phraseEnd = query.indexOf('"', pos)
      if (phraseEnd === -1) {
        // No closing quote, treat rest as phrase
        phraseEnd = len
      }

      const phrase = query.slice(pos, phraseEnd).trim()
      if (phrase.length > 0) {
        if (modifier === '+') {
          result.requiredPhrases.push(phrase)
        } else if (modifier === '-') {
          result.excludedPhrases.push(phrase)
        } else {
          result.phrases.push(phrase)
        }
      }

      pos = phraseEnd + 1
      continue
    }

    // Check for term with modifier (+term or -term)
    let modifier: '+' | '-' | null = null
    if ((char === '+' || char === '-') && pos + 1 < len && !/\s/.test(query[pos + 1]!)) {
      modifier = char
      pos++
    }

    // Read term until whitespace or quote
    const termStart = pos
    while (pos < len && !/[\s"]/.test(query[pos]!)) {
      pos++
    }

    const term = query.slice(termStart, pos).trim().toLowerCase()
    if (term.length > 0) {
      result.terms.push({
        term,
        required: modifier === '+',
        excluded: modifier === '-',
      })
    }
  }

  return result
}

/**
 * Check if a query has any boolean operators or phrase queries
 *
 * @param query - Raw query string
 * @returns true if the query uses advanced features
 */
export function isAdvancedQuery(query: string): boolean {
  // Check for phrases
  if (query.includes('"')) return true

  // Check for boolean operators
  if (/(?:^|\s)[+-]\S/.test(query)) return true

  return false
}

/**
 * Convert a ParsedQuery back to a simple term list (for basic search)
 *
 * @param parsed - Parsed query
 * @returns List of non-excluded terms
 */
export function getSearchTerms(parsed: ParsedQuery): string[] {
  return parsed.terms
    .filter(t => !t.excluded)
    .map(t => t.term)
}

/**
 * Get all required terms from a parsed query
 *
 * @param parsed - Parsed query
 * @returns List of required terms
 */
export function getRequiredTerms(parsed: ParsedQuery): string[] {
  return parsed.terms
    .filter(t => t.required)
    .map(t => t.term)
}

/**
 * Get all excluded terms from a parsed query
 *
 * @param parsed - Parsed query
 * @returns List of excluded terms
 */
export function getExcludedTerms(parsed: ParsedQuery): string[] {
  return parsed.terms
    .filter(t => t.excluded)
    .map(t => t.term)
}

// =============================================================================
// Boolean Query Types
// =============================================================================

/**
 * Boolean query clause
 */
export interface BooleanClause {
  /** Terms in this clause (stemmed) */
  terms: string[]
  /** Phrase to match exactly (original, unstemmed) */
  phrase?: string
  /** Whether this clause is required (AND semantics) */
  required: boolean
  /** Whether this clause is excluded (NOT semantics) */
  excluded: boolean
}

/**
 * Boolean query structure
 */
export interface BooleanQuery {
  /** Type of boolean operation at top level */
  type: 'and' | 'or'
  /** Clauses to evaluate */
  clauses: BooleanClause[]
}

// =============================================================================
// Boolean Query Parser
// =============================================================================

/**
 * Check if a query contains boolean operators (AND, OR, NOT, parentheses)
 *
 * @param query - Raw query string
 * @returns true if the query has boolean operators
 */
export function isBooleanQuery(query: string): boolean {
  if (!query) return false

  // Check for explicit boolean operators (case insensitive)
  if (/\b(AND|OR|NOT)\b/i.test(query)) return true

  // Check for parentheses grouping
  if (query.includes('(') || query.includes(')')) return true

  // Check for +/- modifiers
  if (/(?:^|\s)[+-]\S/.test(query)) return true

  return false
}

/**
 * Tokenize query for boolean parsing
 */
interface BooleanToken {
  type: 'term' | 'phrase' | 'and' | 'or' | 'not' | 'lparen' | 'rparen' | 'plus' | 'minus'
  value: string
}

function tokenizeBooleanQuery(query: string): BooleanToken[] {
  const tokens: BooleanToken[] = []
  let pos = 0
  const len = query.length

  while (pos < len) {
    // Skip whitespace
    while (pos < len && /\s/.test(query[pos]!)) {
      pos++
    }

    if (pos >= len) break

    const char = query[pos]!

    // Check for parentheses
    if (char === '(') {
      tokens.push({ type: 'lparen', value: '(' })
      pos++
      continue
    }
    if (char === ')') {
      tokens.push({ type: 'rparen', value: ')' })
      pos++
      continue
    }

    // Check for phrase
    if (char === '"') {
      pos++ // skip opening quote
      const phraseStart = pos
      while (pos < len && query[pos] !== '"') {
        pos++
      }
      const phrase = query.slice(phraseStart, pos)
      if (phrase.length > 0) {
        tokens.push({ type: 'phrase', value: phrase })
      }
      if (pos < len) pos++ // skip closing quote
      continue
    }

    // Check for + or - modifiers (not followed by space)
    if ((char === '+' || char === '-') && pos + 1 < len && !/\s/.test(query[pos + 1]!)) {
      // Check if next is a phrase
      if (query[pos + 1] === '"') {
        tokens.push({ type: char === '+' ? 'plus' : 'minus', value: char })
        pos++
        continue
      }
      // Otherwise it's a modifier for a term
      tokens.push({ type: char === '+' ? 'plus' : 'minus', value: char })
      pos++
      continue
    }

    // Read word
    const wordStart = pos
    while (pos < len && !/[\s()"+-]/.test(query[pos]!) || (pos === wordStart && (query[pos] === '+' || query[pos] === '-'))) {
      if (pos > wordStart && (query[pos] === '+' || query[pos] === '-')) break
      pos++
    }

    const word = query.slice(wordStart, pos)
    if (word.length > 0) {
      const upperWord = word.toUpperCase()
      if (upperWord === 'AND') {
        tokens.push({ type: 'and', value: 'AND' })
      } else if (upperWord === 'OR') {
        tokens.push({ type: 'or', value: 'OR' })
      } else if (upperWord === 'NOT') {
        tokens.push({ type: 'not', value: 'NOT' })
      } else {
        tokens.push({ type: 'term', value: word.toLowerCase() })
      }
    }
  }

  return tokens
}

import { porterStem } from './tokenizer'

/**
 * Stem terms for matching
 */
function stemTerms(terms: string[]): string[] {
  return terms.map(t => porterStem(t))
}

/**
 * Parse a boolean query string into structured form
 *
 * Supports:
 * - AND: "word1 AND word2" - both must match
 * - OR: "word1 OR word2" - either can match
 * - NOT: "word1 NOT word2" or "-word2" - exclude matches
 * - Parentheses: "(word1 OR word2) AND word3"
 * - Phrases: '"exact phrase" AND word'
 * - Required: "+word" - must contain
 *
 * Operator precedence: NOT > AND > OR
 *
 * @param query - Raw query string
 * @returns Parsed boolean query structure
 */
export function parseBooleanQuery(query: string): BooleanQuery {
  const result: BooleanQuery = {
    type: 'or', // Default to OR for ranking
    clauses: [],
  }

  if (!query || query.trim().length === 0) {
    return result
  }

  const tokens = tokenizeBooleanQuery(query)

  if (tokens.length === 0) {
    return result
  }

  // Determine the top-level operator
  // If we have explicit AND operators (without OR at top level), use AND
  // If we have explicit OR operators, use OR
  // If we have mixed, OR has lower precedence so it's the top-level
  let hasAnd = false
  let hasOr = false
  let parenDepth = 0

  for (const token of tokens) {
    if (token.type === 'lparen') parenDepth++
    else if (token.type === 'rparen') parenDepth--
    else if (parenDepth === 0) {
      if (token.type === 'and') hasAnd = true
      if (token.type === 'or') hasOr = true
    }
  }

  // If we have OR at top level, split by OR first
  if (hasOr) {
    result.type = 'or'
    const orGroups = splitByOperator(tokens, 'or')
    for (const group of orGroups) {
      const subQuery = parseTokenGroup(group)
      if (subQuery.clauses.length > 0) {
        // Flatten AND clauses into the OR
        if (subQuery.type === 'and') {
          // Create a single clause that represents the AND group
          const andClause: BooleanClause = {
            terms: [],
            required: true,
            excluded: false,
          }
          // For AND groups within OR, we'll handle this specially
          // by adding clauses with the sub-query info
          for (const clause of subQuery.clauses) {
            if (clause.excluded) {
              // Excluded clause within an AND group
              result.clauses.push(clause)
            } else {
              // Merge terms
              andClause.terms.push(...clause.terms)
              if (clause.phrase) {
                // Phrase becomes a separate required clause
                result.clauses.push({
                  terms: [],
                  phrase: clause.phrase,
                  required: true,
                  excluded: false,
                })
              }
            }
          }
          if (andClause.terms.length > 0) {
            result.clauses.push(andClause)
          }
        } else {
          // Just add the clauses
          result.clauses.push(...subQuery.clauses)
        }
      }
    }
  } else if (hasAnd) {
    result.type = 'and'
    const andGroups = splitByOperator(tokens, 'and')
    for (const group of andGroups) {
      const { mainClause, excludedClauses } = parseTokenGroupAsClauses(group)
      if (mainClause) {
        result.clauses.push(mainClause)
      }
      // Add excluded clauses separately
      result.clauses.push(...excludedClauses)
    }
  } else {
    // No explicit operators - default to OR for multi-term queries
    // But if there are +/- modifiers, handle those
    result.type = 'or'
    let i = 0
    while (i < tokens.length) {
      const token = tokens[i]!

      if (token.type === 'plus') {
        i++
        if (i < tokens.length) {
          const nextToken = tokens[i]!
          if (nextToken.type === 'term') {
            result.clauses.push({
              terms: stemTerms([nextToken.value]),
              required: true,
              excluded: false,
            })
          } else if (nextToken.type === 'phrase') {
            result.clauses.push({
              terms: [],
              phrase: nextToken.value,
              required: true,
              excluded: false,
            })
          }
          i++
        }
      } else if (token.type === 'minus' || token.type === 'not') {
        i++
        if (i < tokens.length) {
          const nextToken = tokens[i]!
          if (nextToken.type === 'term') {
            result.clauses.push({
              terms: stemTerms([nextToken.value]),
              required: false,
              excluded: true,
            })
          } else if (nextToken.type === 'phrase') {
            result.clauses.push({
              terms: [],
              phrase: nextToken.value,
              required: false,
              excluded: true,
            })
          }
          i++
        }
      } else if (token.type === 'term') {
        result.clauses.push({
          terms: stemTerms([token.value]),
          required: false,
          excluded: false,
        })
        i++
      } else if (token.type === 'phrase') {
        result.clauses.push({
          terms: [],
          phrase: token.value,
          required: false,
          excluded: false,
        })
        i++
      } else if (token.type === 'lparen') {
        // Find matching rparen
        const parenGroup = extractParenGroup(tokens, i)
        const subQuery = parseTokenGroup(parenGroup.tokens)
        // Add clauses from paren group
        for (const clause of subQuery.clauses) {
          result.clauses.push(clause)
        }
        i = parenGroup.endIndex + 1
      } else {
        i++
      }
    }
  }

  return result
}

/**
 * Split tokens by a specific operator at the top level (not inside parens)
 */
function splitByOperator(tokens: BooleanToken[], op: 'and' | 'or'): BooleanToken[][] {
  const groups: BooleanToken[][] = []
  let currentGroup: BooleanToken[] = []
  let parenDepth = 0

  for (const token of tokens) {
    if (token.type === 'lparen') {
      parenDepth++
      currentGroup.push(token)
    } else if (token.type === 'rparen') {
      parenDepth--
      currentGroup.push(token)
    } else if (parenDepth === 0 && token.type === op) {
      if (currentGroup.length > 0) {
        groups.push(currentGroup)
        currentGroup = []
      }
    } else {
      currentGroup.push(token)
    }
  }

  if (currentGroup.length > 0) {
    groups.push(currentGroup)
  }

  return groups
}

/**
 * Parse a token group into a BooleanQuery
 */
function parseTokenGroup(tokens: BooleanToken[]): BooleanQuery {
  // Check for operators within this group
  let hasAnd = false
  let hasOr = false
  let parenDepth = 0

  for (const token of tokens) {
    if (token.type === 'lparen') parenDepth++
    else if (token.type === 'rparen') parenDepth--
    else if (parenDepth === 0) {
      if (token.type === 'and') hasAnd = true
      if (token.type === 'or') hasOr = true
    }
  }

  if (hasOr) {
    const result: BooleanQuery = { type: 'or', clauses: [] }
    const orGroups = splitByOperator(tokens, 'or')
    for (const group of orGroups) {
      const subQuery = parseTokenGroup(group)
      result.clauses.push(...subQuery.clauses)
    }
    return result
  } else if (hasAnd) {
    const result: BooleanQuery = { type: 'and', clauses: [] }
    const andGroups = splitByOperator(tokens, 'and')
    for (const group of andGroups) {
      const clause = parseTokenGroupAsClause(group)
      if (clause) {
        result.clauses.push(clause)
      }
    }
    return result
  } else {
    // No operators - just terms
    const result: BooleanQuery = { type: 'or', clauses: [] }
    let i = 0
    while (i < tokens.length) {
      const token = tokens[i]!
      if (token.type === 'term') {
        result.clauses.push({
          terms: stemTerms([token.value]),
          required: false,
          excluded: false,
        })
      } else if (token.type === 'phrase') {
        result.clauses.push({
          terms: [],
          phrase: token.value,
          required: false,
          excluded: false,
        })
      } else if (token.type === 'plus') {
        i++
        if (i < tokens.length) {
          const next = tokens[i]!
          if (next.type === 'term') {
            result.clauses.push({
              terms: stemTerms([next.value]),
              required: true,
              excluded: false,
            })
          } else if (next.type === 'phrase') {
            result.clauses.push({
              terms: [],
              phrase: next.value,
              required: true,
              excluded: false,
            })
          }
        }
      } else if (token.type === 'minus' || token.type === 'not') {
        i++
        if (i < tokens.length) {
          const next = tokens[i]!
          if (next.type === 'term') {
            result.clauses.push({
              terms: stemTerms([next.value]),
              required: false,
              excluded: true,
            })
          } else if (next.type === 'phrase') {
            result.clauses.push({
              terms: [],
              phrase: next.value,
              required: false,
              excluded: true,
            })
          }
        }
      } else if (token.type === 'lparen') {
        const parenGroup = extractParenGroup(tokens, i)
        const subQuery = parseTokenGroup(parenGroup.tokens)
        result.clauses.push(...subQuery.clauses)
        i = parenGroup.endIndex
      }
      i++
    }
    return result
  }
}

/**
 * Parse a token group into a single clause
 */
/**
 * Result from parsing a token group - may produce multiple clauses
 * if there are mixed included/excluded terms
 */
interface ParsedClauses {
  /** Main clause (non-excluded terms) */
  mainClause: BooleanClause | null
  /** Excluded clauses (each excluded term becomes its own clause) */
  excludedClauses: BooleanClause[]
}

/**
 * Parse a token group into clauses, separating excluded terms
 */
function parseTokenGroupAsClauses(tokens: BooleanToken[]): ParsedClauses {
  const terms: string[] = []
  let phrase: string | undefined
  let required = false
  const excludedClauses: BooleanClause[] = []

  let i = 0
  while (i < tokens.length) {
    const token = tokens[i]!

    if (token.type === 'term') {
      terms.push(...stemTerms([token.value]))
    } else if (token.type === 'phrase') {
      phrase = token.value
    } else if (token.type === 'plus') {
      required = true
      i++
      if (i < tokens.length) {
        const next = tokens[i]!
        if (next.type === 'term') {
          terms.push(...stemTerms([next.value]))
        } else if (next.type === 'phrase') {
          phrase = next.value
        }
      }
    } else if (token.type === 'minus' || token.type === 'not') {
      // Excluded terms become separate clauses
      i++
      if (i < tokens.length) {
        const next = tokens[i]!
        if (next.type === 'term') {
          excludedClauses.push({
            terms: stemTerms([next.value]),
            required: false,
            excluded: true,
          })
        } else if (next.type === 'phrase') {
          excludedClauses.push({
            terms: [],
            phrase: next.value,
            required: false,
            excluded: true,
          })
        }
      }
    } else if (token.type === 'lparen') {
      const parenGroup = extractParenGroup(tokens, i)
      const subQuery = parseTokenGroup(parenGroup.tokens)
      for (const clause of subQuery.clauses) {
        if (clause.excluded) {
          excludedClauses.push(clause)
        } else {
          terms.push(...clause.terms)
          if (clause.phrase) phrase = clause.phrase
          if (clause.required) required = true
        }
      }
      i = parenGroup.endIndex
    }

    i++
  }

  const mainClause: BooleanClause | null = (terms.length > 0 || phrase)
    ? { terms, phrase, required, excluded: false }
    : null

  return { mainClause, excludedClauses }
}

/**
 * Parse a token group into a single clause (legacy function for compatibility)
 */
function parseTokenGroupAsClause(tokens: BooleanToken[]): BooleanClause | null {
  const { mainClause } = parseTokenGroupAsClauses(tokens)
  return mainClause
}

/**
 * Extract tokens within parentheses
 */
function extractParenGroup(tokens: BooleanToken[], startIndex: number): { tokens: BooleanToken[]; endIndex: number } {
  const result: BooleanToken[] = []
  let depth = 0
  let i = startIndex

  while (i < tokens.length) {
    const token = tokens[i]!
    if (token.type === 'lparen') {
      depth++
      if (depth > 1) result.push(token)
    } else if (token.type === 'rparen') {
      depth--
      if (depth === 0) {
        return { tokens: result, endIndex: i }
      }
      result.push(token)
    } else {
      result.push(token)
    }
    i++
  }

  return { tokens: result, endIndex: tokens.length - 1 }
}
