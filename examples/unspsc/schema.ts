/**
 * UNSPSC Schema Definition for ParqueDB
 *
 * The United Nations Standard Products and Services Code (UNSPSC) is a hierarchical
 * taxonomy used globally for classifying products and services in procurement.
 *
 * Hierarchy: Segment (2-digit) -> Family (4-digit) -> Class (6-digit) -> Commodity (8-digit)
 */

// =============================================================================
// Type Definitions
// =============================================================================

/**
 * Base interface for all UNSPSC levels
 */
export interface UNSPSCBase {
  /** UNSPSC code (2, 4, 6, or 8 digits) */
  code: string
  /** Human-readable title */
  title: string
  /** Optional description */
  description?: string
  /** Whether this code is active/current */
  isActive: boolean
}

/**
 * Segment - Top level category (2-digit code)
 * Example: "10" = Live Plant and Animal Material and Accessories and Supplies
 */
export interface Segment extends UNSPSCBase {
  $type: 'Segment'
  /** 2-digit segment code */
  code: string
}

/**
 * Family - Subdivision of Segment (4-digit code)
 * Example: "1010" = Live animals
 */
export interface Family extends UNSPSCBase {
  $type: 'Family'
  /** 4-digit family code */
  code: string
  /** Reference to parent Segment */
  segmentCode: string
}

/**
 * Class - Subdivision of Family (6-digit code)
 * Example: "101015" = Livestock
 */
export interface Class extends UNSPSCBase {
  $type: 'Class'
  /** 6-digit class code */
  code: string
  /** Reference to parent Family */
  familyCode: string
  /** Reference to grandparent Segment */
  segmentCode: string
}

/**
 * Commodity - Specific product/service (8-digit code)
 * Example: "10101501" = Cats
 */
export interface Commodity extends UNSPSCBase {
  $type: 'Commodity'
  /** 8-digit commodity code */
  code: string
  /** Reference to parent Class */
  classCode: string
  /** Reference to grandparent Family */
  familyCode: string
  /** Reference to great-grandparent Segment */
  segmentCode: string
}

/**
 * Union type for all UNSPSC entity types
 */
export type UNSPSCEntity = Segment | Family | Class | Commodity

// =============================================================================
// Schema Definition (ParqueDB/graphdl format)
// =============================================================================

/**
 * ParqueDB schema definition for UNSPSC taxonomy
 *
 * Relationships are defined using graphdl syntax:
 * - `->` forward reference (outbound)
 * - `<-` backward reference (inbound, auto-populated)
 */
export const UNSPSCSchema = {
  // Namespace for UNSPSC entities
  $namespace: 'unspsc',

  // Segment: Top-level category
  Segment: {
    $type: 'schema:DefinedTerm',
    $description: 'UNSPSC Segment - Top-level 2-digit category',

    // Data fields
    code: { type: 'string', index: 'unique', description: '2-digit segment code' },
    title: { type: 'string', index: 'fts', description: 'Segment title' },
    description: { type: 'string', index: 'fts', optional: true },
    isActive: { type: 'boolean', default: true },

    // Inbound relationships (children)
    families: '<- Family.segment[]',
  },

  // Family: Subdivision of Segment
  Family: {
    $type: 'schema:DefinedTerm',
    $description: 'UNSPSC Family - 4-digit subdivision of Segment',

    // Data fields
    code: { type: 'string', index: 'unique', description: '4-digit family code' },
    title: { type: 'string', index: 'fts', description: 'Family title' },
    description: { type: 'string', index: 'fts', optional: true },
    isActive: { type: 'boolean', default: true },

    // Parent reference
    segmentCode: { type: 'string', index: true, description: 'Parent segment code' },

    // Outbound relationships (parent)
    segment: '-> Segment.families',

    // Inbound relationships (children)
    classes: '<- Class.family[]',
  },

  // Class: Subdivision of Family
  Class: {
    $type: 'schema:DefinedTerm',
    $description: 'UNSPSC Class - 6-digit subdivision of Family',

    // Data fields
    code: { type: 'string', index: 'unique', description: '6-digit class code' },
    title: { type: 'string', index: 'fts', description: 'Class title' },
    description: { type: 'string', index: 'fts', optional: true },
    isActive: { type: 'boolean', default: true },

    // Parent references
    familyCode: { type: 'string', index: true, description: 'Parent family code' },
    segmentCode: { type: 'string', index: true, description: 'Grandparent segment code' },

    // Outbound relationships (parent)
    family: '-> Family.classes',

    // Inbound relationships (children)
    commodities: '<- Commodity.class[]',
  },

  // Commodity: Specific product/service
  Commodity: {
    $type: 'schema:DefinedTerm',
    $description: 'UNSPSC Commodity - 8-digit specific product/service',

    // Data fields
    code: { type: 'string', index: 'unique', description: '8-digit commodity code' },
    title: { type: 'string', index: 'fts', description: 'Commodity title' },
    description: { type: 'string', index: 'fts', optional: true },
    isActive: { type: 'boolean', default: true },

    // Parent references (denormalized for efficient queries)
    classCode: { type: 'string', index: true, description: 'Parent class code' },
    familyCode: { type: 'string', index: true, description: 'Grandparent family code' },
    segmentCode: { type: 'string', index: true, description: 'Great-grandparent segment code' },

    // Outbound relationships (parent)
    class: '-> Class.commodities',

    // Cross-dataset relationship: O*NET technologies using this commodity code
    // This enables queries like "What tools are classified under this UNSPSC code?"
    technologies: '<- onet/Technology.unspscCommodity[]',
  },
} as const

// =============================================================================
// Helper Types
// =============================================================================

/**
 * Extract entity ID for a UNSPSC code
 */
export function entityId(type: 'Segment' | 'Family' | 'Class' | 'Commodity', code: string): string {
  const ns = type.toLowerCase() + 's'  // segments, families, classes, commodities
  return `${ns}/${code}`
}

/**
 * Parse a UNSPSC code into its hierarchical components
 */
export function parseCode(code: string): {
  segment: string
  family?: string
  class?: string
  commodity?: string
  level: 'Segment' | 'Family' | 'Class' | 'Commodity'
} {
  const normalized = code.replace(/[^0-9]/g, '')

  if (normalized.length === 2) {
    return { segment: normalized, level: 'Segment' }
  } else if (normalized.length === 4) {
    return {
      segment: normalized.slice(0, 2),
      family: normalized,
      level: 'Family',
    }
  } else if (normalized.length === 6) {
    return {
      segment: normalized.slice(0, 2),
      family: normalized.slice(0, 4),
      class: normalized,
      level: 'Class',
    }
  } else if (normalized.length === 8) {
    return {
      segment: normalized.slice(0, 2),
      family: normalized.slice(0, 4),
      class: normalized.slice(0, 6),
      commodity: normalized,
      level: 'Commodity',
    }
  }

  throw new Error(`Invalid UNSPSC code: ${code}. Must be 2, 4, 6, or 8 digits.`)
}

/**
 * Validate a UNSPSC code format
 */
export function isValidCode(code: string): boolean {
  const normalized = code.replace(/[^0-9]/g, '')
  return [2, 4, 6, 8].includes(normalized.length)
}

/**
 * Get the parent code for a given UNSPSC code
 */
export function getParentCode(code: string): string | null {
  const normalized = code.replace(/[^0-9]/g, '')

  switch (normalized.length) {
    case 8:
      return normalized.slice(0, 6)
    case 6:
      return normalized.slice(0, 4)
    case 4:
      return normalized.slice(0, 2)
    case 2:
      return null  // Segment has no parent
    default:
      throw new Error(`Invalid UNSPSC code: ${code}`)
  }
}

/**
 * Format a UNSPSC code with optional separators
 */
export function formatCode(code: string, separator: string = ''): string {
  const normalized = code.replace(/[^0-9]/g, '')

  if (normalized.length <= 2) return normalized

  const parts: string[] = []
  parts.push(normalized.slice(0, 2))

  if (normalized.length >= 4) parts.push(normalized.slice(2, 4))
  if (normalized.length >= 6) parts.push(normalized.slice(4, 6))
  if (normalized.length >= 8) parts.push(normalized.slice(6, 8))

  return parts.join(separator)
}

// =============================================================================
// CSV Row Types (for data loading)
// =============================================================================

/**
 * Raw CSV row format from UNSPSC data files
 */
export interface UNSPSCCSVRow {
  Segment: string
  'Segment Title': string
  Family: string
  'Family Title': string
  Class: string
  'Class Title': string
  Commodity: string
  'Commodity Title': string
}

/**
 * Alternative CSV format with separate columns
 */
export interface UNSPSCFlatRow {
  Code: string
  Title: string
  Level: 'Segment' | 'Family' | 'Class' | 'Commodity'
  ParentCode?: string
  Description?: string
}
