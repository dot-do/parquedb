/**
 * UNSPSC Example Module for ParqueDB
 *
 * Demonstrates loading and querying the United Nations Standard Products
 * and Services Code (UNSPSC) taxonomy.
 */

// Schema exports
export {
  // Types
  type UNSPSCBase,
  type Segment,
  type Family,
  type Class,
  type Commodity,
  type UNSPSCEntity,
  type UNSPSCCSVRow,
  type UNSPSCFlatRow,

  // Schema definition
  UNSPSCSchema,

  // Helper functions
  entityId,
  parseCode,
  isValidCode,
  getParentCode,
  formatCode,
} from './schema'

// Loader exports
export {
  loadUNSPSC,
  generateSampleData,
} from './load'

// Query exports
export {
  // Types
  type HierarchyPath,
  type SearchResult,
  type TreeNode,
  type Breadcrumb,
  type UNSPSCDatabase,

  // Hierarchy queries
  getHierarchyPath,
  getChildren,
  getParent,
  getAncestors,
  getBreadcrumbs,
  getDescendants,

  // Search queries
  searchUNSPSC,
  findByCodePrefix,

  // Tree building
  buildTree,
  getSegmentsWithCounts,

  // Relationship queries
  findRelated,

  // Statistics
  getStatistics,

  // Example
  exampleQueries,
} from './queries'
