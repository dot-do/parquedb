/**
 * TPC-H Benchmark Schema for ParqueDB
 *
 * Defines the snowflake schema with:
 * - LINEITEM (fact table)
 * - ORDERS, CUSTOMER, PART, SUPPLIER, PARTSUPP (dimension tables)
 * - NATION, REGION (normalized dimensions)
 *
 * Includes bidirectional relationships for graph traversal.
 */

import type { DBSchema } from '../../src/db'
import type { Schema } from '../../src/types/schema'

// =============================================================================
// Type Definitions
// =============================================================================

/**
 * REGION dimension (5 regions: AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
 */
export interface Region {
  r_regionkey: number
  r_name: string
  r_comment: string
}

/**
 * NATION dimension (25 nations, each belonging to a region)
 */
export interface Nation {
  n_nationkey: number
  n_name: string
  n_regionkey: number
  n_comment: string
}

/**
 * SUPPLIER dimension
 */
export interface Supplier {
  s_suppkey: number
  s_name: string
  s_address: string
  s_nationkey: number
  s_phone: string
  s_acctbal: number
  s_comment: string
}

/**
 * PART dimension
 */
export interface Part {
  p_partkey: number
  p_name: string
  p_mfgr: string
  p_brand: string
  p_type: string
  p_size: number
  p_container: string
  p_retailprice: number
  p_comment: string
}

/**
 * PARTSUPP bridge table (Part-Supplier relationship)
 */
export interface PartSupp {
  ps_partkey: number
  ps_suppkey: number
  ps_availqty: number
  ps_supplycost: number
  ps_comment: string
}

/**
 * CUSTOMER dimension
 */
export interface Customer {
  c_custkey: number
  c_name: string
  c_address: string
  c_nationkey: number
  c_phone: string
  c_acctbal: number
  c_mktsegment: string
  c_comment: string
}

/**
 * ORDERS dimension (connected to CUSTOMER)
 */
export interface Order {
  o_orderkey: number
  o_custkey: number
  o_orderstatus: string
  o_totalprice: number
  o_orderdate: string  // ISO date string YYYY-MM-DD
  o_orderpriority: string
  o_clerk: string
  o_shippriority: number
  o_comment: string
}

/**
 * LINEITEM fact table
 */
export interface LineItem {
  l_orderkey: number
  l_partkey: number
  l_suppkey: number
  l_linenumber: number
  l_quantity: number
  l_extendedprice: number
  l_discount: number
  l_tax: number
  l_returnflag: string
  l_linestatus: string
  l_shipdate: string    // ISO date string
  l_commitdate: string  // ISO date string
  l_receiptdate: string // ISO date string
  l_shipinstruct: string
  l_shipmode: string
  l_comment: string
}

// =============================================================================
// TPC-H Schema for DB() Function
// =============================================================================

/**
 * TPC-H Schema for ParqueDB using DB() notation
 *
 * This schema defines the snowflake schema with relationships between
 * the fact table (LINEITEM) and dimension tables.
 */
export const tpchSchema: DBSchema = {
  // REGION dimension (5 rows, fixed)
  Region: {
    $id: 'r_regionkey',
    $name: 'r_name',

    r_regionkey: 'int!##',   // Primary key with unique index
    r_name: 'string!#',      // Indexed for joins
    r_comment: 'string',

    // Reverse relationship from NATION
    nations: '<- Nation.region[]',
  },

  // NATION dimension (25 rows, fixed)
  Nation: {
    $id: 'n_nationkey',
    $name: 'n_name',

    n_nationkey: 'int!##',   // Primary key with unique index
    n_name: 'string!#',      // Indexed for joins
    n_regionkey: 'int!#',    // Foreign key to REGION
    n_comment: 'string',

    // Relationship to REGION
    region: '-> Region.nations',

    // Reverse relationships from CUSTOMER and SUPPLIER
    customers: '<- Customer.nation[]',
    suppliers: '<- Supplier.nation[]',
  },

  // SUPPLIER dimension
  Supplier: {
    $id: 's_suppkey',
    $name: 's_name',

    s_suppkey: 'int!##',     // Primary key with unique index
    s_name: 'string!',
    s_address: 'string!',
    s_nationkey: 'int!#',    // Foreign key to NATION
    s_phone: 'string!',
    s_acctbal: 'float!',
    s_comment: 'string',

    // Relationship to NATION
    nation: '-> Nation.suppliers',

    // Reverse relationships
    partsupps: '<- PartSupp.supplier[]',
    lineitems: '<- LineItem.supplier[]',
  },

  // PART dimension
  Part: {
    $id: 'p_partkey',
    $name: 'p_name',

    p_partkey: 'int!##',     // Primary key with unique index
    p_name: 'string!',
    p_mfgr: 'string!#',      // Indexed for Q4 queries
    p_brand: 'string!#',     // Indexed for Q2/Q19 queries
    p_type: 'string!#',      // Indexed for Q14/Q16 queries
    p_size: 'int!#',         // Indexed for Q19 queries
    p_container: 'string!#', // Indexed for Q19 queries
    p_retailprice: 'float!',
    p_comment: 'string',

    // Reverse relationships
    partsupps: '<- PartSupp.part[]',
    lineitems: '<- LineItem.part[]',
  },

  // PARTSUPP bridge table
  PartSupp: {
    // Composite key from ps_partkey and ps_suppkey
    ps_partkey: 'int!#',     // FK to PART
    ps_suppkey: 'int!#',     // FK to SUPPLIER
    ps_availqty: 'int!',
    ps_supplycost: 'float!#', // Indexed for Q2/Q11 queries
    ps_comment: 'string',

    // Relationships
    part: '-> Part.partsupps',
    supplier: '-> Supplier.partsupps',
  },

  // CUSTOMER dimension
  Customer: {
    $id: 'c_custkey',
    $name: 'c_name',

    c_custkey: 'int!##',     // Primary key with unique index
    c_name: 'string!',
    c_address: 'string!',
    c_nationkey: 'int!#',    // FK to NATION
    c_phone: 'string!',
    c_acctbal: 'float!#',    // Indexed for Q22
    c_mktsegment: 'string!#', // Indexed for Q3
    c_comment: 'string',

    // Relationships
    nation: '-> Nation.customers',
    orders: '<- Order.customer[]',
  },

  // ORDERS dimension
  Order: {
    $id: 'o_orderkey',

    o_orderkey: 'int!##',    // Primary key with unique index
    o_custkey: 'int!#',      // FK to CUSTOMER
    o_orderstatus: 'string!#', // Indexed (O, F, P)
    o_totalprice: 'float!',
    o_orderdate: 'string!#', // ISO date, indexed for range queries
    o_orderpriority: 'string!#', // Indexed for Q4/Q12
    o_clerk: 'string!',
    o_shippriority: 'int!#', // Indexed for Q3
    o_comment: 'string',

    // Relationships
    customer: '-> Customer.orders',
    lineitems: '<- LineItem.order[]',
  },

  // LINEITEM fact table
  LineItem: {
    // Composite key from l_orderkey and l_linenumber
    l_orderkey: 'int!#',     // FK to ORDERS
    l_partkey: 'int!#',      // FK to PART
    l_suppkey: 'int!#',      // FK to SUPPLIER
    l_linenumber: 'int!',
    l_quantity: 'float!#',   // Indexed for Q1/Q6/Q19
    l_extendedprice: 'float!',
    l_discount: 'float!#',   // Indexed for Q1/Q6/Q19
    l_tax: 'float!',
    l_returnflag: 'string!#', // Indexed for Q1/Q10 (R, A, N)
    l_linestatus: 'string!#', // Indexed for Q1 (O, F)
    l_shipdate: 'string!#',  // ISO date, indexed for range queries
    l_commitdate: 'string!', // ISO date
    l_receiptdate: 'string!#', // ISO date, indexed for Q12
    l_shipinstruct: 'string!',
    l_shipmode: 'string!#',  // Indexed for Q12
    l_comment: 'string',

    // Relationships
    order: '-> Order.lineitems',
    part: '-> Part.lineitems',
    supplier: '-> Supplier.lineitems',
  },
}

// =============================================================================
// Alternative Full Schema Definition
// =============================================================================

/**
 * Full TPC-H Schema using the Schema type
 * This provides more detailed type annotations and metadata
 */
export const tpchFullSchema: Schema = {
  Region: {
    $type: 'tpch:Region',
    $ns: 'tpch',
    $description: 'Region dimension table (5 rows)',

    r_regionkey: { type: 'int', required: true, index: 'unique' },
    r_name: { type: 'string', required: true, index: true },
    r_comment: { type: 'string', required: false },

    nations: '[<- Nation.region]',
  },

  Nation: {
    $type: 'tpch:Nation',
    $ns: 'tpch',
    $shred: ['n_regionkey'],
    $description: 'Nation dimension table (25 rows)',

    n_nationkey: { type: 'int', required: true, index: 'unique' },
    n_name: { type: 'string', required: true, index: true },
    n_regionkey: { type: 'int', required: true, index: true },
    n_comment: { type: 'string', required: false },

    region: '-> Region.nations',
    customers: '[<- Customer.nation]',
    suppliers: '[<- Supplier.nation]',
  },

  Supplier: {
    $type: 'tpch:Supplier',
    $ns: 'tpch',
    $shred: ['s_nationkey', 's_acctbal'],
    $description: 'Supplier dimension table',

    s_suppkey: { type: 'int', required: true, index: 'unique' },
    s_name: { type: 'string', required: true },
    s_address: { type: 'string', required: true },
    s_nationkey: { type: 'int', required: true, index: true },
    s_phone: { type: 'string', required: true },
    s_acctbal: { type: 'float', required: true, index: true },
    s_comment: { type: 'string', required: false },

    nation: '-> Nation.suppliers',
    partsupps: '[<- PartSupp.supplier]',
    lineitems: '[<- LineItem.supplier]',
  },

  Part: {
    $type: 'tpch:Part',
    $ns: 'tpch',
    $shred: ['p_brand', 'p_type', 'p_size', 'p_container'],
    $description: 'Part dimension table',

    p_partkey: { type: 'int', required: true, index: 'unique' },
    p_name: { type: 'string', required: true },
    p_mfgr: { type: 'string', required: true, index: true },
    p_brand: { type: 'string', required: true, index: true },
    p_type: { type: 'string', required: true, index: true },
    p_size: { type: 'int', required: true, index: true },
    p_container: { type: 'string', required: true, index: true },
    p_retailprice: { type: 'float', required: true },
    p_comment: { type: 'string', required: false },

    partsupps: '[<- PartSupp.part]',
    lineitems: '[<- LineItem.part]',
  },

  PartSupp: {
    $type: 'tpch:PartSupp',
    $ns: 'tpch',
    $shred: ['ps_partkey', 'ps_suppkey', 'ps_supplycost'],
    $description: 'Part-Supplier bridge table',

    ps_partkey: { type: 'int', required: true, index: true },
    ps_suppkey: { type: 'int', required: true, index: true },
    ps_availqty: { type: 'int', required: true },
    ps_supplycost: { type: 'float', required: true, index: true },
    ps_comment: { type: 'string', required: false },

    part: '-> Part.partsupps',
    supplier: '-> Supplier.partsupps',
  },

  Customer: {
    $type: 'tpch:Customer',
    $ns: 'tpch',
    $shred: ['c_nationkey', 'c_mktsegment', 'c_acctbal'],
    $description: 'Customer dimension table',

    c_custkey: { type: 'int', required: true, index: 'unique' },
    c_name: { type: 'string', required: true },
    c_address: { type: 'string', required: true },
    c_nationkey: { type: 'int', required: true, index: true },
    c_phone: { type: 'string', required: true },
    c_acctbal: { type: 'float', required: true, index: true },
    c_mktsegment: { type: 'string', required: true, index: true },
    c_comment: { type: 'string', required: false },

    nation: '-> Nation.customers',
    orders: '[<- Order.customer]',
  },

  Order: {
    $type: 'tpch:Order',
    $ns: 'tpch',
    $shred: ['o_custkey', 'o_orderdate', 'o_orderstatus', 'o_orderpriority'],
    $description: 'Orders dimension table',

    o_orderkey: { type: 'int', required: true, index: 'unique' },
    o_custkey: { type: 'int', required: true, index: true },
    o_orderstatus: { type: 'string', required: true, index: true },
    o_totalprice: { type: 'float', required: true },
    o_orderdate: { type: 'string', required: true, index: true },
    o_orderpriority: { type: 'string', required: true, index: true },
    o_clerk: { type: 'string', required: true },
    o_shippriority: { type: 'int', required: true, index: true },
    o_comment: { type: 'string', required: false },

    customer: '-> Customer.orders',
    lineitems: '[<- LineItem.order]',
  },

  LineItem: {
    $type: 'tpch:LineItem',
    $ns: 'tpch',
    $shred: [
      'l_orderkey',
      'l_partkey',
      'l_suppkey',
      'l_shipdate',
      'l_returnflag',
      'l_linestatus',
      'l_quantity',
      'l_discount',
    ],
    $description: 'LineItem fact table',

    l_orderkey: { type: 'int', required: true, index: true },
    l_partkey: { type: 'int', required: true, index: true },
    l_suppkey: { type: 'int', required: true, index: true },
    l_linenumber: { type: 'int', required: true },
    l_quantity: { type: 'float', required: true, index: true },
    l_extendedprice: { type: 'float', required: true },
    l_discount: { type: 'float', required: true, index: true },
    l_tax: { type: 'float', required: true },
    l_returnflag: { type: 'string', required: true, index: true },
    l_linestatus: { type: 'string', required: true, index: true },
    l_shipdate: { type: 'string', required: true, index: true },
    l_commitdate: { type: 'string', required: true },
    l_receiptdate: { type: 'string', required: true, index: true },
    l_shipinstruct: { type: 'string', required: true },
    l_shipmode: { type: 'string', required: true, index: true },
    l_comment: { type: 'string', required: false },

    order: '-> Order.lineitems',
    part: '-> Part.lineitems',
    supplier: '-> Supplier.lineitems',
  },
}

// =============================================================================
// Constants
// =============================================================================

/**
 * TPC-H scale factor row counts (approximate)
 */
export const TPCH_SCALE_FACTORS = {
  0.01: {
    lineitem: 60_175,
    orders: 15_000,
    customer: 1_500,
    part: 2_000,
    supplier: 100,
    partsupp: 8_000,
    nation: 25,
    region: 5,
  },
  0.1: {
    lineitem: 600_572,
    orders: 150_000,
    customer: 15_000,
    part: 20_000,
    supplier: 1_000,
    partsupp: 80_000,
    nation: 25,
    region: 5,
  },
  1: {
    lineitem: 6_001_215,
    orders: 1_500_000,
    customer: 150_000,
    part: 200_000,
    supplier: 10_000,
    partsupp: 800_000,
    nation: 25,
    region: 5,
  },
  10: {
    lineitem: 59_986_052,
    orders: 15_000_000,
    customer: 1_500_000,
    part: 2_000_000,
    supplier: 100_000,
    partsupp: 8_000_000,
    nation: 25,
    region: 5,
  },
} as const

/**
 * TPC-H regions (5 total)
 */
export const TPCH_REGIONS = [
  { key: 0, name: 'AFRICA' },
  { key: 1, name: 'AMERICA' },
  { key: 2, name: 'ASIA' },
  { key: 3, name: 'EUROPE' },
  { key: 4, name: 'MIDDLE EAST' },
] as const

/**
 * TPC-H nations (25 total, mapped to regions)
 */
export const TPCH_NATIONS = [
  { key: 0, name: 'ALGERIA', regionkey: 0 },
  { key: 1, name: 'ARGENTINA', regionkey: 1 },
  { key: 2, name: 'BRAZIL', regionkey: 1 },
  { key: 3, name: 'CANADA', regionkey: 1 },
  { key: 4, name: 'EGYPT', regionkey: 4 },
  { key: 5, name: 'ETHIOPIA', regionkey: 0 },
  { key: 6, name: 'FRANCE', regionkey: 3 },
  { key: 7, name: 'GERMANY', regionkey: 3 },
  { key: 8, name: 'INDIA', regionkey: 2 },
  { key: 9, name: 'INDONESIA', regionkey: 2 },
  { key: 10, name: 'IRAN', regionkey: 4 },
  { key: 11, name: 'IRAQ', regionkey: 4 },
  { key: 12, name: 'JAPAN', regionkey: 2 },
  { key: 13, name: 'JORDAN', regionkey: 4 },
  { key: 14, name: 'KENYA', regionkey: 0 },
  { key: 15, name: 'MOROCCO', regionkey: 0 },
  { key: 16, name: 'MOZAMBIQUE', regionkey: 0 },
  { key: 17, name: 'PERU', regionkey: 1 },
  { key: 18, name: 'CHINA', regionkey: 2 },
  { key: 19, name: 'ROMANIA', regionkey: 3 },
  { key: 20, name: 'SAUDI ARABIA', regionkey: 4 },
  { key: 21, name: 'VIETNAM', regionkey: 2 },
  { key: 22, name: 'RUSSIA', regionkey: 3 },
  { key: 23, name: 'UNITED KINGDOM', regionkey: 3 },
  { key: 24, name: 'UNITED STATES', regionkey: 1 },
] as const

/**
 * Market segments (5 total)
 */
export const TPCH_SEGMENTS = [
  'AUTOMOBILE',
  'BUILDING',
  'FURNITURE',
  'HOUSEHOLD',
  'MACHINERY',
] as const

/**
 * Order priorities
 */
export const TPCH_PRIORITIES = [
  '1-URGENT',
  '2-HIGH',
  '3-MEDIUM',
  '4-NOT SPECIFIED',
  '5-LOW',
] as const

/**
 * Ship modes
 */
export const TPCH_SHIP_MODES = [
  'AIR',
  'FOB',
  'MAIL',
  'RAIL',
  'REG AIR',
  'SHIP',
  'TRUCK',
] as const

/**
 * Ship instructions
 */
export const TPCH_SHIP_INSTRUCTIONS = [
  'DELIVER IN PERSON',
  'COLLECT COD',
  'NONE',
  'TAKE BACK RETURN',
] as const

/**
 * Part brands (25 brands per manufacturer, 5 manufacturers)
 */
export const TPCH_BRANDS_PER_MFGR = 5 as const
export const TPCH_MANUFACTURERS = 5 as const

/**
 * Container types
 */
export const TPCH_CONTAINERS = [
  'SM CASE', 'SM BOX', 'SM BAG', 'SM JAR', 'SM PACK', 'SM PKG', 'SM CAN', 'SM DRUM',
  'MED CASE', 'MED BOX', 'MED BAG', 'MED JAR', 'MED PACK', 'MED PKG', 'MED CAN', 'MED DRUM',
  'LG CASE', 'LG BOX', 'LG BAG', 'LG JAR', 'LG PACK', 'LG PKG', 'LG CAN', 'LG DRUM',
  'JUMBO CASE', 'JUMBO BOX', 'JUMBO BAG', 'JUMBO JAR', 'JUMBO PACK', 'JUMBO PKG', 'JUMBO CAN', 'JUMBO DRUM',
  'WRAP CASE', 'WRAP BOX', 'WRAP BAG', 'WRAP JAR', 'WRAP PACK', 'WRAP PKG', 'WRAP CAN', 'WRAP DRUM',
] as const

/**
 * Part types (syllable combinations)
 */
export const TPCH_TYPE_SYLLABLES = {
  syllable1: ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO'],
  syllable2: ['ANODIZED', 'BRUSHED', 'BURNISHED', 'PLATED', 'POLISHED'],
  syllable3: ['BRASS', 'COPPER', 'NICKEL', 'STEEL', 'TIN'],
} as const

/**
 * Part name syllables
 */
export const TPCH_NAME_SYLLABLES = [
  'almond', 'antique', 'aquamarine', 'azure', 'beige',
  'bisque', 'black', 'blanched', 'blue', 'blush',
  'brown', 'burlywood', 'burnished', 'chartreuse', 'chiffon',
  'chocolate', 'coral', 'cornflower', 'cornsilk', 'cream',
  'cyan', 'dark', 'deep', 'dim', 'dodger',
  'drab', 'firebrick', 'floral', 'forest', 'frosted',
  'gainsboro', 'ghost', 'goldenrod', 'green', 'grey',
  'honeydew', 'hot', 'indian', 'ivory', 'khaki',
  'lace', 'lavender', 'lawn', 'lemon', 'light',
  'lime', 'linen', 'magenta', 'maroon', 'medium',
  'metallic', 'midnight', 'mint', 'misty', 'moccasin',
  'navajo', 'navy', 'olive', 'orange', 'orchid',
  'pale', 'papaya', 'peach', 'peru', 'pink',
  'plum', 'powder', 'puff', 'purple', 'red',
  'rose', 'rosy', 'royal', 'saddle', 'salmon',
  'sandy', 'seashell', 'sienna', 'sky', 'slate',
  'smoke', 'snow', 'spring', 'steel', 'tan',
  'thistle', 'tomato', 'turquoise', 'violet', 'wheat',
  'white', 'yellow',
] as const

/**
 * Date range for TPC-H (1992-01-01 to 1998-12-31)
 */
export const TPCH_DATE_RANGE = {
  startDate: '1992-01-01',
  endDate: '1998-12-31',
  startYear: 1992,
  endYear: 1998,
} as const
