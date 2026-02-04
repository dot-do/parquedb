/**
 * Star Schema Benchmark (SSB) Schema for ParqueDB
 *
 * Defines the schema for SSB's star schema model:
 * - LINEORDER (fact table)
 * - CUSTOMER (dimension)
 * - SUPPLIER (dimension)
 * - PART (dimension)
 * - DATE (dimension)
 *
 * Reference: https://www.cs.umb.edu/~poneil/StarSchemaB.PDF
 */

import type { Schema } from '../../src/types/schema'

// =============================================================================
// Type Definitions
// =============================================================================

/**
 * LINEORDER fact table row
 * ~6M rows at SF1
 */
export interface LineOrder {
  lo_orderkey: number
  lo_linenumber: number
  lo_custkey: number
  lo_partkey: number
  lo_suppkey: number
  lo_orderdate: number
  lo_orderpriority: string
  lo_shippriority: number
  lo_quantity: number
  lo_extendedprice: number
  lo_ordtotalprice: number
  lo_discount: number
  lo_revenue: number
  lo_supplycost: number
  lo_tax: number
  lo_commitdate: number
  lo_shipmode: string
}

/**
 * CUSTOMER dimension table row
 * ~30K rows at SF1
 */
export interface Customer {
  c_custkey: number
  c_name: string
  c_address: string
  c_city: string
  c_nation: string
  c_region: string
  c_phone: string
  c_mktsegment: string
}

/**
 * SUPPLIER dimension table row
 * ~2K rows at SF1
 */
export interface Supplier {
  s_suppkey: number
  s_name: string
  s_address: string
  s_city: string
  s_nation: string
  s_region: string
  s_phone: string
}

/**
 * PART dimension table row
 * ~200K rows at SF1
 */
export interface Part {
  p_partkey: number
  p_name: string
  p_mfgr: string
  p_category: string
  p_brand1: string
  p_color: string
  p_type: string
  p_size: number
  p_container: string
}

/**
 * DATE dimension table row
 * ~2556 rows (fixed, 7 years of dates)
 */
export interface DateDim {
  d_datekey: number
  d_date: string
  d_dayofweek: string
  d_month: string
  d_year: number
  d_yearmonthnum: number
  d_yearmonth: string
  d_daynuminweek: number
  d_daynuminmonth: number
  d_daynuminyear: number
  d_monthnuminyear: number
  d_weeknuminyear: number
  d_sellingseason: string
  d_lastdayinweekfl: number
  d_lastdayinmonthfl: number
  d_holidayfl: number
  d_weekdayfl: number
}

// =============================================================================
// SSB Constants
// =============================================================================

/**
 * Regions used in SSB
 */
export const REGIONS = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'] as const
export type Region = (typeof REGIONS)[number]

/**
 * Nations per region (5 per region, 25 total)
 */
export const NATIONS_BY_REGION: Record<Region, string[]> = {
  AFRICA: ['ALGERIA', 'EGYPT', 'ETHIOPIA', 'KENYA', 'MOROCCO'],
  AMERICA: ['ARGENTINA', 'BRAZIL', 'CANADA', 'PERU', 'UNITED STATES'],
  ASIA: ['CHINA', 'INDIA', 'INDONESIA', 'JAPAN', 'VIETNAM'],
  EUROPE: ['FRANCE', 'GERMANY', 'ROMANIA', 'RUSSIA', 'UNITED KINGDOM'],
  'MIDDLE EAST': ['IRAN', 'IRAQ', 'JORDAN', 'SAUDI ARABIA', 'EGYPT'],
}

/**
 * All nations
 */
export const NATIONS = Object.values(NATIONS_BY_REGION).flat()

/**
 * Cities per nation (10 per nation, 250 total)
 * City format: NATION + digit (0-9)
 */
export function getCitiesForNation(nation: string): string[] {
  return Array.from({ length: 10 }, (_, i) => `${nation}${i}`)
}

/**
 * Market segments
 */
export const MARKET_SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY'] as const
export type MarketSegment = (typeof MARKET_SEGMENTS)[number]

/**
 * Order priorities
 */
export const ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'] as const
export type OrderPriority = (typeof ORDER_PRIORITIES)[number]

/**
 * Ship modes
 */
export const SHIP_MODES = ['AIR', 'FOB', 'MAIL', 'RAIL', 'REG AIR', 'SHIP', 'TRUCK'] as const
export type ShipMode = (typeof SHIP_MODES)[number]

/**
 * Part types (syllables combined)
 */
export const PART_TYPE_SYLLABLES = [
  ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO'],
  ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED'],
  ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'],
] as const

/**
 * Part colors
 */
export const PART_COLORS = [
  'almond',
  'antique',
  'aquamarine',
  'azure',
  'beige',
  'bisque',
  'black',
  'blanched',
  'blue',
  'blush',
  'brown',
  'burlywood',
  'burnished',
  'chartreuse',
  'chiffon',
  'chocolate',
  'coral',
  'cornflower',
  'cornsilk',
  'cream',
  'cyan',
  'dark',
  'deep',
  'dim',
  'dodger',
  'drab',
  'firebrick',
  'floral',
  'forest',
  'frosted',
  'gainsboro',
  'ghost',
  'goldenrod',
  'green',
  'grey',
  'honeydew',
  'hot',
  'indian',
  'ivory',
  'khaki',
  'lace',
  'lavender',
  'lawn',
  'lemon',
  'light',
  'lime',
  'linen',
  'magenta',
  'maroon',
  'medium',
  'metallic',
  'midnight',
  'mint',
  'misty',
  'moccasin',
  'navajo',
  'navy',
  'olive',
  'orange',
  'orchid',
  'pale',
  'papaya',
  'peach',
  'peru',
  'pink',
  'plum',
  'powder',
  'puff',
  'purple',
  'red',
  'rose',
  'rosy',
  'royal',
  'saddle',
  'salmon',
  'sandy',
  'seashell',
  'sienna',
  'sky',
  'slate',
  'smoke',
  'snow',
  'spring',
  'steel',
  'tan',
  'thistle',
  'tomato',
  'turquoise',
  'violet',
  'wheat',
  'white',
  'yellow',
] as const
export type PartColor = (typeof PART_COLORS)[number]

/**
 * Container types
 */
export const CONTAINER_TYPES = [
  'SM CASE',
  'SM BOX',
  'SM BAG',
  'SM JAR',
  'SM PACK',
  'SM PKG',
  'SM CAN',
  'SM DRUM',
  'MED CASE',
  'MED BOX',
  'MED BAG',
  'MED JAR',
  'MED PACK',
  'MED PKG',
  'MED CAN',
  'MED DRUM',
  'LG CASE',
  'LG BOX',
  'LG BAG',
  'LG JAR',
  'LG PACK',
  'LG PKG',
  'LG CAN',
  'LG DRUM',
  'JUMBO CASE',
  'JUMBO BOX',
  'JUMBO BAG',
  'JUMBO JAR',
  'JUMBO PACK',
  'JUMBO PKG',
  'JUMBO CAN',
  'JUMBO DRUM',
  'WRAP CASE',
  'WRAP BOX',
  'WRAP BAG',
  'WRAP JAR',
  'WRAP PACK',
  'WRAP PKG',
  'WRAP CAN',
  'WRAP DRUM',
] as const
export type ContainerType = (typeof CONTAINER_TYPES)[number]

/**
 * Days of week
 */
export const DAYS_OF_WEEK = [
  'Sunday',
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
] as const

/**
 * Months
 */
export const MONTHS = [
  'January',
  'February',
  'March',
  'April',
  'May',
  'June',
  'July',
  'August',
  'September',
  'October',
  'November',
  'December',
] as const

/**
 * Month abbreviations
 */
export const MONTH_ABBREVS = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec',
] as const

// =============================================================================
// Scale Factor Configuration
// =============================================================================

/**
 * SSB scale factor configuration
 * Determines the number of rows in each table
 */
export interface ScaleFactorConfig {
  scaleFactor: number
  lineorderRows: number
  customerRows: number
  supplierRows: number
  partRows: number
  dateRows: number // Fixed at ~2556 (7 years)
}

/**
 * Get configuration for a scale factor
 */
export function getScaleFactorConfig(sf: number): ScaleFactorConfig {
  return {
    scaleFactor: sf,
    lineorderRows: Math.round(6_000_000 * sf),
    customerRows: Math.round(30_000 * sf),
    supplierRows: Math.round(2_000 * sf),
    partRows: Math.round(200_000 * sf ** 0.4), // Part grows slower
    dateRows: 2556, // Fixed: 7 years (1992-1998)
  }
}

// =============================================================================
// ParqueDB Schema Definition
// =============================================================================

/**
 * SSB schema for ParqueDB
 *
 * Defines all tables with their fields, relationships, and indexes.
 */
export const ssbSchema: Schema = {
  /**
   * LINEORDER - Fact table
   * Contains order line items with foreign keys to all dimensions
   */
  LineOrder: {
    $type: 'ssb:LineOrder',
    $ns: 'ssb',
    $shred: ['lo_orderdate', 'lo_custkey', 'lo_partkey', 'lo_suppkey', 'lo_quantity', 'lo_discount'],
    $id: 'lo_orderkey',

    // Primary key (composite in original SSB)
    lo_orderkey: 'int!',
    lo_linenumber: 'int!',

    // Foreign keys to dimensions
    lo_custkey: 'int!#', // Indexed for joins
    lo_partkey: 'int!#',
    lo_suppkey: 'int!#',
    lo_orderdate: 'int!#', // Date key in YYYYMMDD format
    lo_commitdate: 'int!',

    // Order attributes
    lo_orderpriority: 'string!',
    lo_shippriority: 'int!',
    lo_shipmode: 'string!',

    // Measures
    lo_quantity: 'int!',
    lo_extendedprice: 'int!', // In cents
    lo_ordtotalprice: 'int!',
    lo_discount: 'int!', // Percentage 0-10
    lo_revenue: 'int!', // Computed: extendedprice * (1 - discount/100)
    lo_supplycost: 'int!',
    lo_tax: 'int!', // Percentage 0-8

    // Relationships to dimensions
    customer: '-> Customer.orders',
    part: '-> Part.orders',
    supplier: '-> Supplier.orders',
    orderDate: '-> DateDim.orders',
  },

  /**
   * CUSTOMER - Dimension table
   */
  Customer: {
    $type: 'ssb:Customer',
    $ns: 'ssb',
    $shred: ['c_city', 'c_nation', 'c_region', 'c_mktsegment'],
    $id: 'c_custkey',

    c_custkey: 'int!##', // Unique index
    c_name: 'string!',
    c_address: 'string!',
    c_city: 'string!#',
    c_nation: 'string!#',
    c_region: 'string!#',
    c_phone: 'string!',
    c_mktsegment: 'string!#',

    // Back-reference from LineOrder
    orders: '<- LineOrder.customer[]',
  },

  /**
   * SUPPLIER - Dimension table
   */
  Supplier: {
    $type: 'ssb:Supplier',
    $ns: 'ssb',
    $shred: ['s_city', 's_nation', 's_region'],
    $id: 's_suppkey',

    s_suppkey: 'int!##', // Unique index
    s_name: 'string!',
    s_address: 'string!',
    s_city: 'string!#',
    s_nation: 'string!#',
    s_region: 'string!#',
    s_phone: 'string!',

    // Back-reference from LineOrder
    orders: '<- LineOrder.supplier[]',
  },

  /**
   * PART - Dimension table
   */
  Part: {
    $type: 'ssb:Part',
    $ns: 'ssb',
    $shred: ['p_mfgr', 'p_category', 'p_brand1'],
    $id: 'p_partkey',

    p_partkey: 'int!##', // Unique index
    p_name: 'string!',
    p_mfgr: 'string!#',
    p_category: 'string!#',
    p_brand1: 'string!#',
    p_color: 'string!',
    p_type: 'string!',
    p_size: 'int!',
    p_container: 'string!',

    // Back-reference from LineOrder
    orders: '<- LineOrder.part[]',
  },

  /**
   * DATE - Dimension table
   * Contains 7 years of dates (1992-1998)
   */
  DateDim: {
    $type: 'ssb:Date',
    $ns: 'ssb',
    $shred: ['d_year', 'd_yearmonthnum', 'd_weeknuminyear'],
    $id: 'd_datekey',

    d_datekey: 'int!##', // YYYYMMDD format, unique index
    d_date: 'string!',
    d_dayofweek: 'string!',
    d_month: 'string!',
    d_year: 'int!#',
    d_yearmonthnum: 'int!#',
    d_yearmonth: 'string!',
    d_daynuminweek: 'int!',
    d_daynuminmonth: 'int!',
    d_daynuminyear: 'int!',
    d_monthnuminyear: 'int!',
    d_weeknuminyear: 'int!',
    d_sellingseason: 'string!',
    d_lastdayinweekfl: 'int!',
    d_lastdayinmonthfl: 'int!',
    d_holidayfl: 'int!',
    d_weekdayfl: 'int!',

    // Back-reference from LineOrder
    orders: '<- LineOrder.orderDate[]',
  },
}

// =============================================================================
// DB Schema Format (for DB() function)
// =============================================================================

/**
 * SSB schema in DB() format for easy initialization
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 * import { ssbDBSchema } from './schema'
 *
 * const db = DB(ssbDBSchema)
 * ```
 */
export const ssbDBSchema = {
  LineOrder: {
    lo_orderkey: 'int!',
    lo_linenumber: 'int!',
    lo_custkey: 'int!#',
    lo_partkey: 'int!#',
    lo_suppkey: 'int!#',
    lo_orderdate: 'int!#',
    lo_commitdate: 'int!',
    lo_orderpriority: 'string!',
    lo_shippriority: 'int!',
    lo_shipmode: 'string!',
    lo_quantity: 'int!',
    lo_extendedprice: 'int!',
    lo_ordtotalprice: 'int!',
    lo_discount: 'int!',
    lo_revenue: 'int!',
    lo_supplycost: 'int!',
    lo_tax: 'int!',
    customer: '-> Customer.orders',
    part: '-> Part.orders',
    supplier: '-> Supplier.orders',
    orderDate: '-> DateDim.orders',
  },
  Customer: {
    c_custkey: 'int!##',
    c_name: 'string!',
    c_address: 'string!',
    c_city: 'string!#',
    c_nation: 'string!#',
    c_region: 'string!#',
    c_phone: 'string!',
    c_mktsegment: 'string!#',
    orders: '<- LineOrder.customer[]',
  },
  Supplier: {
    s_suppkey: 'int!##',
    s_name: 'string!',
    s_address: 'string!',
    s_city: 'string!#',
    s_nation: 'string!#',
    s_region: 'string!#',
    s_phone: 'string!',
    orders: '<- LineOrder.supplier[]',
  },
  Part: {
    p_partkey: 'int!##',
    p_name: 'string!',
    p_mfgr: 'string!#',
    p_category: 'string!#',
    p_brand1: 'string!#',
    p_color: 'string!',
    p_type: 'string!',
    p_size: 'int!',
    p_container: 'string!',
    orders: '<- LineOrder.part[]',
  },
  DateDim: {
    d_datekey: 'int!##',
    d_date: 'string!',
    d_dayofweek: 'string!',
    d_month: 'string!',
    d_year: 'int!#',
    d_yearmonthnum: 'int!#',
    d_yearmonth: 'string!',
    d_daynuminweek: 'int!',
    d_daynuminmonth: 'int!',
    d_daynuminyear: 'int!',
    d_monthnuminyear: 'int!',
    d_weeknuminyear: 'int!',
    d_sellingseason: 'string!',
    d_lastdayinweekfl: 'int!',
    d_lastdayinmonthfl: 'int!',
    d_holidayfl: 'int!',
    d_weekdayfl: 'int!',
    orders: '<- LineOrder.orderDate[]',
  },
} as const
