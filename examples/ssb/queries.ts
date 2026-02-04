/**
 * Star Schema Benchmark (SSB) Queries for ParqueDB
 *
 * Implements the 13 official SSB queries organized into 4 query flights:
 * - Q1: Revenue sum queries (filter on DATE and LINEORDER)
 * - Q2: Revenue by brand/category (join PART and SUPPLIER)
 * - Q3: Revenue by customer/supplier (join CUSTOMER and SUPPLIER)
 * - Q4: Profit queries (complex joins with all dimensions)
 */

import type { DBInstance } from '../../src/db'
import type { DateDim, LineOrder, Customer, Supplier, Part } from './schema'

// Extended DB interface with typed collections for MongoDB-style API
interface SSBDatabase extends DBInstance {
  DateDim: {
    find: (filter: Partial<DateDim>) => Promise<(DateDim & { $id: string })[]>
  }
  LineOrder: {
    find: (filter: Record<string, unknown>) => Promise<(LineOrder & { $id: string })[]>
  }
}

// =============================================================================
// Query Result Types
// =============================================================================

export interface Q1Result {
  revenue: number
}

export interface Q2Result {
  d_year: number
  p_brand1: string
  revenue: number
}

export interface Q3Result {
  c_nation?: string
  c_city?: string
  s_nation?: string
  s_city?: string
  d_year: number
  revenue: number
}

export interface Q4Result {
  d_year: number
  c_nation?: string
  c_city?: string
  p_category?: string
  profit: number
}

export interface SSBQueryResults {
  q11: Q1Result
  q12: Q1Result
  q13: Q1Result
  q21: Q2Result[]
  q22: Q2Result[]
  q23: Q2Result[]
  q31: Q3Result[]
  q32: Q3Result[]
  q33: Q3Result[]
  q34: Q3Result[]
  q41: Q4Result[]
  q42: Q4Result[]
  q43: Q4Result[]
}

// =============================================================================
// Query Parameters
// =============================================================================

export interface Q1Params {
  year: number
  discountLo: number
  discountHi: number
  quantityLt: number
  yearmonthnum?: number  // For Q1.2
  weeknuminyear?: number  // For Q1.3
}

export interface Q2Params {
  region: string
  category?: string
  brands?: string[]
}

export interface Q3Params {
  customerRegion?: string
  customerNation?: string
  customerCities?: string[]
  supplierRegion?: string
  supplierNation?: string
  supplierCities?: string[]
  yearRange?: [number, number]
}

export interface Q4Params {
  customerRegion?: string
  customerNation?: string
  supplierRegion?: string
  supplierNation?: string
  manufacturers?: string[]
  categories?: string[]
  yearRange?: [number, number]
}

// =============================================================================
// Q1 Flight: Revenue Sum Queries
// =============================================================================

/**
 * Q1.1: Sum of revenue for year with discount and quantity filters
 *
 * SQL equivalent:
 * SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 * FROM lineorder, date
 * WHERE lo_orderdate = d_datekey
 *   AND d_year = 1993
 *   AND lo_discount BETWEEN 1 AND 3
 *   AND lo_quantity < 25
 */
export async function runQ11(db: DBInstance, params: Q1Params): Promise<Q1Result> {
  const { year, discountLo, discountHi, quantityLt } = params

  // Using SQL interface for efficient aggregation
  const result = await db.sql`
    SELECT SUM(lo_extendedprice * lo_discount) as revenue
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    WHERE d.d_year = ${year}
      AND lo.lo_discount >= ${discountLo}
      AND lo.lo_discount <= ${discountHi}
      AND lo.lo_quantity < ${quantityLt}
  `

  return { revenue: result[0]?.revenue ?? 0 }
}

/**
 * Q1.2: Sum of revenue for specific year-month with discount and quantity filters
 *
 * SQL equivalent:
 * SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 * FROM lineorder, date
 * WHERE lo_orderdate = d_datekey
 *   AND d_yearmonthnum = 199401
 *   AND lo_discount BETWEEN 4 AND 6
 *   AND lo_quantity BETWEEN 26 AND 35
 */
export async function runQ12(db: DBInstance, params: Q1Params): Promise<Q1Result> {
  const { yearmonthnum = 199401, discountLo, discountHi, quantityLt } = params

  const result = await db.sql`
    SELECT SUM(lo_extendedprice * lo_discount) as revenue
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    WHERE d.d_yearmonthnum = ${yearmonthnum}
      AND lo.lo_discount >= ${discountLo}
      AND lo.lo_discount <= ${discountHi}
      AND lo.lo_quantity >= ${quantityLt - 9}
      AND lo.lo_quantity <= ${quantityLt}
  `

  return { revenue: result[0]?.revenue ?? 0 }
}

/**
 * Q1.3: Sum of revenue for specific week with discount and quantity filters
 *
 * SQL equivalent:
 * SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 * FROM lineorder, date
 * WHERE lo_orderdate = d_datekey
 *   AND d_weeknuminyear = 6
 *   AND d_year = 1994
 *   AND lo_discount BETWEEN 5 AND 7
 *   AND lo_quantity BETWEEN 26 AND 35
 */
export async function runQ13(db: DBInstance, params: Q1Params): Promise<Q1Result> {
  const { year, weeknuminyear = 6, discountLo, discountHi, quantityLt } = params

  const result = await db.sql`
    SELECT SUM(lo_extendedprice * lo_discount) as revenue
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    WHERE d.d_weeknuminyear = ${weeknuminyear}
      AND d.d_year = ${year}
      AND lo.lo_discount >= ${discountLo}
      AND lo.lo_discount <= ${discountHi}
      AND lo.lo_quantity >= ${quantityLt - 9}
      AND lo.lo_quantity <= ${quantityLt}
  `

  return { revenue: result[0]?.revenue ?? 0 }
}

// =============================================================================
// Q2 Flight: Revenue by Brand/Category
// =============================================================================

/**
 * Q2.1: Revenue by product brand for region AMERICA and category MFGR#12
 *
 * SQL equivalent:
 * SELECT SUM(lo_revenue), d_year, p_brand1
 * FROM lineorder, date, part, supplier
 * WHERE lo_orderdate = d_datekey
 *   AND lo_partkey = p_partkey
 *   AND lo_suppkey = s_suppkey
 *   AND p_category = 'MFGR#12'
 *   AND s_region = 'AMERICA'
 * GROUP BY d_year, p_brand1
 * ORDER BY d_year, p_brand1
 */
export async function runQ21(db: DBInstance, params: Q2Params): Promise<Q2Result[]> {
  const { region, category = 'MFGR#12' } = params

  const result = await db.sql`
    SELECT d.d_year, p.p_brand1, SUM(lo.lo_revenue) as revenue
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    JOIN part p ON lo.lo_partkey = p.p_partkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    WHERE p.p_category = ${category}
      AND s.s_region = ${region}
    GROUP BY d.d_year, p.p_brand1
    ORDER BY d.d_year, p.p_brand1
  `

  return result.map((r: { d_year: number; p_brand1: string; revenue: number }) => ({
    d_year: r.d_year,
    p_brand1: r.p_brand1,
    revenue: r.revenue,
  }))
}

/**
 * Q2.2: Revenue by brand for region ASIA with brands between MFGR#2221 and MFGR#2228
 *
 * SQL equivalent:
 * SELECT SUM(lo_revenue), d_year, p_brand1
 * FROM lineorder, date, part, supplier
 * WHERE lo_orderdate = d_datekey
 *   AND lo_partkey = p_partkey
 *   AND lo_suppkey = s_suppkey
 *   AND p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228'
 *   AND s_region = 'ASIA'
 * GROUP BY d_year, p_brand1
 * ORDER BY d_year, p_brand1
 */
export async function runQ22(db: DBInstance, params: Q2Params): Promise<Q2Result[]> {
  const { region, brands = ['MFGR#2221', 'MFGR#2228'] } = params
  const [brandLo, brandHi] = brands

  const result = await db.sql`
    SELECT d.d_year, p.p_brand1, SUM(lo.lo_revenue) as revenue
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    JOIN part p ON lo.lo_partkey = p.p_partkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    WHERE p.p_brand1 >= ${brandLo}
      AND p.p_brand1 <= ${brandHi}
      AND s.s_region = ${region}
    GROUP BY d.d_year, p.p_brand1
    ORDER BY d.d_year, p.p_brand1
  `

  return result.map((r: { d_year: number; p_brand1: string; revenue: number }) => ({
    d_year: r.d_year,
    p_brand1: r.p_brand1,
    revenue: r.revenue,
  }))
}

/**
 * Q2.3: Revenue by brand for region EUROPE with specific brand MFGR#2239
 *
 * SQL equivalent:
 * SELECT SUM(lo_revenue), d_year, p_brand1
 * FROM lineorder, date, part, supplier
 * WHERE lo_orderdate = d_datekey
 *   AND lo_partkey = p_partkey
 *   AND lo_suppkey = s_suppkey
 *   AND p_brand1 = 'MFGR#2239'
 *   AND s_region = 'EUROPE'
 * GROUP BY d_year, p_brand1
 * ORDER BY d_year, p_brand1
 */
export async function runQ23(db: DBInstance, params: Q2Params): Promise<Q2Result[]> {
  const { region, brands = ['MFGR#2239'] } = params
  const brand = brands[0]

  const result = await db.sql`
    SELECT d.d_year, p.p_brand1, SUM(lo.lo_revenue) as revenue
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    JOIN part p ON lo.lo_partkey = p.p_partkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    WHERE p.p_brand1 = ${brand}
      AND s.s_region = ${region}
    GROUP BY d.d_year, p.p_brand1
    ORDER BY d.d_year, p.p_brand1
  `

  return result.map((r: { d_year: number; p_brand1: string; revenue: number }) => ({
    d_year: r.d_year,
    p_brand1: r.p_brand1,
    revenue: r.revenue,
  }))
}

// =============================================================================
// Q3 Flight: Revenue by Customer/Supplier Nation/City
// =============================================================================

/**
 * Q3.1: Revenue by customer and supplier nation for ASIA region
 *
 * SQL equivalent:
 * SELECT c_nation, s_nation, d_year, SUM(lo_revenue) AS revenue
 * FROM customer, lineorder, supplier, date
 * WHERE lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_orderdate = d_datekey
 *   AND c_region = 'ASIA'
 *   AND s_region = 'ASIA'
 *   AND d_year >= 1992 AND d_year <= 1997
 * GROUP BY c_nation, s_nation, d_year
 * ORDER BY d_year ASC, revenue DESC
 */
export async function runQ31(db: DBInstance, params: Q3Params): Promise<Q3Result[]> {
  const { customerRegion = 'ASIA', supplierRegion = 'ASIA', yearRange = [1992, 1997] } = params
  const [yearLo, yearHi] = yearRange

  const result = await db.sql`
    SELECT c.c_nation, s.s_nation, d.d_year, SUM(lo.lo_revenue) as revenue
    FROM lineorder lo
    JOIN customer c ON lo.lo_custkey = c.c_custkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    WHERE c.c_region = ${customerRegion}
      AND s.s_region = ${supplierRegion}
      AND d.d_year >= ${yearLo}
      AND d.d_year <= ${yearHi}
    GROUP BY c.c_nation, s.s_nation, d.d_year
    ORDER BY d.d_year ASC, revenue DESC
  `

  return result.map((r: { c_nation: string; s_nation: string; d_year: number; revenue: number }) => ({
    c_nation: r.c_nation,
    s_nation: r.s_nation,
    d_year: r.d_year,
    revenue: r.revenue,
  }))
}

/**
 * Q3.2: Revenue by customer and supplier city for UNITED STATES
 *
 * SQL equivalent:
 * SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue
 * FROM customer, lineorder, supplier, date
 * WHERE lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_orderdate = d_datekey
 *   AND c_nation = 'UNITED STATES'
 *   AND s_nation = 'UNITED STATES'
 *   AND d_year >= 1992 AND d_year <= 1997
 * GROUP BY c_city, s_city, d_year
 * ORDER BY d_year ASC, revenue DESC
 */
export async function runQ32(db: DBInstance, params: Q3Params): Promise<Q3Result[]> {
  const { customerNation = 'UNITED STATES', supplierNation = 'UNITED STATES', yearRange = [1992, 1997] } = params
  const [yearLo, yearHi] = yearRange

  const result = await db.sql`
    SELECT c.c_city, s.s_city, d.d_year, SUM(lo.lo_revenue) as revenue
    FROM lineorder lo
    JOIN customer c ON lo.lo_custkey = c.c_custkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    WHERE c.c_nation = ${customerNation}
      AND s.s_nation = ${supplierNation}
      AND d.d_year >= ${yearLo}
      AND d.d_year <= ${yearHi}
    GROUP BY c.c_city, s.s_city, d.d_year
    ORDER BY d.d_year ASC, revenue DESC
  `

  return result.map((r: { c_city: string; s_city: string; d_year: number; revenue: number }) => ({
    c_city: r.c_city,
    s_city: r.s_city,
    d_year: r.d_year,
    revenue: r.revenue,
  }))
}

/**
 * Q3.3: Revenue by specific customer and supplier cities
 *
 * SQL equivalent:
 * SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue
 * FROM customer, lineorder, supplier, date
 * WHERE lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_orderdate = d_datekey
 *   AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
 *   AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
 *   AND d_year >= 1992 AND d_year <= 1997
 * GROUP BY c_city, s_city, d_year
 * ORDER BY d_year ASC, revenue DESC
 */
export async function runQ33(db: DBInstance, params: Q3Params): Promise<Q3Result[]> {
  const {
    customerCities = ['UNITED KI1', 'UNITED KI5'],
    supplierCities = ['UNITED KI1', 'UNITED KI5'],
    yearRange = [1992, 1997],
  } = params
  const [yearLo, yearHi] = yearRange

  const result = await db.sql`
    SELECT c.c_city, s.s_city, d.d_year, SUM(lo.lo_revenue) as revenue
    FROM lineorder lo
    JOIN customer c ON lo.lo_custkey = c.c_custkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    WHERE (c.c_city = ${customerCities[0]} OR c.c_city = ${customerCities[1]})
      AND (s.s_city = ${supplierCities[0]} OR s.s_city = ${supplierCities[1]})
      AND d.d_year >= ${yearLo}
      AND d.d_year <= ${yearHi}
    GROUP BY c.c_city, s.s_city, d.d_year
    ORDER BY d.d_year ASC, revenue DESC
  `

  return result.map((r: { c_city: string; s_city: string; d_year: number; revenue: number }) => ({
    c_city: r.c_city,
    s_city: r.s_city,
    d_year: r.d_year,
    revenue: r.revenue,
  }))
}

/**
 * Q3.4: Revenue by city for specific cities with yearmonth filter
 *
 * SQL equivalent:
 * SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue
 * FROM customer, lineorder, supplier, date
 * WHERE lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_orderdate = d_datekey
 *   AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
 *   AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
 *   AND d_yearmonth = 'Dec1997'
 * GROUP BY c_city, s_city, d_year
 * ORDER BY d_year ASC, revenue DESC
 */
export async function runQ34(db: DBInstance, params: Q3Params & { yearmonth?: string }): Promise<Q3Result[]> {
  const {
    customerCities = ['UNITED KI1', 'UNITED KI5'],
    supplierCities = ['UNITED KI1', 'UNITED KI5'],
  } = params
  const yearmonth = (params as { yearmonth?: string }).yearmonth ?? 'Dec1997'

  const result = await db.sql`
    SELECT c.c_city, s.s_city, d.d_year, SUM(lo.lo_revenue) as revenue
    FROM lineorder lo
    JOIN customer c ON lo.lo_custkey = c.c_custkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    WHERE (c.c_city = ${customerCities[0]} OR c.c_city = ${customerCities[1]})
      AND (s.s_city = ${supplierCities[0]} OR s.s_city = ${supplierCities[1]})
      AND d.d_yearmonth = ${yearmonth}
    GROUP BY c.c_city, s.s_city, d.d_year
    ORDER BY d.d_year ASC, revenue DESC
  `

  return result.map((r: { c_city: string; s_city: string; d_year: number; revenue: number }) => ({
    c_city: r.c_city,
    s_city: r.s_city,
    d_year: r.d_year,
    revenue: r.revenue,
  }))
}

// =============================================================================
// Q4 Flight: Profit Queries
// =============================================================================

/**
 * Q4.1: Profit by year and customer nation for AMERICA region with specific manufacturers
 *
 * SQL equivalent:
 * SELECT d_year, c_nation, SUM(lo_revenue - lo_supplycost) AS profit
 * FROM date, customer, supplier, part, lineorder
 * WHERE lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_partkey = p_partkey
 *   AND lo_orderdate = d_datekey
 *   AND c_region = 'AMERICA'
 *   AND s_region = 'AMERICA'
 *   AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
 * GROUP BY d_year, c_nation
 * ORDER BY d_year, c_nation
 */
export async function runQ41(db: DBInstance, params: Q4Params): Promise<Q4Result[]> {
  const {
    customerRegion = 'AMERICA',
    supplierRegion = 'AMERICA',
    manufacturers = ['MFGR#1', 'MFGR#2'],
  } = params

  const result = await db.sql`
    SELECT d.d_year, c.c_nation, SUM(lo.lo_revenue - lo.lo_supplycost) as profit
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    JOIN customer c ON lo.lo_custkey = c.c_custkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    JOIN part p ON lo.lo_partkey = p.p_partkey
    WHERE c.c_region = ${customerRegion}
      AND s.s_region = ${supplierRegion}
      AND (p.p_mfgr = ${manufacturers[0]} OR p.p_mfgr = ${manufacturers[1]})
    GROUP BY d.d_year, c.c_nation
    ORDER BY d.d_year, c.c_nation
  `

  return result.map((r: { d_year: number; c_nation: string; profit: number }) => ({
    d_year: r.d_year,
    c_nation: r.c_nation,
    profit: r.profit,
  }))
}

/**
 * Q4.2: Profit by year and customer nation for AMERICA with category filter
 *
 * SQL equivalent:
 * SELECT d_year, s_nation, p_category, SUM(lo_revenue - lo_supplycost) AS profit
 * FROM date, customer, supplier, part, lineorder
 * WHERE lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_partkey = p_partkey
 *   AND lo_orderdate = d_datekey
 *   AND c_region = 'AMERICA'
 *   AND s_region = 'AMERICA'
 *   AND (d_year = 1997 OR d_year = 1998)
 *   AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
 * GROUP BY d_year, s_nation, p_category
 * ORDER BY d_year, s_nation, p_category
 */
export async function runQ42(db: DBInstance, params: Q4Params): Promise<Q4Result[]> {
  const {
    customerRegion = 'AMERICA',
    supplierRegion = 'AMERICA',
    manufacturers = ['MFGR#1', 'MFGR#2'],
    yearRange = [1997, 1998],
  } = params
  const [yearLo, yearHi] = yearRange

  const result = await db.sql`
    SELECT d.d_year, s.s_nation, p.p_category, SUM(lo.lo_revenue - lo.lo_supplycost) as profit
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    JOIN customer c ON lo.lo_custkey = c.c_custkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    JOIN part p ON lo.lo_partkey = p.p_partkey
    WHERE c.c_region = ${customerRegion}
      AND s.s_region = ${supplierRegion}
      AND d.d_year >= ${yearLo}
      AND d.d_year <= ${yearHi}
      AND (p.p_mfgr = ${manufacturers[0]} OR p.p_mfgr = ${manufacturers[1]})
    GROUP BY d.d_year, s.s_nation, p.p_category
    ORDER BY d.d_year, s.s_nation, p.p_category
  `

  return result.map((r: { d_year: number; s_nation: string; p_category: string; profit: number }) => ({
    d_year: r.d_year,
    c_nation: r.s_nation,  // Note: SSB spec uses s_nation here
    p_category: r.p_category,
    profit: r.profit,
  }))
}

/**
 * Q4.3: Profit by year and city for specific nation and category
 *
 * SQL equivalent:
 * SELECT d_year, s_city, p_brand1, SUM(lo_revenue - lo_supplycost) AS profit
 * FROM date, customer, supplier, part, lineorder
 * WHERE lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_partkey = p_partkey
 *   AND lo_orderdate = d_datekey
 *   AND c_region = 'AMERICA'
 *   AND s_nation = 'UNITED STATES'
 *   AND (d_year = 1997 OR d_year = 1998)
 *   AND p_category = 'MFGR#14'
 * GROUP BY d_year, s_city, p_brand1
 * ORDER BY d_year, s_city, p_brand1
 */
export async function runQ43(db: DBInstance, params: Q4Params): Promise<Q4Result[]> {
  const {
    customerRegion = 'AMERICA',
    supplierNation = 'UNITED STATES',
    categories = ['MFGR#14'],
    yearRange = [1997, 1998],
  } = params
  const category = categories[0]
  const [yearLo, yearHi] = yearRange

  const result = await db.sql`
    SELECT d.d_year, s.s_city, p.p_brand1, SUM(lo.lo_revenue - lo.lo_supplycost) as profit
    FROM lineorder lo
    JOIN datedim d ON lo.lo_orderdate = d.d_datekey
    JOIN customer c ON lo.lo_custkey = c.c_custkey
    JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
    JOIN part p ON lo.lo_partkey = p.p_partkey
    WHERE c.c_region = ${customerRegion}
      AND s.s_nation = ${supplierNation}
      AND d.d_year >= ${yearLo}
      AND d.d_year <= ${yearHi}
      AND p.p_category = ${category}
    GROUP BY d.d_year, s.s_city, p.p_brand1
    ORDER BY d.d_year, s.s_city, p.p_brand1
  `

  return result.map((r: { d_year: number; s_city: string; p_brand1: string; profit: number }) => ({
    d_year: r.d_year,
    c_city: r.s_city,  // Using s_city per SSB spec
    p_category: r.p_brand1,  // p_brand1 in result
    profit: r.profit,
  }))
}

// =============================================================================
// Run All Queries
// =============================================================================

/**
 * Run all 13 SSB queries with default parameters
 */
export async function runSSBQueries(db: DBInstance): Promise<SSBQueryResults> {
  console.log('Running SSB Query Suite...\n')

  // Q1 Flight
  console.log('Q1 Flight: Revenue Sum Queries')
  const q11 = await runQ11(db, { year: 1993, discountLo: 1, discountHi: 3, quantityLt: 25 })
  console.log('  Q1.1:', q11.revenue)

  const q12 = await runQ12(db, { year: 1994, yearmonthnum: 199401, discountLo: 4, discountHi: 6, quantityLt: 35 })
  console.log('  Q1.2:', q12.revenue)

  const q13 = await runQ13(db, { year: 1994, weeknuminyear: 6, discountLo: 5, discountHi: 7, quantityLt: 35 })
  console.log('  Q1.3:', q13.revenue)

  // Q2 Flight
  console.log('\nQ2 Flight: Revenue by Brand/Category')
  const q21 = await runQ21(db, { region: 'AMERICA', category: 'MFGR#12' })
  console.log('  Q2.1:', q21.length, 'rows')

  const q22 = await runQ22(db, { region: 'ASIA', brands: ['MFGR#2221', 'MFGR#2228'] })
  console.log('  Q2.2:', q22.length, 'rows')

  const q23 = await runQ23(db, { region: 'EUROPE', brands: ['MFGR#2239'] })
  console.log('  Q2.3:', q23.length, 'rows')

  // Q3 Flight
  console.log('\nQ3 Flight: Revenue by Customer/Supplier')
  const q31 = await runQ31(db, { customerRegion: 'ASIA', supplierRegion: 'ASIA', yearRange: [1992, 1997] })
  console.log('  Q3.1:', q31.length, 'rows')

  const q32 = await runQ32(db, { customerNation: 'UNITED STATES', supplierNation: 'UNITED STATES', yearRange: [1992, 1997] })
  console.log('  Q3.2:', q32.length, 'rows')

  const q33 = await runQ33(db, {
    customerCities: ['UNITED KI1', 'UNITED KI5'],
    supplierCities: ['UNITED KI1', 'UNITED KI5'],
    yearRange: [1992, 1997],
  })
  console.log('  Q3.3:', q33.length, 'rows')

  const q34 = await runQ34(db, {
    customerCities: ['UNITED KI1', 'UNITED KI5'],
    supplierCities: ['UNITED KI1', 'UNITED KI5'],
    yearmonth: 'Dec1997',
  })
  console.log('  Q3.4:', q34.length, 'rows')

  // Q4 Flight
  console.log('\nQ4 Flight: Profit Queries')
  const q41 = await runQ41(db, {
    customerRegion: 'AMERICA',
    supplierRegion: 'AMERICA',
    manufacturers: ['MFGR#1', 'MFGR#2'],
  })
  console.log('  Q4.1:', q41.length, 'rows')

  const q42 = await runQ42(db, {
    customerRegion: 'AMERICA',
    supplierRegion: 'AMERICA',
    manufacturers: ['MFGR#1', 'MFGR#2'],
    yearRange: [1997, 1998],
  })
  console.log('  Q4.2:', q42.length, 'rows')

  const q43 = await runQ43(db, {
    customerRegion: 'AMERICA',
    supplierNation: 'UNITED STATES',
    categories: ['MFGR#14'],
    yearRange: [1997, 1998],
  })
  console.log('  Q4.3:', q43.length, 'rows')

  return {
    q11, q12, q13,
    q21, q22, q23,
    q31, q32, q33, q34,
    q41, q42, q43,
  }
}

// =============================================================================
// Alternative: MongoDB-style API Queries
// =============================================================================

/**
 * Example using MongoDB-style find() API instead of SQL
 * (Less efficient for analytical queries but demonstrates the API)
 */
export async function runQ11WithFindAPI(db: DBInstance, params: Q1Params): Promise<Q1Result> {
  const { year, discountLo, discountHi, quantityLt } = params

  // Get date keys for the year
  const dates = await db.DateDim.find({ d_year: year })
  const dateKeys = new Set(dates.map((d: { d_datekey: number }) => d.d_datekey))

  // Find matching line orders
  const lineOrders = await db.LineOrder.find({
    lo_discount: { $gte: discountLo, $lte: discountHi },
    lo_quantity: { $lt: quantityLt },
  })

  // Filter by date and sum revenue
  let revenue = 0
  for (const lo of lineOrders) {
    if (dateKeys.has(lo.lo_orderdate)) {
      revenue += lo.lo_extendedprice * lo.lo_discount
    }
  }

  return { revenue }
}
