/**
 * TPC-H Benchmark Queries for ParqueDB
 *
 * Implements 8 key TPC-H queries:
 * - Q1: Pricing Summary Report
 * - Q3: Shipping Priority
 * - Q5: Local Supplier Volume
 * - Q6: Forecasting Revenue Change
 * - Q10: Returned Item Reporting
 * - Q12: Shipping Modes and Order Priority
 * - Q14: Promotion Effect
 * - Q19: Discounted Revenue
 */

import type { DBInstance } from '../../src/db'

// =============================================================================
// Query Result Types
// =============================================================================

export interface Q1Result {
  l_returnflag: string
  l_linestatus: string
  sum_qty: number
  sum_base_price: number
  sum_disc_price: number
  sum_charge: number
  avg_qty: number
  avg_price: number
  avg_disc: number
  count_order: number
}

export interface Q3Result {
  l_orderkey: number
  revenue: number
  o_orderdate: string
  o_shippriority: number
}

export interface Q5Result {
  n_name: string
  revenue: number
}

export interface Q6Result {
  revenue: number
}

export interface Q10Result {
  c_custkey: number
  c_name: string
  revenue: number
  c_acctbal: number
  n_name: string
  c_address: string
  c_phone: string
  c_comment: string
}

export interface Q12Result {
  l_shipmode: string
  high_line_count: number
  low_line_count: number
}

export interface Q14Result {
  promo_revenue: number
}

export interface Q19Result {
  revenue: number
}

export interface TPCHQueryResults {
  q1: Q1Result[]
  q3: Q3Result[]
  q5: Q5Result[]
  q6: Q6Result
  q10: Q10Result[]
  q12: Q12Result[]
  q14: Q14Result
  q19: Q19Result
}

// =============================================================================
// Query Parameters
// =============================================================================

export interface Q1Params {
  /** Filter date: l_shipdate <= date */
  date?: string
}

export interface Q3Params {
  /** Market segment filter */
  segment?: string
  /** Order date cutoff */
  date?: string
}

export interface Q5Params {
  /** Region name filter */
  region?: string
  /** Year filter */
  year?: number
}

export interface Q6Params {
  /** Year filter */
  year?: number
  /** Discount range low */
  discountLo?: number
  /** Discount range high */
  discountHi?: number
  /** Quantity threshold */
  quantity?: number
}

export interface Q10Params {
  /** Year filter */
  year?: number
  /** Quarter filter (1-4) */
  quarter?: number
}

export interface Q12Params {
  /** Ship modes to analyze */
  shipModes?: [string, string]
  /** Year filter */
  year?: number
}

export interface Q14Params {
  /** Year-month filter (YYYY-MM-DD for first day of month) */
  date?: string
}

export interface Q19Params {
  /** Brand filter */
  brand1?: string
  brand2?: string
  brand3?: string
  /** Quantity ranges */
  quantity1Lo?: number
  quantity1Hi?: number
  quantity2Lo?: number
  quantity2Hi?: number
  quantity3Lo?: number
  quantity3Hi?: number
}

// =============================================================================
// Q1: Pricing Summary Report
// =============================================================================

/**
 * Q1: Pricing Summary Report
 *
 * Reports the amount of business billed, shipped, and returned.
 * Aggregates line items by return flag and line status with a date filter.
 *
 * SQL equivalent:
 * SELECT
 *   l_returnflag,
 *   l_linestatus,
 *   SUM(l_quantity) AS sum_qty,
 *   SUM(l_extendedprice) AS sum_base_price,
 *   SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
 *   SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
 *   AVG(l_quantity) AS avg_qty,
 *   AVG(l_extendedprice) AS avg_price,
 *   AVG(l_discount) AS avg_disc,
 *   COUNT(*) AS count_order
 * FROM lineitem
 * WHERE l_shipdate <= '1998-09-02'
 * GROUP BY l_returnflag, l_linestatus
 * ORDER BY l_returnflag, l_linestatus
 */
export async function runQ1(db: DBInstance, params: Q1Params = {}): Promise<Q1Result[]> {
  const { date = '1998-09-02' } = params

  const result = await db.sql`
    SELECT
      l_returnflag,
      l_linestatus,
      SUM(l_quantity) AS sum_qty,
      SUM(l_extendedprice) AS sum_base_price,
      SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
      SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
      AVG(l_quantity) AS avg_qty,
      AVG(l_extendedprice) AS avg_price,
      AVG(l_discount) AS avg_disc,
      COUNT(*) AS count_order
    FROM lineitem
    WHERE l_shipdate <= ${date}
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
  `

  return result.rows.map((r: Record<string, unknown>) => ({
    l_returnflag: r.l_returnflag as string,
    l_linestatus: r.l_linestatus as string,
    sum_qty: Number(r.sum_qty),
    sum_base_price: Number(r.sum_base_price),
    sum_disc_price: Number(r.sum_disc_price),
    sum_charge: Number(r.sum_charge),
    avg_qty: Number(r.avg_qty),
    avg_price: Number(r.avg_price),
    avg_disc: Number(r.avg_disc),
    count_order: Number(r.count_order),
  }))
}

// =============================================================================
// Q3: Shipping Priority
// =============================================================================

/**
 * Q3: Shipping Priority
 *
 * Retrieves the shipping priority and potential revenue of orders
 * not yet shipped, by market segment.
 *
 * SQL equivalent:
 * SELECT
 *   l_orderkey,
 *   SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *   o_orderdate,
 *   o_shippriority
 * FROM customer, orders, lineitem
 * WHERE c_mktsegment = 'BUILDING'
 *   AND c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate < '1995-03-15'
 *   AND l_shipdate > '1995-03-15'
 * GROUP BY l_orderkey, o_orderdate, o_shippriority
 * ORDER BY revenue DESC, o_orderdate
 * LIMIT 10
 */
export async function runQ3(db: DBInstance, params: Q3Params = {}): Promise<Q3Result[]> {
  const { segment = 'BUILDING', date = '1995-03-15' } = params

  const result = await db.sql`
    SELECT
      l.l_orderkey,
      SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
      o.o_orderdate,
      o.o_shippriority
    FROM lineitem l
    JOIN "order" o ON l.l_orderkey = o.o_orderkey
    JOIN customer c ON o.o_custkey = c.c_custkey
    WHERE c.c_mktsegment = ${segment}
      AND o.o_orderdate < ${date}
      AND l.l_shipdate > ${date}
    GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
    ORDER BY revenue DESC, o.o_orderdate
    LIMIT 10
  `

  return result.rows.map((r: Record<string, unknown>) => ({
    l_orderkey: Number(r.l_orderkey),
    revenue: Number(r.revenue),
    o_orderdate: r.o_orderdate as string,
    o_shippriority: Number(r.o_shippriority),
  }))
}

// =============================================================================
// Q5: Local Supplier Volume
// =============================================================================

/**
 * Q5: Local Supplier Volume
 *
 * Lists the revenue volume done through local suppliers in a region.
 * Customers and suppliers must be in the same nation within the region.
 *
 * SQL equivalent:
 * SELECT
 *   n_name,
 *   SUM(l_extendedprice * (1 - l_discount)) AS revenue
 * FROM customer, orders, lineitem, supplier, nation, region
 * WHERE c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND l_suppkey = s_suppkey
 *   AND c_nationkey = s_nationkey
 *   AND s_nationkey = n_nationkey
 *   AND n_regionkey = r_regionkey
 *   AND r_name = 'ASIA'
 *   AND o_orderdate >= '1994-01-01'
 *   AND o_orderdate < '1995-01-01'
 * GROUP BY n_name
 * ORDER BY revenue DESC
 */
export async function runQ5(db: DBInstance, params: Q5Params = {}): Promise<Q5Result[]> {
  const { region = 'ASIA', year = 1994 } = params
  const startDate = `${year}-01-01`
  const endDate = `${year + 1}-01-01`

  const result = await db.sql`
    SELECT
      n.n_name,
      SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
    FROM lineitem l
    JOIN "order" o ON l.l_orderkey = o.o_orderkey
    JOIN customer c ON o.o_custkey = c.c_custkey
    JOIN supplier s ON l.l_suppkey = s.s_suppkey
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    JOIN region r ON n.n_regionkey = r.r_regionkey
    WHERE c.c_nationkey = s.s_nationkey
      AND r.r_name = ${region}
      AND o.o_orderdate >= ${startDate}
      AND o.o_orderdate < ${endDate}
    GROUP BY n.n_name
    ORDER BY revenue DESC
  `

  return result.rows.map((r: Record<string, unknown>) => ({
    n_name: r.n_name as string,
    revenue: Number(r.revenue),
  }))
}

// =============================================================================
// Q6: Forecasting Revenue Change
// =============================================================================

/**
 * Q6: Forecasting Revenue Change
 *
 * Quantifies the amount of revenue increase that would have resulted
 * from eliminating certain discounts in a given year.
 *
 * SQL equivalent:
 * SELECT SUM(l_extendedprice * l_discount) AS revenue
 * FROM lineitem
 * WHERE l_shipdate >= '1994-01-01'
 *   AND l_shipdate < '1995-01-01'
 *   AND l_discount BETWEEN 0.05 AND 0.07
 *   AND l_quantity < 24
 */
export async function runQ6(db: DBInstance, params: Q6Params = {}): Promise<Q6Result> {
  const {
    year = 1994,
    discountLo = 0.05,
    discountHi = 0.07,
    quantity = 24,
  } = params

  const startDate = `${year}-01-01`
  const endDate = `${year + 1}-01-01`

  const result = await db.sql`
    SELECT SUM(l_extendedprice * l_discount) AS revenue
    FROM lineitem
    WHERE l_shipdate >= ${startDate}
      AND l_shipdate < ${endDate}
      AND l_discount >= ${discountLo}
      AND l_discount <= ${discountHi}
      AND l_quantity < ${quantity}
  `

  return { revenue: Number(result.rows[0]?.revenue ?? 0) }
}

// =============================================================================
// Q10: Returned Item Reporting
// =============================================================================

/**
 * Q10: Returned Item Reporting
 *
 * Identifies customers who have returned parts and the amount of
 * revenue lost through returned orders.
 *
 * SQL equivalent:
 * SELECT
 *   c_custkey, c_name,
 *   SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *   c_acctbal, n_name, c_address, c_phone, c_comment
 * FROM customer, orders, lineitem, nation
 * WHERE c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate >= '1993-10-01'
 *   AND o_orderdate < '1994-01-01'
 *   AND l_returnflag = 'R'
 *   AND c_nationkey = n_nationkey
 * GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
 * ORDER BY revenue DESC
 * LIMIT 20
 */
export async function runQ10(db: DBInstance, params: Q10Params = {}): Promise<Q10Result[]> {
  const { year = 1993, quarter = 4 } = params

  // Calculate quarter start/end dates
  const startMonth = (quarter - 1) * 3 + 1
  const endMonth = startMonth + 3
  const endYear = endMonth > 12 ? year + 1 : year
  const endMonthNorm = endMonth > 12 ? endMonth - 12 : endMonth

  const startDate = `${year}-${startMonth.toString().padStart(2, '0')}-01`
  const endDate = `${endYear}-${endMonthNorm.toString().padStart(2, '0')}-01`

  const result = await db.sql`
    SELECT
      c.c_custkey,
      c.c_name,
      SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
      c.c_acctbal,
      n.n_name,
      c.c_address,
      c.c_phone,
      c.c_comment
    FROM lineitem l
    JOIN "order" o ON l.l_orderkey = o.o_orderkey
    JOIN customer c ON o.o_custkey = c.c_custkey
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    WHERE o.o_orderdate >= ${startDate}
      AND o.o_orderdate < ${endDate}
      AND l.l_returnflag = 'R'
    GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment
    ORDER BY revenue DESC
    LIMIT 20
  `

  return result.rows.map((r: Record<string, unknown>) => ({
    c_custkey: Number(r.c_custkey),
    c_name: r.c_name as string,
    revenue: Number(r.revenue),
    c_acctbal: Number(r.c_acctbal),
    n_name: r.n_name as string,
    c_address: r.c_address as string,
    c_phone: r.c_phone as string,
    c_comment: r.c_comment as string,
  }))
}

// =============================================================================
// Q12: Shipping Modes and Order Priority
// =============================================================================

/**
 * Q12: Shipping Modes and Order Priority
 *
 * Determines whether selecting less expensive shipping modes
 * is negatively affecting the critical-priority orders.
 *
 * SQL equivalent:
 * SELECT
 *   l_shipmode,
 *   SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
 *       THEN 1 ELSE 0 END) AS high_line_count,
 *   SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
 *       THEN 1 ELSE 0 END) AS low_line_count
 * FROM orders, lineitem
 * WHERE o_orderkey = l_orderkey
 *   AND l_shipmode IN ('MAIL', 'SHIP')
 *   AND l_commitdate < l_receiptdate
 *   AND l_shipdate < l_commitdate
 *   AND l_receiptdate >= '1994-01-01'
 *   AND l_receiptdate < '1995-01-01'
 * GROUP BY l_shipmode
 * ORDER BY l_shipmode
 */
export async function runQ12(db: DBInstance, params: Q12Params = {}): Promise<Q12Result[]> {
  const { shipModes = ['MAIL', 'SHIP'], year = 1994 } = params
  const startDate = `${year}-01-01`
  const endDate = `${year + 1}-01-01`

  const result = await db.sql`
    SELECT
      l.l_shipmode,
      SUM(CASE
        WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH'
        THEN 1 ELSE 0 END) AS high_line_count,
      SUM(CASE
        WHEN o.o_orderpriority <> '1-URGENT' AND o.o_orderpriority <> '2-HIGH'
        THEN 1 ELSE 0 END) AS low_line_count
    FROM lineitem l
    JOIN "order" o ON l.l_orderkey = o.o_orderkey
    WHERE (l.l_shipmode = ${shipModes[0]} OR l.l_shipmode = ${shipModes[1]})
      AND l.l_commitdate < l.l_receiptdate
      AND l.l_shipdate < l.l_commitdate
      AND l.l_receiptdate >= ${startDate}
      AND l.l_receiptdate < ${endDate}
    GROUP BY l.l_shipmode
    ORDER BY l.l_shipmode
  `

  return result.rows.map((r: Record<string, unknown>) => ({
    l_shipmode: r.l_shipmode as string,
    high_line_count: Number(r.high_line_count),
    low_line_count: Number(r.low_line_count),
  }))
}

// =============================================================================
// Q14: Promotion Effect
// =============================================================================

/**
 * Q14: Promotion Effect
 *
 * Monitors the market response to a promotion such as TV ads
 * or a special campaign.
 *
 * SQL equivalent:
 * SELECT
 *   100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
 *       THEN l_extendedprice * (1 - l_discount) ELSE 0 END) /
 *   SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
 * FROM lineitem, part
 * WHERE l_partkey = p_partkey
 *   AND l_shipdate >= '1995-09-01'
 *   AND l_shipdate < '1995-10-01'
 */
export async function runQ14(db: DBInstance, params: Q14Params = {}): Promise<Q14Result> {
  const { date = '1995-09-01' } = params

  // Calculate end date (next month)
  const startDate = new Date(date)
  const endDate = new Date(startDate)
  endDate.setMonth(endDate.getMonth() + 1)
  const endDateStr = endDate.toISOString().split('T')[0]

  const result = await db.sql`
    SELECT
      100.00 * SUM(CASE
        WHEN p.p_type LIKE 'PROMO%'
        THEN l.l_extendedprice * (1 - l.l_discount)
        ELSE 0 END) /
      SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
    FROM lineitem l
    JOIN part p ON l.l_partkey = p.p_partkey
    WHERE l.l_shipdate >= ${date}
      AND l.l_shipdate < ${endDateStr}
  `

  return { promo_revenue: Number(result.rows[0]?.promo_revenue ?? 0) }
}

// =============================================================================
// Q19: Discounted Revenue
// =============================================================================

/**
 * Q19: Discounted Revenue
 *
 * Reports the gross discounted revenue attributed to the sale of
 * parts for certain brands and containers at specific quantities.
 *
 * SQL equivalent:
 * SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
 * FROM lineitem, part
 * WHERE (
 *   p_partkey = l_partkey
 *   AND p_brand = 'Brand#12'
 *   AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
 *   AND l_quantity >= 1 AND l_quantity <= 11
 *   AND p_size BETWEEN 1 AND 5
 *   AND l_shipmode IN ('AIR', 'AIR REG')
 *   AND l_shipinstruct = 'DELIVER IN PERSON'
 * ) OR (
 *   p_partkey = l_partkey
 *   AND p_brand = 'Brand#23'
 *   AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
 *   AND l_quantity >= 10 AND l_quantity <= 20
 *   AND p_size BETWEEN 1 AND 10
 *   AND l_shipmode IN ('AIR', 'AIR REG')
 *   AND l_shipinstruct = 'DELIVER IN PERSON'
 * ) OR (
 *   p_partkey = l_partkey
 *   AND p_brand = 'Brand#34'
 *   AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
 *   AND l_quantity >= 20 AND l_quantity <= 30
 *   AND p_size BETWEEN 1 AND 15
 *   AND l_shipmode IN ('AIR', 'AIR REG')
 *   AND l_shipinstruct = 'DELIVER IN PERSON'
 * )
 */
export async function runQ19(db: DBInstance, params: Q19Params = {}): Promise<Q19Result> {
  const {
    brand1 = 'Brand#12',
    brand2 = 'Brand#23',
    brand3 = 'Brand#34',
    quantity1Lo = 1,
    quantity1Hi = 11,
    quantity2Lo = 10,
    quantity2Hi = 20,
    quantity3Lo = 20,
    quantity3Hi = 30,
  } = params

  const result = await db.sql`
    SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
    FROM lineitem l
    JOIN part p ON l.l_partkey = p.p_partkey
    WHERE (
      (
        p.p_brand = ${brand1}
        AND (p.p_container = 'SM CASE' OR p.p_container = 'SM BOX' OR p.p_container = 'SM PACK' OR p.p_container = 'SM PKG')
        AND l.l_quantity >= ${quantity1Lo} AND l.l_quantity <= ${quantity1Hi}
        AND p.p_size >= 1 AND p.p_size <= 5
        AND (l.l_shipmode = 'AIR' OR l.l_shipmode = 'REG AIR')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
      ) OR (
        p.p_brand = ${brand2}
        AND (p.p_container = 'MED BAG' OR p.p_container = 'MED BOX' OR p.p_container = 'MED PKG' OR p.p_container = 'MED PACK')
        AND l.l_quantity >= ${quantity2Lo} AND l.l_quantity <= ${quantity2Hi}
        AND p.p_size >= 1 AND p.p_size <= 10
        AND (l.l_shipmode = 'AIR' OR l.l_shipmode = 'REG AIR')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
      ) OR (
        p.p_brand = ${brand3}
        AND (p.p_container = 'LG CASE' OR p.p_container = 'LG BOX' OR p.p_container = 'LG PACK' OR p.p_container = 'LG PKG')
        AND l.l_quantity >= ${quantity3Lo} AND l.l_quantity <= ${quantity3Hi}
        AND p.p_size >= 1 AND p.p_size <= 15
        AND (l.l_shipmode = 'AIR' OR l.l_shipmode = 'REG AIR')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
      )
    )
  `

  return { revenue: Number(result.rows[0]?.revenue ?? 0) }
}

// =============================================================================
// Run All Queries
// =============================================================================

/**
 * Run all 8 implemented TPC-H queries with default parameters
 */
export async function runTPCHQueries(db: DBInstance): Promise<TPCHQueryResults> {
  console.log('Running TPC-H Query Suite...\n')

  // Q1: Pricing Summary Report
  console.log('Q1: Pricing Summary Report')
  const q1 = await runQ1(db, { date: '1998-09-02' })
  console.log(`  ${q1.length} rows returned`)

  // Q3: Shipping Priority
  console.log('\nQ3: Shipping Priority')
  const q3 = await runQ3(db, { segment: 'BUILDING', date: '1995-03-15' })
  console.log(`  ${q3.length} rows returned`)

  // Q5: Local Supplier Volume
  console.log('\nQ5: Local Supplier Volume')
  const q5 = await runQ5(db, { region: 'ASIA', year: 1994 })
  console.log(`  ${q5.length} rows returned`)

  // Q6: Forecasting Revenue Change
  console.log('\nQ6: Forecasting Revenue Change')
  const q6 = await runQ6(db, { year: 1994, discountLo: 0.05, discountHi: 0.07, quantity: 24 })
  console.log(`  Revenue: ${q6.revenue.toFixed(2)}`)

  // Q10: Returned Item Reporting
  console.log('\nQ10: Returned Item Reporting')
  const q10 = await runQ10(db, { year: 1993, quarter: 4 })
  console.log(`  ${q10.length} rows returned`)

  // Q12: Shipping Modes and Order Priority
  console.log('\nQ12: Shipping Modes and Order Priority')
  const q12 = await runQ12(db, { shipModes: ['MAIL', 'SHIP'], year: 1994 })
  console.log(`  ${q12.length} rows returned`)

  // Q14: Promotion Effect
  console.log('\nQ14: Promotion Effect')
  const q14 = await runQ14(db, { date: '1995-09-01' })
  console.log(`  Promo Revenue: ${q14.promo_revenue.toFixed(2)}%`)

  // Q19: Discounted Revenue
  console.log('\nQ19: Discounted Revenue')
  const q19 = await runQ19(db)
  console.log(`  Revenue: ${q19.revenue.toFixed(2)}`)

  return { q1, q3, q5, q6, q10, q12, q14, q19 }
}

// =============================================================================
// Benchmark Runner
// =============================================================================

export interface BenchmarkResult {
  query: string
  duration: number
  rows: number
  coldStart: boolean
}

export interface BenchmarkSummary {
  totalDuration: number
  coldStartAvg: number
  warmCacheAvg: number
  results: BenchmarkResult[]
}

/**
 * Run TPC-H benchmark with timing
 */
export async function runTPCHBenchmark(
  db: DBInstance,
  options: { iterations?: number; warmupIterations?: number; verbose?: boolean } = {}
): Promise<BenchmarkSummary> {
  const { iterations = 3, warmupIterations = 1, verbose = false } = options
  const results: BenchmarkResult[] = []
  const log = verbose ? console.log : () => {}

  const queries: { name: string; fn: () => Promise<unknown[]> }[] = [
    { name: 'Q1', fn: () => runQ1(db) },
    { name: 'Q3', fn: () => runQ3(db) },
    { name: 'Q5', fn: () => runQ5(db) },
    { name: 'Q6', fn: async () => [await runQ6(db)] },
    { name: 'Q10', fn: () => runQ10(db) },
    { name: 'Q12', fn: () => runQ12(db) },
    { name: 'Q14', fn: async () => [await runQ14(db)] },
    { name: 'Q19', fn: async () => [await runQ19(db)] },
  ]

  for (const query of queries) {
    log(`\nRunning ${query.name}...`)

    // Warmup iterations (cold start)
    for (let i = 0; i < warmupIterations; i++) {
      const start = performance.now()
      const result = await query.fn()
      const duration = performance.now() - start

      results.push({
        query: query.name,
        duration,
        rows: Array.isArray(result) ? result.length : 1,
        coldStart: true,
      })

      log(`  Cold start ${i + 1}: ${duration.toFixed(2)}ms`)
    }

    // Timed iterations (warm cache)
    for (let i = 0; i < iterations; i++) {
      const start = performance.now()
      const result = await query.fn()
      const duration = performance.now() - start

      results.push({
        query: query.name,
        duration,
        rows: Array.isArray(result) ? result.length : 1,
        coldStart: false,
      })

      log(`  Warm cache ${i + 1}: ${duration.toFixed(2)}ms`)
    }
  }

  // Calculate summary
  const coldStartResults = results.filter(r => r.coldStart)
  const warmCacheResults = results.filter(r => !r.coldStart)

  const totalDuration = results.reduce((sum, r) => sum + r.duration, 0)
  const coldStartAvg = coldStartResults.length > 0
    ? coldStartResults.reduce((sum, r) => sum + r.duration, 0) / coldStartResults.length
    : 0
  const warmCacheAvg = warmCacheResults.length > 0
    ? warmCacheResults.reduce((sum, r) => sum + r.duration, 0) / warmCacheResults.length
    : 0

  return {
    totalDuration,
    coldStartAvg,
    warmCacheAvg,
    results,
  }
}

/**
 * Format benchmark results as a report
 */
export function formatBenchmarkReport(summary: BenchmarkSummary): string {
  const lines: string[] = []

  lines.push('='.repeat(70))
  lines.push('TPC-H Benchmark Report')
  lines.push('='.repeat(70))
  lines.push('')
  lines.push(`Total Duration: ${summary.totalDuration.toFixed(2)}ms`)
  lines.push(`Cold Start Avg: ${summary.coldStartAvg.toFixed(2)}ms`)
  lines.push(`Warm Cache Avg: ${summary.warmCacheAvg.toFixed(2)}ms`)
  lines.push('')
  lines.push('-'.repeat(70))
  lines.push('Query Details:')
  lines.push('-'.repeat(70))

  // Group by query
  const byQuery = new Map<string, BenchmarkResult[]>()
  for (const result of summary.results) {
    if (!byQuery.has(result.query)) {
      byQuery.set(result.query, [])
    }
    byQuery.get(result.query)!.push(result)
  }

  for (const [query, queryResults] of Array.from(byQuery.entries())) {
    const cold = queryResults.filter(r => r.coldStart)
    const warm = queryResults.filter(r => !r.coldStart)

    const coldAvg = cold.reduce((sum, r) => sum + r.duration, 0) / (cold.length || 1)
    const warmAvg = warm.reduce((sum, r) => sum + r.duration, 0) / (warm.length || 1)

    lines.push(`\n${query}:`)
    lines.push(`  Cold Start: ${coldAvg.toFixed(2)}ms`)
    lines.push(`  Warm Cache: ${warmAvg.toFixed(2)}ms`)
    lines.push(`  Speedup: ${(coldAvg / warmAvg).toFixed(2)}x`)
    lines.push(`  Rows: ${queryResults[0].rows}`)
  }

  lines.push('')
  lines.push('='.repeat(70))

  return lines.join('\n')
}
