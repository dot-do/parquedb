# Star Schema Benchmark (SSB) Example for ParqueDB

This example demonstrates using ParqueDB with the Star Schema Benchmark (SSB), a popular data warehouse benchmark derived from TPC-H. SSB is designed to measure the performance of database systems on decision support workloads with a star schema data model.

## Schema Overview

SSB uses a classic star schema with one fact table and four dimension tables:

```
                        +----------------+
                        |     DATE       |
                        +----------------+
                              ^
                              |
+------------+     +------------------+     +------------+
|  CUSTOMER  | <-- |    LINEORDER     | --> |  SUPPLIER  |
+------------+     |    (fact table)  |     +------------+
                   +------------------+
                              |
                              v
                        +----------------+
                        |      PART      |
                        +----------------+
```

### Fact Table: LINEORDER

The LINEORDER table contains order line items with measures (quantity, prices, revenue) and foreign keys to dimension tables. At Scale Factor 1 (SF1), this table contains approximately 6 million rows.

| Column | Type | Description |
|--------|------|-------------|
| lo_orderkey | int | Order key |
| lo_linenumber | int | Line number |
| lo_custkey | int | Customer key (FK to CUSTOMER) |
| lo_partkey | int | Part key (FK to PART) |
| lo_suppkey | int | Supplier key (FK to SUPPLIER) |
| lo_orderdate | int | Order date key (FK to DATE) |
| lo_orderpriority | string | Order priority |
| lo_shippriority | int | Ship priority (0 or 1) |
| lo_quantity | int | Quantity ordered |
| lo_extendedprice | int | Extended price (in cents) |
| lo_ordtotalprice | int | Order total price (in cents) |
| lo_discount | int | Discount percentage (0-10) |
| lo_revenue | int | Revenue (in cents) |
| lo_supplycost | int | Supply cost (in cents) |
| lo_tax | int | Tax percentage (0-8) |
| lo_commitdate | int | Commit date key (FK to DATE) |
| lo_shipmode | string | Shipping mode |

### Dimension Table: CUSTOMER (~30K rows at SF1)

| Column | Type | Description |
|--------|------|-------------|
| c_custkey | int | Customer key (PK) |
| c_name | string | Customer name |
| c_address | string | Address |
| c_city | string | City |
| c_nation | string | Nation |
| c_region | string | Region |
| c_phone | string | Phone number |
| c_mktsegment | string | Market segment |

### Dimension Table: SUPPLIER (~2K rows at SF1)

| Column | Type | Description |
|--------|------|-------------|
| s_suppkey | int | Supplier key (PK) |
| s_name | string | Supplier name |
| s_address | string | Address |
| s_city | string | City |
| s_nation | string | Nation |
| s_region | string | Region |
| s_phone | string | Phone number |

### Dimension Table: PART (~200K rows at SF1)

| Column | Type | Description |
|--------|------|-------------|
| p_partkey | int | Part key (PK) |
| p_name | string | Part name |
| p_mfgr | string | Manufacturer (MFGR#1-5) |
| p_category | string | Category (MFGR#1#1-5) |
| p_brand1 | string | Brand (MFGR#1#1-40) |
| p_color | string | Color |
| p_type | string | Type |
| p_size | int | Size (1-50) |
| p_container | string | Container type |

### Dimension Table: DATE (~2.5K rows)

| Column | Type | Description |
|--------|------|-------------|
| d_datekey | int | Date key (PK, YYYYMMDD format) |
| d_date | string | Full date string |
| d_dayofweek | string | Day of week name |
| d_month | string | Month name |
| d_year | int | Year |
| d_yearmonthnum | int | Year * 100 + month |
| d_yearmonth | string | "Mon YYYY" format |
| d_daynuminweek | int | Day number in week (1-7) |
| d_daynuminmonth | int | Day number in month (1-31) |
| d_daynuminyear | int | Day number in year (1-366) |
| d_monthnuminyear | int | Month number (1-12) |
| d_weeknuminyear | int | Week number (1-53) |
| d_sellingseason | string | Selling season |
| d_lastdayinweekfl | int | Last day in week flag (0/1) |
| d_lastdayinmonthfl | int | Last day in month flag (0/1) |
| d_holidayfl | int | Holiday flag (0/1) |
| d_weekdayfl | int | Weekday flag (0/1) |

## SSB Queries

SSB includes 13 queries organized into 4 query flights:

### Flight 1: Revenue by Year/Discount (Q1.1-Q1.3)
Filter-heavy queries focusing on date ranges and discount percentages.

### Flight 2: Revenue by Part/Supplier Region (Q2.1-Q2.3)
Join-heavy queries combining part and supplier dimensions.

### Flight 3: Revenue by Customer/Supplier Location (Q3.1-Q3.4)
Three-way joins between customers, suppliers, and dates.

### Flight 4: Profit by Date/Customer/Supplier/Part (Q4.1-Q4.3)
Four-way joins with all dimensions for detailed profit analysis.

## Usage

### Generate Data

```bash
# Generate SF1 data (default)
npx tsx examples/ssb/generate.ts

# Generate SF10 data
npx tsx examples/ssb/generate.ts --scale-factor 10

# Generate with custom output directory
npx tsx examples/ssb/generate.ts --output ./data/ssb-sf1
```

### Run Benchmark

```bash
# Run all queries
npx tsx examples/ssb/index.ts

# Run specific query
npx tsx examples/ssb/index.ts --query Q1.1

# Run with verbose output
npx tsx examples/ssb/index.ts --verbose
```

### Programmatic Usage

```typescript
import { DB } from 'parquedb'
import { ssbSchema } from './examples/ssb/schema'
import { generateSSBData } from './examples/ssb/generate'
import { runSSBQueries, Q1_1, Q2_1, Q3_1, Q4_1 } from './examples/ssb/queries'

// Create database with SSB schema
const db = DB(ssbSchema)

// Generate data at scale factor 1
await generateSSBData(db, { scaleFactor: 1 })

// Run all queries
const results = await runSSBQueries(db)

// Run individual query
const q1Result = await Q1_1(db)
console.log('Q1.1 Revenue:', q1Result.revenue)
```

## Scale Factors

| SF | LINEORDER Rows | CUSTOMER | SUPPLIER | PART | Estimated Size |
|----|----------------|----------|----------|------|----------------|
| 1 | ~6M | ~30K | ~2K | ~200K | ~1 GB |
| 10 | ~60M | ~300K | ~20K | ~800K | ~10 GB |
| 100 | ~600M | ~3M | ~200K | ~1.6M | ~100 GB |

## Implementation Notes

### ParqueDB Features Used

1. **Schema Definition**: Using ParqueDB's type system with relationships
2. **Batch Inserts**: Efficient bulk loading with `createMany()`
3. **SQL Queries**: Using `db.sql` template tag for analytical queries
4. **Indexes**: Leveraging column statistics for predicate pushdown

### Optimizations

- Date dimension is pre-computed (not generated dynamically)
- Foreign keys use integer types for efficient joins
- Fact table is partitioned by year for time-range queries
- Dimension tables are small enough to fit in memory

## References

- [Original SSB Paper](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF) - O'Neil et al.
- [SSB on DuckDB](https://duckdb.org/docs/extensions/ssb) - DuckDB's SSB implementation
- [TPC-H Specification](http://www.tpc.org/tpch/) - Parent benchmark specification
