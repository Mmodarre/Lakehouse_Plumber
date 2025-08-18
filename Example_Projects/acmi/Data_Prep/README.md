# ACMI Data Warehouse Source Data Simulation

This project simulates realistic source system data for a data warehouse ETL process using the TPC-H dataset as a foundation. The data represents two distinct phases: **Migration** and **Operational**.

## 📊 Data Timeline

- **Migration Period (2019-2020)**: Historical baseline data migrated from existing systems
- **Operational Period (2021-2025)**: Live operational data with progressive changes
- **Date Adjustment**: All dates shifted +27 years from original TPC-H timeline (1992-1998 → 2019-2025)

## 🔄 Data Delivery Patterns

### Migration Data (2019-2020)
All migration data stored in `old_schema` tables as baseline snapshots.

### Operational Data (2021-2025)

| Table | Pattern | Description | Frequency | Location |
|-------|---------|-------------|-----------|----------|
| **Customer** | 🔄 **Incremental** | SCD Type 2 simulation - only changed records | Yearly | `/volumes/customer/YYYY/` |
| **Supplier** | 🔄 **Incremental** | Progressive updates - only changed records | Yearly | `/volumes/supplier/YYYY/` |
| **Part** | 🔄 **Incremental** | Product updates - pricing, names, specifications | Yearly | `/volumes/part/YYYY/` |
| **Partsupp** | 📸 **Snapshot** | Complete dataset with updated pricing/quantities | Yearly | `/volumes/partsup/YYYY/` |
| **Orders** | 📈 **Transactional** | Raw business events by region | Regional* | `/volumes/orders/region_X_name/` |
| **Lineitem** | 📈 **Transactional** | Raw business events by region | Regional* | `/volumes/lineitem/region_X_name/` |
| **Nation** | 🚫 **Static** | Reference data - no changes | One-time | `/volumes/nation/` |
| **Region** | 🚫 **Static** | Reference data - no changes | One-time | `/volumes/region/` |

**Regional Frequency (Orders & Lineitem):**
- **AMERICA (region_1)**: Monthly folders (`2021-01/`, `2021-02/`, etc.)
- **Other regions**: Yearly folders (`2021/`, `2022/`, etc.)

## 📋 Table Schemas and Types

### Customer (SCD Type 2 Dimension)
```sql
CREATE TABLE customer (
    c_custkey       BIGINT,           -- Customer ID (Primary Key)
    c_name          STRING,           -- Customer Name
    c_address       STRING,           -- Customer Address (Changes for SCD2)
    c_nationkey     BIGINT,           -- Nation Key (Foreign Key)
    c_phone         STRING,           -- Phone Number (Changes for SCD2)
    c_acctbal       DECIMAL(18,2),    -- Account Balance (Changes for SCD2)
    c_mktsegment    STRING,           -- Market Segment (Changes for SCD2)
    c_comment       STRING,           -- Customer Comment
    last_modified_dt DATE             -- Last Modification Date
);
```
**Change Simulation**: 8-10% of customers change yearly
**Changing Attributes**: `c_address`, `c_phone`, `c_acctbal`, `c_mktsegment`

### Orders (Fact Table)
```sql
CREATE TABLE orders (
    o_orderkey      BIGINT,           -- Order ID (Primary Key)
    o_custkey       BIGINT,           -- Customer ID (Foreign Key)
    o_orderstatus   STRING,           -- Order Status (F, O, P)
    o_totalprice    DECIMAL(18,2),    -- Total Order Price
    o_orderdate     DATE,             -- Order Date (+27 years adjusted)
    o_orderpriority STRING,           -- Order Priority
    o_clerk         STRING,           -- Clerk Name
    o_shippriority  INT,              -- Shipping Priority
    o_comment       STRING            -- Order Comment
);
```
**Regional Split**: Orders partitioned by customer's region

### Lineitem (Fact Table)
```sql
CREATE TABLE lineitem (
    l_orderkey      BIGINT,           -- Order ID (Foreign Key)
    l_partkey       BIGINT,           -- Part ID (Foreign Key)
    l_suppkey       BIGINT,           -- Supplier ID (Foreign Key)
    l_linenumber    INT,              -- Line Number within Order
    l_quantity      DECIMAL(18,2),    -- Quantity
    l_extendedprice DECIMAL(18,2),    -- Extended Price
    l_discount      DECIMAL(18,2),    -- Discount
    l_tax           DECIMAL(18,2),    -- Tax
    l_returnflag    STRING,           -- Return Flag (A, N, R)
    l_linestatus    STRING,           -- Line Status (F, O)
    l_shipdate      DATE,             -- Ship Date
    l_commitdate    DATE,             -- Commit Date
    l_receiptdate   DATE,             -- Receipt Date
    l_shipinstruct  STRING,           -- Shipping Instructions
    l_shipmode      STRING,           -- Shipping Mode
    l_comment       STRING            -- Line Comment
);
```
**Partitioning**: Monthly by order date (`year_month` partition)

### Supplier (SCD Type 2 Dimension)
```sql
CREATE TABLE supplier (
    s_suppkey       BIGINT,           -- Supplier ID (Primary Key)
    s_name          STRING,           -- Supplier Name
    s_address       STRING,           -- Supplier Address (Changes)
    s_nationkey     BIGINT,           -- Nation Key (Foreign Key)
    s_phone         STRING,           -- Phone Number (Changes)
    s_acctbal       DECIMAL(18,2),    -- Account Balance (Changes)
    s_comment       STRING,           -- Supplier Comment
    last_modified_dt DATE             -- Last Modification Date
);
```
**Change Simulation**: 15-40% of suppliers change yearly
**Changing Attributes**: `s_address`, `s_phone`, `s_acctbal`

### Partsupp (Pricing/Inventory Bridge Table)
```sql
CREATE TABLE partsupp (
    ps_partkey      BIGINT,           -- Part ID (Composite Primary Key)
    ps_suppkey      BIGINT,           -- Supplier ID (Composite Primary Key)
    ps_availqty     INT,              -- Available Quantity (Changes)
    ps_supplycost   DECIMAL(18,2),    -- Supply Cost (Changes)
    ps_comment      STRING,           -- Part-Supplier Comment
    last_modified_date DATE           -- Last Modification Date
);
```
**Change Simulation**: 20-40% of part-supplier combinations change yearly
**Snapshot Pattern**: Complete dataset delivered each period

### Part (Product Dimension)
```sql
CREATE TABLE part (
    p_partkey       BIGINT,           -- Part ID (Primary Key)
    p_name          STRING,           -- Part Name (Changes for product updates)
    p_mfgr          STRING,           -- Manufacturer
    p_brand         STRING,           -- Brand
    p_type          STRING,           -- Part Type
    p_size          INT,              -- Size
    p_container     STRING,           -- Container Type
    p_retailprice   DECIMAL(18,2),    -- Retail Price (Changes frequently)
    p_comment       STRING,           -- Part Comment (Changes for spec updates)
    last_modified_dt DATE             -- Last Modification Date
);
```
**Change Simulation**: 15% of parts change yearly
**Changing Attributes**: `p_retailprice` (price changes), `p_name` (product updates), `p_comment` (specification changes)

### Nation (Static Reference)
```sql
CREATE TABLE nation (
    n_nationkey     BIGINT,           -- Nation ID (Primary Key)
    n_name          STRING,           -- Nation Name
    n_regionkey     BIGINT,           -- Region ID (Foreign Key)
    n_comment       STRING            -- Nation Comment
);
```
**Pattern**: Static reference data (25 nations)

### Region (Static Reference)
```sql
CREATE TABLE region (
    r_regionkey     BIGINT,           -- Region ID (Primary Key)
    r_name          STRING,           -- Region Name
    r_comment       STRING            -- Region Comment
);
```
**Pattern**: Static reference data (5 regions: AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)

## 📁 File Structure

```
Migration Data (old_schema tables):
├── old_schema.customer      (2019-2020 baseline)
├── old_schema.orders        (2019-2020 baseline)
├── old_schema.lineitem      (2019-2020 baseline)
├── old_schema.partsupp      (2019 baseline)
├── old_schema.supplier      (2019 baseline)
└── old_schema.part          (2019-2020 baseline)

Operational Data (volumes):
├── /volumes/customer/
│   ├── 2021/ (CSV - incremental changes)
│   ├── 2022/ (CSV - incremental changes)
│   ├── 2023/ (CSV - incremental changes)
│   ├── 2024/ (CSV - incremental changes)
│   └── 2025/ (CSV - incremental changes)
├── /volumes/orders/
│   ├── region_0_africa/
│   │   ├── 2021/ (Parquet - yearly)
│   │   ├── 2022/ (Parquet - yearly)
│   │   └── 2023/ (Parquet - yearly)
│   ├── region_1_america/
│   │   ├── 2021-01/ (Parquet - monthly)
│   │   ├── 2021-02/ (Parquet - monthly)
│   │   └── ... (Parquet - monthly)
│   ├── region_2_asia/ (yearly)
│   ├── region_3_europe/ (yearly)
│   └── region_4_middle_east/ (yearly)
├── /volumes/lineitem/
│   ├── region_0_africa/
│   │   ├── 2021/ (JSON - yearly)
│   │   ├── 2022/ (JSON - yearly)
│   │   └── 2023/ (JSON - yearly)
│   ├── region_1_america/
│   │   ├── 2021-01/ (JSON - monthly)
│   │   ├── 2021-02/ (JSON - monthly)
│   │   └── ... (JSON - monthly)
│   ├── region_2_asia/ (yearly)
│   ├── region_3_europe/ (yearly)
│   └── region_4_middle_east/ (yearly)
├── /volumes/partsup/
│   ├── 2021/ (Parquet - complete snapshot)
│   ├── 2022/ (Parquet - complete snapshot)
│   ├── 2023/ (Parquet - complete snapshot)
│   ├── 2024/ (Parquet - complete snapshot)
│   └── 2025/ (Parquet - complete snapshot)
├── /volumes/supplier/
│   ├── 2021/ (Parquet - incremental changes)
│   ├── 2022/ (Parquet - incremental changes)
│   ├── 2023/ (Parquet - incremental changes)
│   ├── 2024/ (Parquet - incremental changes)
│   └── 2025/ (Parquet - incremental changes)
├── /volumes/part/
│   ├── 2021/ (JSON - incremental changes)
│   ├── 2022/ (JSON - incremental changes)
│   ├── 2023/ (JSON - incremental changes)
│   ├── 2024/ (JSON - incremental changes)
│   └── 2025/ (JSON - incremental changes)
├── /volumes/nation/ (Parquet - static)
└── /volumes/region/ (Parquet - static)
```

## 🔧 ETL Implementation Notes

### SCD Type 2 Handling
- **Customer** and **Supplier** implement progressive changes over time
- Changes include realistic modifications to addresses, phone numbers, and account balances
- Modification timestamps align with business timeline (+27 years)

### Regional Data Distribution
- **Orders & Lineitem**: Both follow identical regional patterns
- **AMERICA region** simulates high-frequency CDC with monthly delivery
- **Other regions** simulate daily batch loads delivered yearly
- Regional split based on customer's nation → region mapping
- **Lineitem** inherits region from its parent order via `l_orderkey = o_orderkey`

### Data Quality Features
- **Consistent timestamps**: All modification dates align with business events + processing delays
- **Deterministic changes**: Hash-based selection ensures reproducible change patterns
- **Realistic volumes**: Change rates vary between 8-40% based on table characteristics

### Migration vs Operational
- **Migration data** represents historical baseline with `last_modification_date`
- **Operational data** includes progressive changes with realistic source system metadata
- Clean separation supports migration testing followed by incremental load testing

## 📊 Usage in Data Warehouse ETL

This simulated data supports testing:
1. **Migration**: Load historical baselines from `old_schema` tables
2. **SCD Type 2**: Handle customer and supplier dimension changes
3. **Incremental Loading**: Process only changed records for dimensions
4. **Snapshot Loading**: Handle complete dataset refreshes for pricing tables
5. **Regional Processing**: Manage different delivery frequencies by region (Orders/Lineitem)
6. **Data Consistency**: Validate lineitem-order relationships across regional boundaries  
7. **Data Quality**: Validate consistency across related tables
8. **Performance**: Test with realistic data volumes and change patterns