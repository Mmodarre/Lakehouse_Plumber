# Databricks notebook source
# DBTITLE 1,Widgets for Catalog, Schema, Volume, and Daily Period Configuration
# dbutils.widgets.text("catalog", "catalog_name", "Catalog")
# dbutils.widgets.text("schema", "schema_name", "Schema")
# dbutils.widgets.text("volume", "volume_name", "Volume")
# dbutils.widgets.text("daily_start_date", "2021-01-01", "Daily period start date (YYYY-MM-DD)")
# dbutils.widgets.text("daily_end_date", "2021-03-31", "Daily period end date (YYYY-MM-DD) - inclusive")
# dbutils.widgets.dropdown("run_migration", "true", ["true", "false"], "Run Migration Data (2019-2020)")

# COMMAND ----------

# DBTITLE 1,Retrieve Configuration and Set Daily Period
# Catalog = dbutils.widgets.get("catalog")
# Schema = dbutils.widgets.get("schema")
# Volume = dbutils.widgets.get("volume")
# DAILY_START_DATE_STR = dbutils.widgets.get("daily_start_date")
# DAILY_END_DATE_STR = dbutils.widgets.get("daily_end_date")
# RUN_MIGRATION_STR = dbutils.widgets.get("run_migration")
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

spark = DatabricksSession.builder.getOrCreate()
# COMMAND ----------

Catalog = "acme_edw_dev"
Schema = "edw_raw"
Volume = "incoming_volume"
migration_schema = "edw_old"
DAILY_START_DATE_STR = "2021-01-10"  # Daily period start date
DAILY_END_DATE_STR = "2021-01-10"    # Daily period end date (inclusive)
Run_NEXT_DAY = False
RUN_MIGRATION_STR = "false"  # Run migration data (2019-2020)

# COMMAND ----------
# DBTITLE 1,Create tables which replicate Operational Data using a smaller subset of TPCH data

spark.sql(f"""create table if not exists {Catalog}.oltp.customer as
select * from samples.tpch.customer order by rand() limit 4000""")

spark.sql(f"""create table if not exists {Catalog}.oltp.orders as
select o.* from samples.tpch.orders o where o.o_custkey in (select c_custkey from {Catalog}.oltp.customer)""")

spark.sql(f"""create table if not exists {Catalog}.oltp.lineitem as
select l.* from samples.tpch.lineitem l where l.l_orderkey in (select o_orderkey from {Catalog}.oltp.orders)""")

spark.sql(f"""create table if not exists {Catalog}.oltp.partsupp as
select 
* except(rn)
from (
  select *, row_number() over (partition by ps_partkey, ps_suppkey order by ps_availqty desc) as rn
  from (select ps.* from samples.tpch.partsupp ps join acme_edw_dev.oltp.lineitem l on ps.ps_partkey = l.l_partkey and ps.ps_suppkey = l.l_suppkey)
) t
where rn = 1""")

spark.sql(f"""create table if not exists {Catalog}.oltp.part as
select p.* from samples.tpch.part p where p.p_partkey in (select ps_partkey from {Catalog}.oltp.partsupp)""")

spark.sql(f"""create table if not exists {Catalog}.oltp.supplier as
select s.* from samples.tpch.supplier s where s.s_suppkey in (select ps_suppkey from {Catalog}.oltp.partsupp)""")

spark.sql(f"""create table if not exists {Catalog}.oltp.nation as
select * from samples.tpch.nation""")

spark.sql(f"""create table if not exists {Catalog}.oltp.region as
select * from samples.tpch.region""")







# COMMAND ----------
# DBTITLE 1,Retrieve latest last_modified_dt and Set Daily Period
if Run_NEXT_DAY:
    try:
        next_modified_dt = spark.sql(f"select date_add(max(last_modified_dt), 1) from {Catalog}.{Schema}.customer_raw").collect()[0][0]
        DAILY_START_DATE_STR = DAILY_END_DATE_STR = str(next_modified_dt)
        RUN_MIGRATION_STR = "false"
    except Exception as e:
        print(f"Error getting max last_modified_dt, Table does not exists or is empty")
        print(f"Setting daily start date to 2021-01-01 and end date to 2021-01-03 (inclusive)")
        DAILY_START_DATE_STR = "2021-01-01"
        DAILY_END_DATE_STR = "2021-01-03"
        RUN_MIGRATION_STR = "true"

# COMMAND ----------

# DBTITLE 1,Parse migration flag
RUN_MIGRATION = RUN_MIGRATION_STR.lower() == "true"

# COMMAND ----------
# DBTITLE 1,Print Configuration
print(f"Configuration: {Catalog}.{Schema}, Volume: {Volume}, Period: {DAILY_START_DATE_STR} to {DAILY_END_DATE_STR}, Migration: {RUN_MIGRATION}")

# COMMAND ----------
# DBTITLE 1,Setup and Date Processing
spark.sql(f"Create catalog if not exists {Catalog}")
spark.sql(f"Create schema if not exists {Catalog}.{Schema}")
spark.sql(f"Create schema if not exists {Catalog}.{migration_schema}")
spark.sql(f"Create volume if not exists {Catalog}.{Schema}.{Volume}")

# COMMAND ----------

# DBTITLE 1,Import Libraries
from datetime import datetime, timedelta, date
from pyspark.sql.functions import expr, col, lit, when, rand, randn, round as spark_round, monotonically_increasing_id, row_number, concat, substr, length, lpad, date_add, date_sub, to_timestamp, year
from pyspark.sql.window import Window
import random

# Parse dates with validation
try:
    DAILY_START_DATE = datetime.strptime(DAILY_START_DATE_STR, '%Y-%m-%d').date()
    DAILY_END_DATE = datetime.strptime(DAILY_END_DATE_STR, '%Y-%m-%d').date()
except ValueError as e:
    raise ValueError(f"Invalid date format. Please use YYYY-MM-DD format. Error: {e}")

if DAILY_START_DATE > DAILY_END_DATE:
    raise ValueError(f"Start date ({DAILY_START_DATE}) must be before or equal to end date ({DAILY_END_DATE})")

# Calculate base timeline (original TPCH dates + 27 years)
BASE_START_DATE = datetime(2019, 1, 1).date()  # 1992 + 27 years
BASE_END_DATE = datetime(2025, 8, 2).date()    # 1998 + 27 years

def generate_daily_dates(start_date, end_date):
    """Generate list of daily dates for the daily period (end_date is inclusive)"""
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates

def generate_weekly_dates(start_date, end_date):
    """Generate list of weekly dates for snapshots (end_date is inclusive)"""
    weeks = []
    current_date = start_date
    while current_date <= end_date:
        week_start = current_date
        week_end = min(current_date + timedelta(days=1), end_date)
        # Use actual ISO week number for the year
        iso_year, iso_week, _ = current_date.isocalendar()
        week_label = f"{iso_year}-W{iso_week:02d}"
        weeks.append((week_start, week_end, week_label))
        current_date += timedelta(days=7)
    return weeks

# Generate date ranges
DAILY_DATES = generate_daily_dates(DAILY_START_DATE, DAILY_END_DATE)
WEEKLY_PERIODS = generate_weekly_dates(DAILY_START_DATE, DAILY_END_DATE)

# Check if any day in the period is a Sunday (weekday 6)
PROCESS_WEEKLY_DATA = any(date.weekday() == 6 for date in DAILY_DATES)

print(f"Processing {len(DAILY_DATES)} daily dates, {len(WEEKLY_PERIODS)} weekly periods, Weekly processing: {PROCESS_WEEKLY_DATA}")

# COMMAND ----------

# DBTITLE 1,Timestamp Generation Functions
def add_order_timestamp_plus(date_col):
    """Add random 0-4 hours to order date: order_date + 0-4 hours"""
    return expr(f"from_unixtime(unix_timestamp({date_col}) + cast(rand() * 4 * 3600 as int))")

def add_order_timestamp_minus(date_col):
    """Subtract random 0-4 hours from order date: order_date - 0-4 hours"""
    return expr(f"from_unixtime(unix_timestamp({date_col}) - cast(rand() * 4 * 3600 as int))")

def add_random_day_hours(date_col):
    """Add random hours within the day: date + random hours"""
    return expr(f"from_unixtime(unix_timestamp({date_col}) + cast(rand() * 24 * 3600 as int))")

def random_timestamp_in_period(start_date, end_date):
    """Generate random timestamp between two dates"""
    days_diff = (end_date - start_date).days
    return expr(f"from_unixtime(unix_timestamp(date_add('{start_date}', cast(rand() * {days_diff} as int)), 'yyyy-MM-dd') + cast(rand() * 24 * 3600 as int))")

def random_timestamp_past_week():
    """Generate random timestamp in past week"""
    return expr("from_unixtime(unix_timestamp(current_timestamp()) - cast(rand() * 7 * 24 * 3600 as int))")

def random_timestamp_past_week_from_date(end_date):
    """Generate random timestamp in past week from specific date"""
    return expr(f"from_unixtime(unix_timestamp('{end_date} 23:59:59', 'yyyy-MM-dd HH:mm:ss') - cast(rand() * 7 * 24 * 3600 as int))")

# COMMAND ----------

# DBTITLE 1,Load Base Data
customer_df = spark.sql(f"""
with customer_orders as (
  select
    c.*,
    min(o.o_orderdate) as min_orderdate
  from {Catalog}.oltp.customer c
  join {Catalog}.oltp.orders o
    on c.c_custkey = o.o_custkey
  group by
    c.c_custkey, c.c_name, c.c_address, c.c_nationkey, c.c_phone, c.c_acctbal, c.c_mktsegment, c.c_comment
)
select
  c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, min_orderdate
from customer_orders
""")

orders_with_region_df = spark.sql(f"""
select
  o.*,
  c.c_nationkey,
  n.n_regionkey,
  r.r_name as region_name
from {Catalog}.oltp.orders o
join {Catalog}.oltp.customer c on o.o_custkey = c.c_custkey
join {Catalog}.oltp.nation n on c.c_nationkey = n.n_nationkey
join {Catalog}.oltp.region r on n.n_regionkey = r.r_regionkey
""")

lineitem_with_region_df = spark.sql(f"""
select
  l.*,
  o.o_orderdate,
  c.c_custkey,
  r.r_name as region_name
from {Catalog}.oltp.lineitem l
join {Catalog}.oltp.orders o on l.l_orderkey = o.o_orderkey
join {Catalog}.oltp.customer c on o.o_custkey = c.c_custkey
join {Catalog}.oltp.nation n on c.c_nationkey = n.n_nationkey
join {Catalog}.oltp.region r on n.n_regionkey = r.r_regionkey
""")

supplier_df = spark.sql(f"select * from {Catalog}.oltp.supplier")
part_df = spark.sql(f"select * from {Catalog}.oltp.part")
partsupp_df = spark.sql(f"select * from {Catalog}.oltp.partsupp")

print("Base data loaded")

# COMMAND ----------

# DBTITLE 1,Migration Data (2019-2020)
migration_customer_count = 0
migration_orders_count = 0
migration_lineitem_count = 0
migration_supplier_count = 0
migration_part_count = 0
migration_partsupp_count = 0

if RUN_MIGRATION:
    print("Processing migration data (2019-2020)")
    
    # Drop existing migration tables to avoid schema conflicts (only during migration reload)
    print("Dropping existing migration tables for clean recreation...")
    for table_name in ["customer", "orders", "lineitem", "supplier", "part", "partsupp"]:
        spark.sql(f"DROP TABLE IF EXISTS {Catalog}.{migration_schema}.{table_name}")
    
    # Customer Migration: customers whose first order is in 2019-2020
    migration_customers = customer_df.withColumn(
        "adjusted_first_order", 
        expr("add_months(min_orderdate, 12 * 27)")  # Adjust +27 years
    ).filter(
        f"adjusted_first_order >= '{BASE_START_DATE}' AND adjusted_first_order <= '{BASE_START_DATE.replace(year=BASE_START_DATE.year + 1)}'"
    ).withColumn(
        "last_modified_dt", 
        add_order_timestamp_minus("adjusted_first_order")  # first_order_date - 0-4 hours
    ).drop("adjusted_first_order", "min_orderdate")
    
    migration_customers.write.mode("append").saveAsTable(f"{Catalog}.{migration_schema}.customer")
    migration_customer_count = migration_customers.count()
    
    # Orders Migration: orders where order_date is in 2019-2020
    migration_orders = orders_with_region_df.withColumn(
        "adjusted_order_date", 
        expr("add_months(o_orderdate, 12 * 27)")  # Adjust +27 years
    ).filter(
        f"adjusted_order_date >= '{BASE_START_DATE}' AND adjusted_order_date <= '{BASE_START_DATE.replace(year=BASE_START_DATE.year + 1)}'"
    ).withColumn(
        "o_orderdate", col("adjusted_order_date")  # Replace with adjusted date
    ).withColumn(
        "last_modified_dt", 
        add_order_timestamp_plus("adjusted_order_date")  # order_date + 0-4 hours
    ).drop("adjusted_order_date", "c_nationkey", "n_regionkey", "region_name")
    
    migration_orders.write.mode("append").saveAsTable(f"{Catalog}.{migration_schema}.orders")
    migration_orders_count = migration_orders.count()
    
    # Lineitem Migration: lineitems where order_date is in 2019-2020
    migration_lineitem = lineitem_with_region_df.withColumn(
        "adjusted_order_date", 
        expr("add_months(o_orderdate, 12 * 27)")  # Adjust +27 years
    ).filter(
        f"adjusted_order_date >= '{BASE_START_DATE}' AND adjusted_order_date <= '{BASE_START_DATE.replace(year=BASE_START_DATE.year + 1)}'"
    ).withColumn(
        "o_orderdate", col("adjusted_order_date")  # Replace with adjusted date
    ).withColumn(
        "last_modified_dt", 
        add_order_timestamp_plus("adjusted_order_date")  # order_date + 0-4 hours
    ).drop("adjusted_order_date", "c_custkey", "region_name")
    
    migration_lineitem.write.mode("append").saveAsTable(f"{Catalog}.{migration_schema}.lineitem")
    migration_lineitem_count = migration_lineitem.count()
    
    # Supplier Migration: ALL suppliers with random timestamp in 2019-2020
    migration_supplier = supplier_df.withColumn(
        "last_modified_dt", 
        random_timestamp_in_period(BASE_START_DATE, BASE_START_DATE.replace(year=BASE_START_DATE.year + 1))
    )
    
    migration_supplier.write.mode("append").saveAsTable(f"{Catalog}.{migration_schema}.supplier")
    migration_supplier_count = migration_supplier.count()
    
    # Part Migration: ALL parts with random timestamp in 2019-2020
    migration_part = part_df.withColumn(
        "last_modified_dt", 
        random_timestamp_in_period(BASE_START_DATE, BASE_START_DATE.replace(year=BASE_START_DATE.year + 1))
    )
    
    migration_part.write.mode("append").saveAsTable(f"{Catalog}.{migration_schema}.part")
    migration_part_count = migration_part.count()
    
    # Partsupp Migration: ALL partsupp with random timestamp in 2019-2020
    migration_partsupp = partsupp_df.withColumn(
        "last_modified_dt", 
        random_timestamp_in_period(BASE_START_DATE, BASE_START_DATE.replace(year=BASE_START_DATE.year + 1))
    )
    
    migration_partsupp.write.mode("append").saveAsTable(f"{Catalog}.{migration_schema}.partsupp")
    migration_partsupp_count = migration_partsupp.count()
    
    # Static reference data
    spark.sql(f"select * from {Catalog}.oltp.nation").write.mode("append").parquet(f"/Volumes/{Catalog}/{Schema}/{Volume}/nation")
    spark.sql(f"select * from {Catalog}.oltp.region").write.mode("append").parquet(f"/Volumes/{Catalog}/{Schema}/{Volume}/region")
    
    print(f"Migration completed: Customer: {migration_customer_count}, Orders: {migration_orders_count}, Lineitem: {migration_lineitem_count}, Supplier: {migration_supplier_count}, Part: {migration_part_count}, Partsupp: {migration_partsupp_count}")
    
else:
    print("Skipping migration data")

# COMMAND ----------

# DBTITLE 1,Daily Processing
print(f"Processing {len(DAILY_DATES)} daily dates")

for daily_date in DAILY_DATES:
    date_str = daily_date.strftime('%Y-%m-%d')
    
    # Customer Daily Processing
    # NEW customers: first order = run_date (INSERT)
    new_customers = customer_df.withColumn(
        "adjusted_first_order", 
        expr("add_months(min_orderdate, 12 * 27)")  # Adjust +27 years
    ).filter(
        f"adjusted_first_order = '{daily_date}'"
    ).withColumn(
        "last_modified_dt", 
        add_order_timestamp_plus("adjusted_first_order")  # order_date + 0-4 hours
    ).drop("adjusted_first_order", "min_orderdate")
    
    # EXISTING customers: first order < run_date, 5% updates
    existing_customers = customer_df.withColumn(
        "adjusted_first_order", 
        expr("add_months(min_orderdate, 12 * 27)")  # Adjust +27 years
    ).filter(
        f"adjusted_first_order < '{daily_date}'"
    ).sample(fraction=0.05, seed=hash(date_str) % 1000).withColumn(
        "last_modified_dt", 
        expr(f"from_unixtime(unix_timestamp('{daily_date} 00:00:00', 'yyyy-MM-dd HH:mm:ss') + cast(rand() * 24 * 3600 as int))")  # run_date + random hours
    ).drop("adjusted_first_order", "min_orderdate")
    
    # Apply changes to existing customers (5% modification patterns)
    modified_existing = existing_customers.withColumn(
        "c_acctbal",
        spark_round(col("c_acctbal") * (0.95 + (rand() * 0.10)), 2)  # ±5% variation
    ).withColumn(
        "c_address", 
        when(rand() < 0.02, concat(col("c_address"), lit(" (Updated)"))).otherwise(col("c_address"))
    ).withColumn(
        "c_phone",
        when(rand() < 0.03, concat(substr(col("c_phone"), lit(1), lit(2)), lit("-"), (rand() * 999999999).cast("bigint").cast("string"))).otherwise(col("c_phone"))
    )
    
    # Combine new and updated customers
    daily_customers = new_customers.union(modified_existing)
    
    # Write daily customer data
    if daily_customers.count() > 0:
        daily_customers.write.mode("append").option("header", True).csv(f"/Volumes/{Catalog}/{Schema}/{Volume}/customer/{date_str}")
    
    # Orders Daily Processing: orders where order_date = run_date
    daily_orders = orders_with_region_df.withColumn(
        "adjusted_order_date", 
        expr("add_months(o_orderdate, 12 * 27)")  # Adjust +27 years
    ).filter(
        f"adjusted_order_date = '{daily_date}'"
    ).withColumn(
        "o_orderdate", col("adjusted_order_date")  # Replace with adjusted date
    ).withColumn(
        "last_modified_dt", 
        add_order_timestamp_plus("adjusted_order_date")  # order_date + 0-4 hours
    ).drop("adjusted_order_date", "c_nationkey", "n_regionkey")
    
    # Write orders by region
    if daily_orders.count() > 0:
        regions = daily_orders.select("region_name").distinct().collect()
        for region_row in regions:
            region_name = region_row["region_name"]
            region_name_formatted = region_name.replace(" ", "_")
            region_orders = daily_orders.filter(col("region_name") == region_name).drop("region_name")
            region_orders.write.mode("append").parquet(f"/Volumes/{Catalog}/{Schema}/{Volume}/orders/region_{region_name_formatted}/{date_str}")
    
    # Lineitem Daily Processing: lineitems where order_date = run_date
    daily_lineitem = lineitem_with_region_df.withColumn(
        "adjusted_order_date", 
        expr("add_months(o_orderdate, 12 * 27)")  # Adjust +27 years
    ).filter(
        f"adjusted_order_date = '{daily_date}'"
    ).withColumn(
        "o_orderdate", col("adjusted_order_date")  # Replace with adjusted date
    ).withColumn(
        "last_modified_dt", 
        add_order_timestamp_plus("adjusted_order_date")  # order_date + 0-4 hours
    ).drop("adjusted_order_date", "c_custkey")
    
    # Write lineitem by region
    if daily_lineitem.count() > 0:
        regions = daily_lineitem.select("region_name").distinct().collect()
        for region_row in regions:
            region_name = region_row["region_name"]
            region_name_formatted = region_name.replace(" ", "_")
            region_lineitem = daily_lineitem.filter(col("region_name") == region_name).drop("region_name")
            region_lineitem.write.mode("append").parquet(f"/Volumes/{Catalog}/{Schema}/{Volume}/lineitem/region_{region_name_formatted}/{date_str}")

print("Daily processing completed")

# COMMAND ----------

# DBTITLE 1,Weekly Processing (Sundays only)
if PROCESS_WEEKLY_DATA:
    print(f"Processing {len(WEEKLY_PERIODS)} weekly periods")
    
    # Track current state for progressive changes
    current_supplier_state = supplier_df
    current_part_state = part_df
    current_partsupp_state = partsupp_df
    
    for week_start, week_end, week_label in WEEKLY_PERIODS:
        # Supplier Weekly Snapshot: 15% changes
        suppliers_to_change = current_supplier_state.sample(fraction=0.15, seed=hash(week_label) % 1000)
        unchanged_suppliers = current_supplier_state.subtract(suppliers_to_change)
        
        changed_suppliers = suppliers_to_change.withColumn(
            "s_acctbal", spark_round(col("s_acctbal") * (0.95 + (rand() * 0.10)), 2)
        ).withColumn(
            "s_address", when(rand() < 0.05, concat(col("s_address"), lit(" (Updated)"))).otherwise(col("s_address"))
        ).withColumn(
            "last_modified_dt", random_timestamp_past_week_from_date(week_end)
        )
        
        weekly_supplier_snapshot = unchanged_suppliers.withColumn("last_modified_dt", random_timestamp_past_week_from_date(week_end)).union(changed_suppliers)
        weekly_supplier_snapshot.write.mode("append").parquet(f"/Volumes/{Catalog}/{Schema}/{Volume}/supplier/{week_label}")
        current_supplier_state = weekly_supplier_snapshot.drop("last_modified_dt")
        
        # Part Weekly Incremental: 15% changes (JSON format)
        parts_to_change = current_part_state.sample(fraction=0.15, seed=hash(week_label) % 1000)
        
        changed_parts = parts_to_change.withColumn(
            "p_retailprice", spark_round(col("p_retailprice") * (0.98 + (rand() * 0.04)), 2)
        ).withColumn(
            "p_name", when(rand() < 0.01, concat(col("p_name"), lit(" V2"))).otherwise(col("p_name"))
        ).withColumn(
            "last_modified_dt", random_timestamp_past_week_from_date(week_end)
        ).withColumn(
            "source_system_timestamp", random_timestamp_past_week_from_date(week_end)
        ).withColumn(
            "extract_timestamp", random_timestamp_past_week_from_date(week_end)
        ).withColumn(
            "source_system", lit("PART_MGMT_SYS")
        )
        
        if changed_parts.count() > 0:
            changed_parts.write.mode("append").json(f"/Volumes/{Catalog}/{Schema}/{Volume}/part/{week_label}")
        
        # Update current part state (without metadata columns)
        updated_part_state = parts_to_change.withColumn(
            "p_retailprice", spark_round(col("p_retailprice") * (0.98 + (rand() * 0.04)), 2)
        ).withColumn(
            "p_name", when(rand() < 0.01, concat(col("p_name"), lit(" V2"))).otherwise(col("p_name"))
        )
        current_part_state = current_part_state.subtract(parts_to_change).union(updated_part_state)
        
        # Partsupp Weekly Snapshot: 15% changes
        partsupp_to_change = current_partsupp_state.sample(fraction=0.15, seed=hash(week_label) % 1000)
        unchanged_partsupp = current_partsupp_state.subtract(partsupp_to_change)
        
        changed_partsupp = partsupp_to_change.withColumn(
            "ps_availqty", (col("ps_availqty") * (0.9 + (rand() * 0.2))).cast("int")  # ±10% variation
        ).withColumn(
            "ps_supplycost", spark_round(col("ps_supplycost") * (0.98 + (rand() * 0.04)), 2)
        ).withColumn(
            "last_modified_dt", random_timestamp_past_week_from_date(week_end)
        )
        
        weekly_partsupp_snapshot = unchanged_partsupp.withColumn("last_modified_dt", random_timestamp_past_week_from_date(week_end)).union(changed_partsupp)
        weekly_partsupp_snapshot.write.mode("append").parquet(f"/Volumes/{Catalog}/{Schema}/{Volume}/partsupp/{week_label}")
        current_partsupp_state = weekly_partsupp_snapshot.drop("last_modified_dt")
    
    print("Weekly processing completed")
else:
    print("Skipping weekly processing (no Sunday in period)")

# COMMAND ----------

# DBTITLE 1,Summary
total_days = len(DAILY_DATES)
total_weeks = len(WEEKLY_PERIODS) if PROCESS_WEEKLY_DATA else 0

print(f"\\nProcessing Summary:")
print(f"Period: {DAILY_START_DATE} to {DAILY_END_DATE} ({total_days} days)")
if RUN_MIGRATION:
    print(f"Migration: {migration_customer_count} customers, {migration_orders_count} orders, {migration_lineitem_count} lineitems")
    print(f"           {migration_supplier_count} suppliers, {migration_part_count} parts, {migration_partsupp_count} partsupp")
print(f"Daily: Customer/Orders/Lineitem files generated for {total_days} days")
print(f"Weekly: {total_weeks} supplier/part/partsupp snapshots generated" if PROCESS_WEEKLY_DATA else "Weekly: Skipped (no Sunday)")
print("Processing completed successfully")

# COMMAND ----------