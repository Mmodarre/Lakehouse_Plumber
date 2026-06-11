# Databricks notebook source
# MAGIC %md
# MAGIC # LHP sample quickstart — data preparation
# MAGIC
# MAGIC Task 0 of the sample job (deployed via Declarative Automation Bundles).
# MAGIC Seeds everything the generated Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC consume:
# MAGIC
# MAGIC - the bronze / silver / gold schemas and the `landing` Unity Catalog Volume;
# MAGIC - per-round **JSON** (orders) and **CSV** (lineitem) files in the landing
# MAGIC   zone — Auto Loader picks up each new round incrementally;
# MAGIC - `customer_cdf` — a change-data-feed-enabled table mutated every round
# MAGIC   (the streaming CDC source behind the SCD2 `dim_customer`);
# MAGIC - `supplier_snapshots` — a versioned full-snapshot table that gains
# MAGIC   `snapshot_id = N` every round (the snapshot CDC source for `dim_supplier`).
# MAGIC
# MAGIC The notebook is idempotent and round-aware. With `round = auto` (the
# MAGIC default) the next round is inferred from existing state, so simply
# MAGIC re-running the job advances the demo. An explicit integer `round`
# MAGIC overrides; re-running an already-landed round is a no-op because every
# MAGIC per-round step checks for its own output first.
# MAGIC
# MAGIC Reads only `samples.tpch.*` (present on every Unity Catalog workspace);
# MAGIC writes only to the user-supplied catalog.

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("bronze_schema", "sample_bronze")
dbutils.widgets.text("round", "auto")

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
round_param = dbutils.widgets.get("round").strip()

# Silver/gold schema names mirror substitutions/dev.yaml. Derive them from the
# bronze schema name so a custom prefix carries through
# (my_bronze -> my_silver / my_gold); fall back to the sample defaults.
if "bronze" in bronze_schema:
    silver_schema = bronze_schema.replace("bronze", "silver")
    gold_schema = bronze_schema.replace("bronze", "gold")
else:
    silver_schema = "sample_silver"
    gold_schema = "sample_gold"

landing_root = f"/Volumes/{catalog}/{bronze_schema}/landing"
customer_cdf = f"{catalog}.{bronze_schema}.customer_cdf"
supplier_snapshots = f"{catalog}.{bronze_schema}.supplier_snapshots"

# Deterministic demo bounds.
CUSTOMER_SUBSET_MAX_KEY = 1500  # round-1 seed: customers with c_custkey <= 1500
CUSTOMER_INSERTS_PER_ROUND = 100  # round N >= 2 inserts the next 100 keys
CUSTOMER_DELETES_PER_ROUND = 10  # round N >= 2 deletes the next 10 keys (from 51)
SUPPLIER_BASE_MAX_KEY = 500  # snapshot 1: suppliers with s_suppkey <= 500
SUPPLIER_GROWTH_PER_ROUND = 25  # each later snapshot also adds 25 new suppliers

summary = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def table_exists(name):
    return spark.catalog.tableExists(name)


def snapshot_exists(n):
    """True when supplier snapshot N has already been inserted."""
    if not table_exists(supplier_snapshots):
        return False
    row = spark.sql(
        f"SELECT count(*) AS c FROM {supplier_snapshots} WHERE snapshot_id = {n}"
    ).collect()[0]
    return row.c > 0


def dir_has_files(path):
    """True when the landing directory for a round already holds files."""
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False


def month_window(n):
    """Round N -> the N-th month of TPC-H order dates (starting 1992-01)."""
    month_index = n - 1  # months since 1992-01
    start = date(1992 + month_index // 12, month_index % 12 + 1, 1)
    end = date(1992 + (month_index + 1) // 12, (month_index + 1) % 12 + 1, 1)
    return start.isoformat(), end.isoformat()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas + landing volume

# COMMAND ----------

for schema in (bronze_schema, silver_schema, gold_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{bronze_schema}`.landing")
summary.append(
    f"schemas {bronze_schema}/{silver_schema}/{gold_schema} + volume landing: ensured"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round resolution
# MAGIC
# MAGIC `auto` infers the next round from existing state: `max(snapshot_id)` in
# MAGIC `supplier_snapshots` if the table exists, else round 1.

# COMMAND ----------

if round_param.lower() == "auto":
    if table_exists(supplier_snapshots):
        latest = spark.sql(
            f"SELECT max(snapshot_id) AS v FROM {supplier_snapshots}"
        ).collect()[0].v
        round_n = int(latest) + 1 if latest is not None else 1
    else:
        round_n = 1
    summary.append(f"round = auto -> resolved to round {round_n}")
else:
    round_n = int(round_param)
    summary.append(f"round = {round_n} (explicit override)")

print(f"Preparing data for round {round_n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land this round's files (orders → JSON, lineitem → CSV)
# MAGIC
# MAGIC Each round slices one more month of TPC-H data into new files under
# MAGIC `.../landing/<entity>/round{N}/`, so the cloudfiles loads in the ingest
# MAGIC pipeline pick them up incrementally.

# COMMAND ----------

window_start, window_end = month_window(round_n)

orders_dir = f"{landing_root}/orders/round{round_n}"
if dir_has_files(orders_dir):
    summary.append(f"orders round {round_n}: files already present — skipped")
else:
    orders = spark.sql(
        f"""
        SELECT *, CAST(o_orderdate AS TIMESTAMP) AS received_at
        FROM samples.tpch.orders
        WHERE o_orderdate >= DATE'{window_start}'
          AND o_orderdate <  DATE'{window_end}'
        """
    )
    orders.write.mode("overwrite").json(orders_dir)
    summary.append(
        f"orders round {round_n}: {orders.count()} rows "
        f"({window_start}..{window_end}) -> JSON {orders_dir}"
    )

lineitem_dir = f"{landing_root}/lineitem/round{round_n}"
if dir_has_files(lineitem_dir):
    summary.append(f"lineitem round {round_n}: files already present — skipped")
else:
    lineitem = spark.sql(
        f"""
        SELECT * FROM samples.tpch.lineitem
        WHERE l_shipdate >= DATE'{window_start}'
          AND l_shipdate <  DATE'{window_end}'
        """
    )
    lineitem.write.mode("overwrite").option("header", "true").csv(lineitem_dir)
    summary.append(
        f"lineitem round {round_n}: {lineitem.count()} rows "
        f"({window_start}..{window_end}) -> CSV {lineitem_dir}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 1 — seed the CDC sources
# MAGIC
# MAGIC `customer_cdf` (change data feed enabled) and `supplier_snapshots`
# MAGIC (snapshot 1). Both steps skip when their output already exists, so
# MAGIC re-running round 1 leaves state unchanged.

# COMMAND ----------

if round_n == 1:
    if table_exists(customer_cdf):
        summary.append("customer_cdf: already exists — skipped")
    else:
        spark.sql(
            f"""
            CREATE TABLE {customer_cdf}
            TBLPROPERTIES (delta.enableChangeDataFeed = true)
            AS SELECT * FROM samples.tpch.customer
            WHERE c_custkey <= {CUSTOMER_SUBSET_MAX_KEY}
            """
        )
        summary.append(
            f"customer_cdf: created with change data feed "
            f"(c_custkey <= {CUSTOMER_SUBSET_MAX_KEY})"
        )

    if snapshot_exists(1):
        summary.append("supplier_snapshots: snapshot 1 already present — skipped")
    elif not table_exists(supplier_snapshots):
        spark.sql(
            f"""
            CREATE TABLE {supplier_snapshots}
            AS SELECT *, CAST(1 AS INT) AS snapshot_id
            FROM samples.tpch.supplier
            WHERE s_suppkey <= {SUPPLIER_BASE_MAX_KEY}
            """
        )
        summary.append(
            f"supplier_snapshots: created with snapshot 1 "
            f"(s_suppkey <= {SUPPLIER_BASE_MAX_KEY})"
        )
    else:
        spark.sql(
            f"""
            INSERT INTO {supplier_snapshots}
            SELECT *, CAST(1 AS INT) AS snapshot_id
            FROM samples.tpch.supplier
            WHERE s_suppkey <= {SUPPLIER_BASE_MAX_KEY}
            """
        )
        summary.append("supplier_snapshots: snapshot 1 inserted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round ≥ 2 — advance the CDC sources
# MAGIC
# MAGIC UPDATE / INSERT / DELETE on `customer_cdf` (insert, update **and** delete
# MAGIC change-feed commits → SCD2 history including the `apply_as_deletes` path),
# MAGIC then supplier snapshot N with some mutated columns. The whole block is
# MAGIC guarded by snapshot N: if it already exists, this round's changes were
# MAGIC applied before and everything is skipped.

# COMMAND ----------

if round_n >= 2:
    if snapshot_exists(round_n):
        summary.append(
            f"round {round_n} CDC changes: snapshot {round_n} already present — skipped"
        )
    else:
        # UPDATE: same 50 customers every round, with values that change each
        # time -> visible update history in dim_customer.
        spark.sql(
            f"""
            UPDATE {customer_cdf}
            SET c_acctbal = c_acctbal + 100.0,
                c_address = concat('round-{round_n} ', c_address)
            WHERE c_custkey <= 50
            """
        )
        summary.append("customer_cdf: updated customers c_custkey <= 50")

        # INSERT: a fresh, non-overlapping band of 100 new customers per round.
        ins_lo = CUSTOMER_SUBSET_MAX_KEY + (round_n - 2) * CUSTOMER_INSERTS_PER_ROUND + 1
        ins_hi = CUSTOMER_SUBSET_MAX_KEY + (round_n - 1) * CUSTOMER_INSERTS_PER_ROUND
        spark.sql(
            f"""
            INSERT INTO {customer_cdf}
            SELECT * FROM samples.tpch.customer
            WHERE c_custkey BETWEEN {ins_lo} AND {ins_hi}
            """
        )
        summary.append(f"customer_cdf: inserted customers {ins_lo}..{ins_hi}")

        # DELETE: a fresh band of 10 customers per round (never overlaps the
        # update band 1..50 or earlier rounds' deletes).
        del_lo = 51 + (round_n - 2) * CUSTOMER_DELETES_PER_ROUND
        del_hi = del_lo + CUSTOMER_DELETES_PER_ROUND - 1
        spark.sql(
            f"DELETE FROM {customer_cdf} WHERE c_custkey BETWEEN {del_lo} AND {del_hi}"
        )
        summary.append(f"customer_cdf: deleted customers {del_lo}..{del_hi}")

        # Snapshot N: the supplier subset (grown by 25 keys per round) with
        # mutated address/acctbal slices -> snapshot CDC sees real changes.
        snap_max_key = SUPPLIER_BASE_MAX_KEY + (round_n - 1) * SUPPLIER_GROWTH_PER_ROUND
        spark.sql(
            f"""
            INSERT INTO {supplier_snapshots}
            SELECT s_suppkey,
                   s_name,
                   CASE WHEN s_suppkey % 5 = 0
                        THEN concat('round-{round_n} ', s_address)
                        ELSE s_address END AS s_address,
                   s_nationkey,
                   s_phone,
                   CASE WHEN s_suppkey % 7 = 0
                        THEN s_acctbal + {round_n} * 10.0
                        ELSE s_acctbal END AS s_acctbal,
                   s_comment,
                   CAST({round_n} AS INT) AS snapshot_id
            FROM samples.tpch.supplier
            WHERE s_suppkey <= {snap_max_key}
            """
        )
        summary.append(
            f"supplier_snapshots: inserted snapshot {round_n} "
            f"(s_suppkey <= {snap_max_key}, mutated address/acctbal slices)"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"=== Data prep complete — round {round_n} ===")
for line in summary:
    print(f"  - {line}")
