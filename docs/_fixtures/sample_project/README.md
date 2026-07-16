# sample_lakehouse

A ready-to-run Lakehouse Plumber (LHP) sample project. It builds a compact
medallion architecture (bronze → silver → gold) on top of the read-only
`samples.tpch` dataset that ships with every Unity Catalog workspace, so there
is **zero data setup**. LHP compiles the YAML in `pipelines/` into
Lakeflow Spark Declarative Pipelines (SDP) Python code, and the project deploys
as a Declarative Automation Bundle.

Three pipelines, orchestrated by one job (`resources/sample_job.yml`):

- `sample_ingest` — `pipelines/01_ingest/` (raw → bronze)
- `sample_silver` — `pipelines/02_silver/` (cleansing, SCD2 dimensions)
- `sample_gold` — `pipelines/03_gold/` (aggregated materialized view)

A data-prep notebook (`notebooks/data_prep.py`) runs as the first job task: it
creates the schemas and landing volume, slices `samples.tpch` into per-round
files, and seeds the change-data-feed and snapshot source tables. Every later
job task is generated pipeline code.

## Prerequisites

- A Databricks workspace with **Unity Catalog** enabled. The demo reads the
  read-only `samples.tpch` catalog, which is available on every Unity Catalog
  workspace.
- **Serverless compute** enabled — the notebook task and all three pipelines
  run serverless.
- A **catalog you can `CREATE` in**. The demo creates three schemas
  (`sample_bronze`, `sample_silver`, `sample_gold`) and one volume
  (`<your_catalog>.sample_bronze.landing`) inside it.
- The **Databricks CLI** installed and authenticated
  (`databricks auth login --host https://<your-workspace>`).
- `lhp` installed (it generated this project).

## Run the demo — three steps

### Step 1 — point the project at your workspace and catalog

Edit **two files**:

1. `databricks.yml` — set `workspace.host` under `targets.dev` to your
   workspace URL, and set the `catalog` variable default to your catalog.
2. `substitutions/dev.yaml` — set `catalog` to the **same** value, and update
   `landing_volume` to `/Volumes/<your_catalog>/sample_bronze/landing`.

#### The catalog lives in two places — set both

This is a known wrinkle, stated plainly: the catalog appears in
`substitutions/dev.yaml` **and** in `databricks.yml`, and the two settings
drive different systems. The substitution token is consumed by
`lhp generate` and baked into the generated pipeline code (table names,
volume paths). The `catalog` variable in `databricks.yml` is a
Declarative Automation Bundles variable that `resources/sample_job.yml`
passes to the data-prep notebook at run time (`${var.catalog}`). Code
generation and bundle deployment resolve their values independently —
neither can read the other's setting. If they disagree, the notebook seeds
one catalog while the pipelines read and write another, and the demo fails.

(A third file, `config/pipeline_config.yaml`, also names the catalog — but
via the `${catalog}` substitution token, so it follows `substitutions/dev.yaml`
automatically and needs no edit.)

### Step 2 — generate and deploy

```bash
lhp generate --env dev -pc config/pipeline_config.yaml
databricks bundle deploy
```

(To check the YAML without generating anything, run
`lhp validate --env dev -pc config/pipeline_config.yaml` first.)

`lhp generate` writes the pipeline Python to `generated/` and syncs the
pipeline resource files under `resources/lhp/` (that directory is LHP-managed
and rewritten on every generate — keep hand-edits out of it;
`resources/sample_job.yml` is hand-authored and never touched).

The `-pc` / `--pipeline-config` flag is required because the project is
bundle-enabled (`databricks.yml` is present): `config/pipeline_config.yaml`
supplies the catalog/schema and compute settings for the generated pipeline
resources. It ships ready to use — its catalog/schema values are `${...}`
substitution tokens, so it follows your step 1 edits automatically.

### Step 3 — run the job and inspect the results

```bash
databricks bundle run sample_job
```

The job runs four tasks in order: `data_prep` (notebook) → `ingest` →
`silver` → `gold`. When it finishes, inspect the output:

```sql
-- Bronze: ingested raw tables
SELECT count(*) FROM <your_catalog>.sample_bronze.orders_raw;

-- Silver: cleansed orders and the SCD2 dimensions
SELECT count(*) FROM <your_catalog>.sample_silver.orders;
SELECT count(*) FROM <your_catalog>.sample_silver.dim_customer;

-- Gold: the aggregated materialized view
SELECT * FROM <your_catalog>.sample_gold.sales_by_nation ORDER BY revenue DESC;
```

## Run it again

Re-run the same command — nothing else:

```bash
databricks bundle run sample_job
```

The data-prep notebook's `round` parameter defaults to `auto`: it detects what
the previous run produced and advances the demo one round. On round 2 (and
each later round):

- **New files land** under the volume's `orders/round2/` and
  `lineitem/round2/` directories. Auto Loader (cloudfiles) ingests **only the
  new files** — watch the bronze row counts grow incrementally.
- **CDC changes flow**: the notebook applies `UPDATE`s, `INSERT`s, and
  `DELETE`s to `sample_bronze.customer_cdf`. The change feed drives the SCD2
  write in `dim_customer` — updated customers get a new version row, and
  deleted customers have their current row closed out (`apply_as_deletes`).
- **Snapshot v2 arrives**: the notebook inserts a new supplier snapshot
  (`snapshot_id = 2`, with some mutated columns), and snapshot CDC advances
  `dim_supplier`'s SCD2 history.

See the history that round 2 created:

```sql
-- Customers with closed-out versions (updated or deleted in round 2)
SELECT c_custkey, c_name, __START_AT, __END_AT
FROM <your_catalog>.sample_silver.dim_customer
WHERE __END_AT IS NOT NULL
ORDER BY c_custkey, __START_AT;

-- Supplier history tracked from snapshot v1 -> v2
SELECT s_suppkey, s_acctbal, __START_AT, __END_AT
FROM <your_catalog>.sample_silver.dim_supplier
ORDER BY s_suppkey, __START_AT;
```

Re-running an already-landed round is a no-op — each per-round step checks for
its own output first and skips.

## Feature tour

Every LHP feature in the project, and the file that demonstrates it:

| Feature | Where to look |
|---|---|
| Delta load (batch) | `pipelines/01_ingest/nation_region.yaml` — reads `samples.tpch.nation`/`region` directly from the read-only sample catalog (a cross-catalog delta load, no copy step) |
| Delta load (streaming) | `pipelines/02_silver/orders_clean.yaml` (bronze table) and `pipelines/02_silver/dim_customer.yaml` (change feed via `readChangeFeed`) |
| Cloudfiles (Auto Loader), JSON + **file-based** schema hints | `pipelines/01_ingest/orders_ingest.yaml` + `schemas/orders_hints.yaml` |
| Cloudfiles, CSV + **inline DDL** schema hints | `pipelines/01_ingest/lineitem_ingest.yaml` |
| Templates (`{{ param }}` parameters) | `templates/cloudfiles_ingest.yaml` — one template instantiated twice (JSON orders, CSV lineitem) |
| Streaming CDC → SCD2 (`mode: cdc`, `apply_as_deletes`) | `pipelines/02_silver/dim_customer.yaml` — change-feed load → change filter → SCD2 write |
| Snapshot CDC (`mode: snapshot_cdc`, `source_function`) | `pipelines/02_silver/dim_supplier.yaml` + `functions/supplier_snapshot.py` |
| SQL transform (inline `sql:`) | `pipelines/02_silver/orders_clean.yaml` (bad-row filter) and `pipelines/02_silver/dim_customer.yaml` (change-type filter) |
| Schema transform (rename/cast) | `pipelines/02_silver/orders_clean.yaml` + `schema_transforms/orders_typed.yaml` |
| Data-quality transform (`mode: dqe`) | `pipelines/02_silver/orders_clean.yaml` + `expectations/orders.yaml` — note the expectations use the **post-rename** column names, because the schema transform runs first |
| Python transform | `pipelines/02_silver/orders_clean.yaml` + `transforms/normalize_tz.py` — localizes *every* timestamp column by inspecting the schema at runtime, the canonical "loop over the schema" case where Python beats SQL |
| Write — streaming table (append) | all `pipelines/01_ingest/` flowgroups |
| Write — materialized view + external `sql_path:` | `pipelines/03_gold/sales_by_nation.yaml` + `sql/sales_by_nation.sql` (pairs with the inline `sql:` transforms above to show both SQL forms) |
| Substitutions (`${token}`) | `substitutions/dev.yaml` — `${catalog}`, `${bronze_schema}`, `${landing_volume}`, `${target_tz}`, ... |
| Local variables (`%{var}`) | `pipelines/02_silver/orders_clean.yaml` — `%{entity}` names every action and view in the chain |
| Secrets (`${secret:scope/key}`) | `examples_optional/jdbc_with_secret.yaml.txt` — inactive, see below |
| Operational metadata | column definitions in `lhp.yaml`; each flowgroup opts in with an explicit `operational_metadata:` list (cloudfiles flowgroups add file lineage columns; CDC flowgroups opt out — see below) |
| Presets | `presets/bronze_layer.yaml` (change data feed + quality tag on bronze writes), `presets/silver_quality.yaml` |
| Pipeline configuration (catalog/schema + compute per pipeline) | `config/pipeline_config.yaml` — passed to every validate/generate via `-pc`; project defaults + per-pipeline overrides |
| Hand-authored job next to LHP-managed resources | `resources/sample_job.yml` orchestrates notebook + pipelines; LHP owns only `resources/lhp/` |
| Data prep / demo rounds | `notebooks/data_prep.py` |

### Why the CDC flowgroups skip operational metadata

`dim_customer` and `dim_supplier` deliberately do **not** enable
`operational_metadata`. Columns like `_processing_timestamp` get a fresh value
every run; feeding an always-changing column through an SCD2 `apply changes`
write would make every source row look modified on every run and churn the
dimension history with spurious versions. So the CDC flowgroups leave metadata
off, and every other flowgroup opts in explicitly with a list.

### Why cross-pipeline reads are fully qualified

Lakeflow views are **pipeline-scoped** — a view defined in `sample_ingest` is
invisible to `sample_silver`. That is why `orders_clean.yaml` opens with a
delta load of the fully qualified `${catalog}.${bronze_schema}.orders_raw`
table (separate `catalog:`/`schema:`/`table:` keys) instead of referencing a
bare view name from the ingest pipeline, and why the customer change-feed load
lives inside `dim_customer.yaml` in `sample_silver` rather than in the ingest
pipeline.

## The secret example (inactive by design)

`examples_optional/jdbc_with_secret.yaml.txt` shows the
`${secret:db_secrets/password}` syntax for credentials in a JDBC load. It
ships with a `.txt` suffix so `lhp generate` never picks it up — the demo
stays runnable with zero secret setup.

To use the pattern for real:

1. Create the secret scope and key:

   ```bash
   databricks secrets create-scope db_secrets
   databricks secrets put-secret db_secrets password
   ```

2. Copy the file into a pipeline directory (e.g.
   `pipelines/01_ingest/jdbc_ingest.yaml`), drop the `.txt` suffix, and point
   the connection settings at a JDBC database you can actually reach — as
   shipped it references a placeholder host and will not run unmodified.

## Cost and cleanup

The serverless job and pipelines are small but **not free** — each round runs
one notebook task and three pipeline updates over modest slices of TPC-H data.

To tear everything down:

```bash
databricks bundle destroy
```

```sql
DROP SCHEMA IF EXISTS <your_catalog>.sample_bronze CASCADE;
DROP SCHEMA IF EXISTS <your_catalog>.sample_silver CASCADE;
DROP SCHEMA IF EXISTS <your_catalog>.sample_gold CASCADE;
```

`bundle destroy` removes the job and pipelines; the `DROP SCHEMA ... CASCADE`
statements remove the demo tables and the landing volume (it lives inside
`sample_bronze`).