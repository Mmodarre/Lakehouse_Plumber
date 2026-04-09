#!/usr/bin/env python3
"""Generate 2000 flowgroup YAML files for performance testing.

20 domains × 20 entities × 5 layers = 2000 flowgroups.
Mix of template-based and direct-action flowgroups across layers.

Uses plain string templates with placeholder replacement to avoid
f-string brace escaping issues with LHP substitution tokens like {catalog}.
"""

from pathlib import Path

BASE = Path(__file__).parent / "pipelines"

DOMAINS = [
    "domain_a", "domain_b", "domain_c", "domain_d", "domain_e",
    "domain_f", "domain_g", "domain_h", "domain_i", "domain_j",
    "domain_k", "domain_l", "domain_m", "domain_n", "domain_o",
    "domain_p", "domain_q", "domain_r", "domain_s", "domain_t",
]

ENTITIES_PER_DOMAIN = {
    # --- Original 10 domains ---
    "domain_a": [
        "customer", "order", "product", "supplier", "shipment",
        "invoice", "payment", "refund", "coupon", "promotion",
        "warehouse", "inventory", "category", "brand", "review",
        "wishlist", "cart", "address", "contact", "subscription",
    ],
    "domain_b": [
        "employee", "department", "project", "task", "timesheet",
        "salary", "benefit", "leave_request", "training", "certification",
        "performance", "goal", "feedback", "interview", "candidate",
        "job_posting", "offer_letter", "onboarding", "offboarding", "payroll",
    ],
    "domain_c": [
        "transaction", "account", "ledger", "journal", "budget",
        "forecast", "tax", "audit", "compliance", "regulation",
        "asset", "depreciation", "liability", "equity", "revenue",
        "expense", "cost_center", "profit_center", "currency", "exchange_rate",
    ],
    "domain_d": [
        "patient", "doctor", "appointment", "prescription", "diagnosis",
        "treatment", "insurance", "claim", "hospital", "ward",
        "lab_test", "lab_result", "medication", "allergy", "immunization",
        "referral", "discharge", "admission", "medical_procedure", "vital_sign",
    ],
    "domain_e": [
        "vehicle", "driver", "route", "delivery", "tracking",
        "fuel_log", "maintenance", "inspection", "toll", "parking",
        "fleet", "dispatch", "cargo", "manifest", "consignment",
        "hub", "depot", "zone", "tariff", "carrier",
    ],
    "domain_f": [
        "student", "course", "enrollment", "instructor", "classroom",
        "assignment", "grade", "semester", "degree", "scholarship",
        "lecture", "exam", "library_book", "campus", "tuition",
        "transcript", "advisor", "faculty", "curriculum", "program",
    ],
    "domain_g": [
        "property", "tenant", "lease", "landlord", "unit",
        "building", "amenity", "work_order", "rent_payment", "eviction",
        "listing", "showing", "appraisal", "mortgage", "escrow",
        "closing", "deed", "survey", "zoning", "permit",
    ],
    "domain_h": [
        "store", "product_listing", "purchase", "return_item", "loyalty",
        "gift_card", "shelf", "aisle", "price_tag", "barcode",
        "cashier", "register", "receipt", "stock_count", "reorder",
        "vendor", "markdown", "clearance", "seasonal", "display",
    ],
    "domain_i": [
        "machine", "assembly", "quality_check", "raw_material", "work_cell",
        "shift", "operator", "defect", "batch", "bom",
        "routing", "downtime", "changeover", "yield_rate", "scrap",
        "tooling", "fixture", "calibration", "specification", "tolerance",
    ],
    "domain_j": [
        "meter", "reading", "grid", "transformer", "substation",
        "outage", "load_profile", "demand", "generation", "solar_panel",
        "wind_turbine", "battery", "inverter", "tariff_plan", "billing_cycle",
        "peak_usage", "off_peak", "net_metering", "power_factor", "voltage",
    ],
    # --- New 10 domains ---
    "domain_k": [
        "flight", "passenger", "booking", "airport", "runway",
        "gate", "baggage", "boarding_pass", "crew", "cockpit",
        "cabin", "ticket", "itinerary", "lounge", "terminal",
        "airline", "charter", "fuel_tank", "hangar", "air_route",
    ],
    "domain_l": [
        "restaurant", "menu_item", "table_seat", "reservation", "chef",
        "waiter", "kitchen", "food_order", "ingredient", "recipe",
        "supplier_contract", "health_inspection", "tip", "shift_schedule", "pantry",
        "dish", "beverage", "appetizer", "dessert", "special_menu",
    ],
    "domain_m": [
        "sensor", "iot_device", "telemetry", "firmware", "gateway",
        "edge_node", "data_stream", "alert_rule", "threshold", "anomaly",
        "actuator", "protocol", "mqtt_topic", "payload", "heartbeat",
        "device_group", "ota_update", "configuration", "diagnostic", "network_node",
    ],
    "domain_n": [
        "policy", "premium", "beneficiary", "adjuster", "underwriter",
        "risk_score", "coverage", "deductible", "endorsement", "exclusion",
        "renewal", "cancellation", "loss_report", "salvage", "subrogation",
        "reinsurance", "actuary", "reserve", "settlement", "litigation",
    ],
    "domain_o": [
        "campaign", "ad_group", "keyword", "impression", "click",
        "conversion", "landing_page", "audience", "creative", "bid",
        "budget_alloc", "channel", "attribution", "funnel_stage", "lead",
        "segment", "ab_test", "engagement", "bounce", "cta",
    ],
    "domain_p": [
        "genome", "sequence", "protein", "gene", "mutation",
        "sample", "experiment", "reagent", "microscope", "centrifuge",
        "incubator", "petri_dish", "culture", "assay", "biomarker",
        "trial", "placebo", "cohort", "phenotype", "genotype",
    ],
    "domain_q": [
        "match", "player", "team", "stadium", "referee",
        "score", "season", "league", "trophy", "coach",
        "transfer", "contract_deal", "injury", "lineup", "substitution",
        "fan", "broadcast", "sponsor", "merchandise", "ticket_sale",
    ],
    "domain_r": [
        "satellite", "orbit", "ground_station", "antenna", "transponder",
        "telemetry_frame", "uplink", "downlink", "spectrum", "footprint",
        "beam", "payload_data", "ephemeris", "tle", "launch_vehicle",
        "mission", "constellation", "frequency_band", "modulation", "link_budget",
    ],
    "domain_s": [
        "vineyard", "grape_variety", "harvest", "fermentation", "barrel",
        "bottle", "label", "vintage", "tasting_note", "sommelier",
        "cellar", "blend", "pressing", "filtration", "aging",
        "cork", "capsule", "appellation", "terroir", "cuvee",
    ],
    "domain_t": [
        "music_track", "album", "artist", "playlist", "listener",
        "stream_event", "royalty", "record_label", "genre", "producer",
        "mixing", "mastering", "release", "chart_position", "concert",
        "venue", "tour", "setlist", "merch_item", "fan_club",
    ],
}

RAW_TEMPLATES = ["parquet_ingestion_template", "csv_ingestion_template", "json_ingestion_template"]
SCHEMA_FILES = ["customer_schema", "lineitem_schema", "nation_schema", "orders_schema",
                "part_schema", "partsupp_schema", "product_schema"]


# ---------------------------------------------------------------------------
# YAML string templates — plain strings with placeholder replacement
# ---------------------------------------------------------------------------

# --- RAW LAYER: template-based ingestion ---
RAW_TMPL_BASED = """pipeline: perf_raw
flowgroup: ENTITY_ingestion
presets:
  - default_delta_properties
use_template: TMPL_NAME
template_parameters:
  table_name: ENTITY_raw
  landing_folder: ENTITY
  schema_file: SCHEMA_NAME
"""

# --- RAW LAYER: direct CloudFiles (no template) ---
RAW_DIRECT = """pipeline: perf_raw
flowgroup: ENTITY_ingestion
presets:
  - default_delta_properties
actions:
  - name: load_ENTITY_cloudfiles
    type: load
    operational_metadata:
      - "_source_file_path"
      - "_processing_timestamp"
    source:
      type: cloudfiles
      path: "{landing_volume}/ENTITY/*.parquet"
      format: parquet
      options:
        cloudFiles.format: parquet
        cloudFiles.maxFilesPerTrigger: 50
        cloudFiles.inferColumnTypes: True
        cloudFiles.schemaEvolutionMode: "addNewColumns"
        cloudFiles.rescuedDataColumn: "_rescued_data"
    target: v_ENTITY_cloudfiles
    description: "Load ENTITY from landing volume"

  - name: write_ENTITY_raw
    type: write
    source: v_ENTITY_cloudfiles
    write_target:
      type: streaming_table
      catalog: "{catalog}"
      schema: "{raw_schema}"
      table: "ENTITY_raw"
"""

# --- BRONZE LAYER: template-based ---
BRONZE_TMPL_BASED = """pipeline: perf_bronze
flowgroup: ENTITY_bronze
presets:
  - default_delta_properties
use_template: bronze_cleanse_template
template_parameters:
  entity_name: ENTITY
  source_table: ENTITY_raw
"""

# --- BRONZE LAYER: direct multi-action ---
BRONZE_DIRECT = """pipeline: perf_bronze
flowgroup: ENTITY_bronze
presets:
  - default_delta_properties
actions:
  - name: ENTITY_raw_load
    type: load
    operational_metadata: ["_processing_timestamp"]
    readMode: stream
    source:
      type: delta
      catalog: "{catalog}"
      schema: "{raw_schema}"
      table: ENTITY_raw
    target: v_ENTITY_raw
    description: "Load ENTITY from raw schema"

  - name: ENTITY_bronze_cleanse
    type: transform
    transform_type: sql
    source: v_ENTITY_raw
    target: v_ENTITY_bronze_cleaned
    sql: |
      SELECT
        xxhash64(*) as ENTITY_key,
        *,
        current_timestamp() as _etl_timestamp
      FROM v_ENTITY_raw
      WHERE 1=1

  - name: ENTITY_bronze_dqe
    type: transform
    transform_type: data_quality
    source: v_ENTITY_bronze_cleaned
    target: v_ENTITY_bronze_dqe
    readMode: stream
    expectations_file: "expectations/generic_quality.json"
    description: "Apply data quality checks to ENTITY"

  - name: write_ENTITY_bronze
    type: write
    source: v_ENTITY_bronze_dqe
    write_target:
      create_table: true
      type: streaming_table
      catalog: "{catalog}"
      schema: "{bronze_schema}"
      table: "ENTITY"
"""

# --- BRONZE LAYER: direct with migration (7 actions) ---
BRONZE_MIGRATION = """pipeline: perf_bronze
flowgroup: ENTITY_bronze
presets:
  - default_delta_properties
actions:
  - name: ENTITY_raw_load
    type: load
    operational_metadata: ["_processing_timestamp"]
    readMode: stream
    source:
      type: delta
      catalog: "{catalog}"
      schema: "{raw_schema}"
      table: ENTITY_raw
    target: v_ENTITY_raw
    description: "Load ENTITY from raw schema"

  - name: ENTITY_bronze_cleanse
    type: transform
    transform_type: sql
    source: v_ENTITY_raw
    target: v_ENTITY_bronze_cleaned
    sql: |
      SELECT
        xxhash64(*) as ENTITY_key,
        *,
        current_timestamp() as _etl_timestamp
      FROM v_ENTITY_raw

  - name: ENTITY_bronze_dqe
    type: transform
    transform_type: data_quality
    source: v_ENTITY_bronze_cleaned
    target: v_ENTITY_bronze_dqe
    readMode: stream
    expectations_file: "expectations/generic_quality.json"
    description: "Data quality for ENTITY bronze"

  - name: write_ENTITY_bronze
    type: write
    source: v_ENTITY_bronze_dqe
    write_target:
      create_table: true
      type: streaming_table
      catalog: "{catalog}"
      schema: "{bronze_schema}"
      table: "ENTITY"

  - name: ENTITY_migration_load
    type: load
    operational_metadata: ["_processing_timestamp"]
    readMode: batch
    source:
      type: delta
      catalog: "{catalog}"
      schema: "{raw_schema}"
      table: ENTITY_migration
    target: v_ENTITY_migration
    description: "Load ENTITY migration data"

  - name: ENTITY_migration_cleanse
    type: transform
    transform_type: sql
    readMode: batch
    source: v_ENTITY_migration
    target: v_ENTITY_migration_cleaned
    sql: |
      SELECT
        xxhash64(*) as ENTITY_key,
        *,
        'MIGRATION' as _source_file_path,
        current_timestamp() as _etl_timestamp
      FROM v_ENTITY_migration

  - name: write_ENTITY_migration
    type: write
    source: v_ENTITY_migration_cleaned
    readMode: batch
    once: true
    write_target:
      create_table: false
      type: streaming_table
      catalog: "{catalog}"
      schema: "{bronze_schema}"
      table: "ENTITY"
"""

# --- SILVER LAYER: direct CDC ---
SILVER_CDC = """pipeline: perf_silver
flowgroup: ENTITY_silver_SUFFIX
presets:
  - default_delta_properties
actions:
  - name: ENTITY_silver_load
    type: load
    operational_metadata: ["_processing_timestamp"]
    readMode: stream
    source:
      type: delta
      catalog: "{catalog}"
      schema: "{bronze_schema}"
      table: ENTITY
    target: v_ENTITY_bronze
    description: "Load ENTITY from bronze for silver layer"

  - name: write_ENTITY_silver
    type: write
    source: v_ENTITY_bronze
    write_target:
      type: streaming_table
      catalog: "{catalog}"
      schema: "{silver_schema}"
      table: "ENTITY_SUFFIX"
      mode: "cdc"
      cdc_config:
        keys: ["ENTITY_key"]
        sequence_by: "_etl_timestamp"
        scd_type: 2
        ignore_null_updates: true
        track_history_except_column_list: ["_source_file_path", "_processing_timestamp", "_etl_timestamp"]
"""

# --- GOLD LAYER: direct MV ---
GOLD_MV = """pipeline: perf_gold
flowgroup: ENTITY_MVTYPE_mv
actions:
  - name: ENTITY_MVTYPE_sql
    type: load
    source:
      type: sql
      sql: |
        SELECT
          *,
          current_timestamp() as _report_generated_at
        FROM {catalog}.{silver_schema}.ENTITY_dim
    target: v_ENTITY_MVTYPE

  - name: write_ENTITY_MVTYPE_mv
    type: write
    source: v_ENTITY_MVTYPE
    write_target:
      type: materialized_view
      catalog: "{catalog}"
      schema: "{gold_schema}"
      table: "ENTITY_MVTYPE_mv"
"""

# --- GOLD LAYER: direct aggregation MV ---
GOLD_AGG = """pipeline: perf_gold
flowgroup: ENTITY_AGGTYPE_agg_mv
actions:
  - name: ENTITY_AGGTYPE_agg_load
    type: load
    source:
      type: sql
      sql: |
        SELECT
          date_trunc('AGGTRUNC', _etl_timestamp) as period,
          count(*) as record_count,
          current_timestamp() as _generated_at
        FROM {catalog}.{silver_schema}.ENTITY_dim
        GROUP BY 1
    target: v_ENTITY_AGGTYPE_agg

  - name: write_ENTITY_AGGTYPE_agg_mv
    type: write
    source: v_ENTITY_AGGTYPE_agg
    write_target:
      type: materialized_view
      catalog: "{catalog}"
      schema: "{gold_schema}"
      table: "ENTITY_AGGTYPE_agg_mv"
"""

# --- MODELLED LAYER: direct load + transform + write ---
MODELLED_DIRECT = """pipeline: perf_modelled
flowgroup: ENTITY_modelled
presets:
  - default_delta_properties
actions:
  - name: ENTITY_silver_source
    type: load
    readMode: batch
    source:
      type: delta
      catalog: "{catalog}"
      schema: "{silver_schema}"
      table: ENTITY_dim
    target: v_ENTITY_silver
    description: "Load ENTITY from silver for modelling"

  - name: ENTITY_model_transform
    type: transform
    transform_type: sql
    readMode: batch
    source: v_ENTITY_silver
    target: v_ENTITY_modelled
    sql: |
      SELECT
        *,
        current_timestamp() as _modelled_at,
        'v1' as _model_version
      FROM v_ENTITY_silver

  - name: write_ENTITY_modelled
    type: write
    source: v_ENTITY_modelled
    write_target:
      type: materialized_view
      catalog: "{catalog}"
      schema: "{modelled_schema}"
      table: "ENTITY_modelled"
"""


def main():
    total = 0
    template_count = 0
    direct_count = 0

    for domain in DOMAINS:
        entities = ENTITIES_PER_DOMAIN[domain]

        # --- RAW: 20 per domain = 200 total ---
        # ~60% template-based (idx % 5 != 0,1), ~40% direct (idx % 5 == 0 or 1)
        for i, entity in enumerate(entities):
            if i % 5 in (0, 1):
                # Direct CloudFiles load (no template)
                content = RAW_DIRECT.replace("ENTITY", entity)
                direct_count += 1
            else:
                # Template-based
                tmpl = RAW_TEMPLATES[i % len(RAW_TEMPLATES)]
                schema = SCHEMA_FILES[i % len(SCHEMA_FILES)]
                content = (RAW_TMPL_BASED
                           .replace("ENTITY", entity)
                           .replace("TMPL_NAME", tmpl)
                           .replace("SCHEMA_NAME", schema))
                template_count += 1
            path = BASE / "01_raw" / domain / f"{entity}_ingestion.yaml"
            path.write_text(content)
            total += 1

        # --- BRONZE: 20 per domain = 200 total ---
        # ~40% template-based (idx % 5 in 0,1,2,3 → first 8 of 20), rest direct
        # Every 5th entity gets migration variant (7 actions)
        for i, entity in enumerate(entities):
            if i < 8:
                # Template-based bronze
                content = BRONZE_TMPL_BASED.replace("ENTITY", entity)
                template_count += 1
            elif i % 5 == 4:
                # Direct with migration (7 actions)
                content = BRONZE_MIGRATION.replace("ENTITY", entity)
                direct_count += 1
            else:
                # Direct multi-action (4 actions)
                content = BRONZE_DIRECT.replace("ENTITY", entity)
                direct_count += 1
            path = BASE / "02_bronze" / domain / f"{entity}_bronze.yaml"
            path.write_text(content)
            total += 1

        # --- SILVER: 20 per domain = 200 total (all direct CDC) ---
        for i, entity in enumerate(entities):
            suffix = "dim" if i % 3 != 0 else "fct"
            content = SILVER_CDC.replace("ENTITY", entity).replace("SUFFIX", suffix)
            path = BASE / "03_silver" / domain / f"{entity}_silver_{suffix}.yaml"
            path.write_text(content)
            direct_count += 1
            total += 1

        # --- GOLD: 20 per domain = 200 total (all direct MV/agg) ---
        for i, entity in enumerate(entities):
            if i < 10:
                mv_type = ["summary", "analysis", "report", "dashboard"][i % 4]
                content = GOLD_MV.replace("ENTITY", entity).replace("MVTYPE", mv_type)
                path = BASE / "04_gold" / domain / f"{entity}_{mv_type}_mv.yaml"
            else:
                agg_type = ["daily", "weekly", "monthly", "quarterly"][i % 4]
                agg_trunc = ["day", "week", "mon", "quarter"][i % 4]
                content = (GOLD_AGG
                           .replace("ENTITY", entity)
                           .replace("AGGTYPE", agg_type)
                           .replace("AGGTRUNC", agg_trunc))
                path = BASE / "04_gold" / domain / f"{entity}_{agg_type}_agg_mv.yaml"
            path.write_text(content)
            direct_count += 1
            total += 1

        # --- MODELLED: 20 per domain = 200 total (all direct) ---
        for i, entity in enumerate(entities):
            content = MODELLED_DIRECT.replace("ENTITY", entity)
            path = BASE / "05_modelled" / domain / f"{entity}_modelled.yaml"
            path.write_text(content)
            direct_count += 1
            total += 1

    print(f"Generated {total} flowgroup YAML files")
    print(f"  Template-based: {template_count} ({template_count*100//total}%)")
    print(f"  Direct actions:  {direct_count} ({direct_count*100//total}%)")
    print()
    for layer in ["01_raw", "02_bronze", "03_silver", "04_gold", "05_modelled"]:
        count = sum(1 for _ in (BASE / layer).rglob("*.yaml"))
        print(f"  {layer}: {count} files")
    for domain in DOMAINS:
        count = sum(
            1
            for layer in ["01_raw", "02_bronze", "03_silver", "04_gold", "05_modelled"]
            for _ in (BASE / layer / domain).rglob("*.yaml")
        )
        print(f"  {domain}: {count} files")


if __name__ == "__main__":
    main()
