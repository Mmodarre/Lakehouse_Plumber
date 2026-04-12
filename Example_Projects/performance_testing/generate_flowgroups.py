#!/usr/bin/env python3
"""Generate 4000 flowgroup YAML files for performance testing.

20 domains × 40 entities × 5 layers = 4000 flowgroups.
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
    # --- Original 10 domains (now 40 entities each) ---
    "domain_a": [
        "customer", "order", "product", "supplier", "shipment",
        "invoice", "payment", "refund", "coupon", "promotion",
        "warehouse", "inventory", "category", "brand", "review",
        "wishlist", "cart", "address", "contact", "subscription",
        "return_request", "gift_wrap", "loyalty_point", "price_rule", "tax_rate",
        "shipping_method", "packaging", "barcode_item", "sku", "catalog_entry",
        "product_image", "variant", "bundle_offer", "flash_sale", "pre_order",
        "backorder", "dropship", "fulfillment", "pick_list", "packing_slip",
    ],
    "domain_b": [
        "employee", "department", "project", "task", "timesheet",
        "salary", "benefit", "leave_request", "training", "certification",
        "performance", "goal", "feedback", "interview", "candidate",
        "job_posting", "offer_letter", "onboarding", "offboarding", "payroll",
        "bonus", "commission", "overtime", "attendance", "badge",
        "workspace", "team_lead", "org_chart", "headcount", "requisition",
        "background_check", "drug_test", "exit_interview", "alumni", "mentor",
        "skill_matrix", "competency", "succession_plan", "workforce_plan", "labor_cost",
    ],
    "domain_c": [
        "transaction", "account", "ledger", "journal", "budget",
        "forecast", "tax", "audit", "compliance", "regulation",
        "asset", "depreciation", "liability", "equity", "revenue",
        "expense", "cost_center", "profit_center", "currency", "exchange_rate",
        "invoice_line", "credit_note", "debit_note", "bank_statement", "reconciliation",
        "cash_flow", "trial_balance", "balance_sheet", "income_statement", "variance",
        "accrual", "prepayment", "write_off", "provision", "intercompany",
        "consolidation", "segment_report", "tax_filing", "withholding", "vat_return",
    ],
    "domain_d": [
        "patient", "doctor", "appointment", "prescription", "diagnosis",
        "treatment", "insurance", "claim", "hospital", "ward",
        "lab_test", "lab_result", "medication", "allergy", "immunization",
        "referral", "discharge", "admission", "medical_procedure", "vital_sign",
        "nursing_note", "radiology", "pathology", "pharmacy", "blood_bank",
        "surgery", "anesthesia", "recovery", "rehab", "outpatient",
        "emergency", "triage", "bed_assignment", "diet_plan", "consent_form",
        "care_plan", "case_manager", "home_care", "telemedicine", "wearable_data",
    ],
    "domain_e": [
        "vehicle", "driver", "route", "delivery", "tracking",
        "fuel_log", "maintenance", "inspection", "toll", "parking",
        "fleet", "dispatch", "cargo", "manifest", "consignment",
        "hub", "depot", "zone", "tariff", "carrier",
        "trailer", "load_plan", "weight_ticket", "customs_doc", "bill_of_lading",
        "proof_of_delivery", "damage_report", "return_shipment", "cross_dock", "last_mile",
        "route_optimization", "geofence", "speed_alert", "idle_time", "driver_score",
        "eld_log", "dot_inspection", "hazmat", "temperature_log", "chain_of_custody",
    ],
    "domain_f": [
        "student", "course", "enrollment", "instructor", "classroom",
        "assignment", "grade", "semester", "degree", "scholarship",
        "lecture", "exam", "library_book", "campus", "tuition",
        "transcript", "advisor", "faculty", "curriculum", "program",
        "thesis", "dissertation", "research_grant", "lab_session", "fieldwork",
        "study_group", "tutoring", "plagiarism_check", "accreditation", "alumni_donor",
        "career_fair", "internship", "co_op", "exchange_program", "dormitory",
        "meal_plan", "student_club", "athletics", "financial_aid", "work_study",
    ],
    "domain_g": [
        "property", "tenant", "lease", "landlord", "unit",
        "building", "amenity", "work_order", "rent_payment", "eviction",
        "listing", "showing", "appraisal", "mortgage", "escrow",
        "closing", "deed", "survey", "zoning", "permit",
        "hoa", "condo_fee", "property_tax", "title_search", "lien",
        "easement", "covenant", "subdivision", "plat", "setback",
        "floor_plan", "staging", "open_house", "counteroffer", "earnest_money",
        "home_inspection", "termite_report", "radon_test", "lead_paint", "roof_cert",
    ],
    "domain_h": [
        "store", "product_listing", "purchase", "return_item", "loyalty",
        "gift_card", "shelf", "aisle", "price_tag", "barcode",
        "cashier", "register", "receipt", "stock_count", "reorder",
        "vendor", "markdown", "clearance", "seasonal", "display",
        "planogram", "endcap", "signage", "foot_traffic", "conversion_rate",
        "basket_size", "shrinkage", "loss_prevention", "mystery_shopper", "nps_score",
        "store_layout", "checkout_lane", "self_checkout", "curbside", "click_collect",
        "endless_aisle", "clienteling", "pos_terminal", "price_override", "rain_check",
    ],
    "domain_i": [
        "machine", "assembly", "quality_check", "raw_material", "work_cell",
        "shift", "operator", "defect", "batch", "bom",
        "routing", "downtime", "changeover", "yield_rate", "scrap",
        "tooling", "fixture", "calibration", "specification", "tolerance",
        "work_instruction", "spc_chart", "control_plan", "fmea", "ppap",
        "first_article", "gauge_rr", "capability_study", "corrective_action", "preventive_action",
        "kaizen", "kanban", "andon", "poka_yoke", "takt_time",
        "cycle_time", "oee", "mtbf", "mttr", "spare_part",
    ],
    "domain_j": [
        "meter", "reading", "grid", "transformer", "substation",
        "outage", "load_profile", "demand", "generation", "solar_panel",
        "wind_turbine", "battery", "inverter", "tariff_plan", "billing_cycle",
        "peak_usage", "off_peak", "net_metering", "power_factor", "voltage",
        "frequency", "reactive_power", "capacitor_bank", "relay", "circuit_breaker",
        "feeder", "distribution_line", "transmission_line", "voltage_regulator", "smart_meter",
        "demand_response", "curtailment", "interconnection", "wheeling", "ancillary_service",
        "spinning_reserve", "black_start", "island_mode", "microgrid", "virtual_power_plant",
    ],
    # --- Domains k-t (40 entities each) ---
    "domain_k": [
        "flight", "passenger", "booking", "airport", "runway",
        "gate", "baggage", "boarding_pass", "crew", "cockpit",
        "cabin", "ticket", "itinerary", "lounge", "terminal",
        "airline", "charter", "fuel_tank", "hangar", "air_route",
        "taxiway", "apron", "control_tower", "radar", "waypoint",
        "airspace", "slot_time", "ground_handling", "catering", "cargo_hold",
        "duty_free", "immigration", "customs_check", "visa", "transit",
        "codeshare", "interline", "frequent_flyer", "upgrade", "standby",
    ],
    "domain_l": [
        "restaurant", "menu_item", "table_seat", "reservation", "chef",
        "waiter", "kitchen", "food_order", "ingredient", "recipe",
        "supplier_contract", "health_inspection", "tip", "shift_schedule", "pantry",
        "dish", "beverage", "appetizer", "dessert", "special_menu",
        "prep_station", "walk_in", "cooler", "freezer", "dry_storage",
        "portion_control", "food_cost", "waste_log", "allergen", "nutritional_info",
        "happy_hour", "catering_event", "takeout", "delivery_partner", "online_order",
        "table_turn", "wait_time", "host_stand", "patio", "private_dining",
    ],
    "domain_m": [
        "sensor", "iot_device", "telemetry", "firmware", "gateway",
        "edge_node", "data_stream", "alert_rule", "threshold", "anomaly",
        "actuator", "protocol", "mqtt_topic", "payload", "heartbeat",
        "device_group", "ota_update", "configuration", "diagnostic", "network_node",
        "digital_twin", "simulation", "predictive_model", "time_series", "event_stream",
        "rule_engine", "workflow_trigger", "escalation", "sla_metric", "dashboard_widget",
        "data_lake_ingestion", "schema_registry", "data_catalog", "lineage_node", "quality_rule",
        "retention_policy", "archival", "replay", "dead_letter", "partition_key",
    ],
    "domain_n": [
        "policy", "premium", "beneficiary", "adjuster", "underwriter",
        "risk_score", "coverage", "deductible", "endorsement", "exclusion",
        "renewal", "cancellation", "loss_report", "salvage", "subrogation",
        "reinsurance", "actuary", "reserve", "settlement", "litigation",
        "policyholder", "agent", "broker", "commission_payout", "quote",
        "application_form", "medical_exam", "inspection_report", "fraud_flag", "investigation",
        "catastrophe", "exposure", "aggregate_limit", "occurrence", "retro_date",
        "treaty", "facultative", "bordereaux", "commutation", "runoff",
    ],
    "domain_o": [
        "campaign", "ad_group", "keyword", "impression", "click",
        "conversion", "landing_page", "audience", "creative", "bid",
        "budget_alloc", "channel", "attribution", "funnel_stage", "lead",
        "segment", "ab_test", "engagement", "bounce", "cta",
        "email_blast", "open_rate", "click_through", "unsubscribe", "spam_report",
        "social_post", "influencer", "hashtag", "mention", "sentiment",
        "seo_rank", "backlink", "domain_authority", "crawl_error", "sitemap",
        "retargeting", "lookalike", "custom_audience", "pixel_event", "dynamic_ad",
    ],
    "domain_p": [
        "genome", "sequence", "protein", "gene", "mutation",
        "sample", "experiment", "reagent", "microscope", "centrifuge",
        "incubator", "petri_dish", "culture", "assay", "biomarker",
        "trial", "placebo", "cohort", "phenotype", "genotype",
        "allele", "chromosome", "dna_extract", "rna_extract", "pcr_run",
        "gel_electrophoresis", "western_blot", "elisa", "flow_cytometry", "mass_spec",
        "bioinformatics_pipeline", "alignment", "variant_call", "annotation", "pathway",
        "drug_target", "compound", "dose_response", "toxicology", "pharmacokinetics",
    ],
    "domain_q": [
        "match", "player", "team", "stadium", "referee",
        "score", "season", "league", "trophy", "coach",
        "transfer", "contract_deal", "injury", "lineup", "substitution",
        "fan", "broadcast", "sponsor", "merchandise", "ticket_sale",
        "training_session", "fitness_test", "scouting_report", "draft_pick", "free_agent",
        "salary_cap", "luxury_tax", "revenue_share", "tv_deal", "streaming_rights",
        "highlight", "match_replay", "var_decision", "penalty", "red_card",
        "yellow_card", "corner_kick", "free_kick", "offside", "possession_stat",
    ],
    "domain_r": [
        "satellite", "orbit", "ground_station", "antenna", "transponder",
        "telemetry_frame", "uplink", "downlink", "spectrum", "footprint",
        "beam", "payload_data", "ephemeris", "tle", "launch_vehicle",
        "mission", "constellation", "frequency_band", "modulation", "link_budget",
        "solar_array", "thermal_control", "attitude_control", "propulsion", "debris_track",
        "collision_avoidance", "deorbit_plan", "space_weather", "radiation_dose", "eclipse_period",
        "ranging", "doppler_shift", "signal_strength", "bit_error_rate", "coding_scheme",
        "multiplexing", "bandwidth_alloc", "interference", "jamming_detect", "encryption_key",
    ],
    "domain_s": [
        "vineyard", "grape_variety", "harvest", "fermentation", "barrel",
        "bottle", "label", "vintage", "tasting_note", "sommelier",
        "cellar", "blend", "pressing", "filtration", "aging",
        "cork", "capsule", "appellation", "terroir", "cuvee",
        "malolactic", "racking", "fining", "stabilization", "bottling_line",
        "case_production", "allocation", "distributor", "retail_partner", "tasting_room",
        "wine_club", "release_date", "critic_score", "auction_lot", "provenance",
        "cellar_temp", "humidity_log", "barrel_toast", "oak_origin", "yeast_strain",
    ],
    "domain_t": [
        "music_track", "album", "artist", "playlist", "listener",
        "stream_event", "royalty", "record_label", "genre", "producer",
        "mixing", "mastering", "release", "chart_position", "concert",
        "venue", "tour", "setlist", "merch_item", "fan_club",
        "session_musician", "songwriter", "publishing_deal", "sync_license", "mechanical_royalty",
        "performance_royalty", "digital_distributor", "vinyl_pressing", "cd_pressing", "cassette_run",
        "music_video", "lyric_sheet", "liner_note", "album_art", "press_kit",
        "radio_play", "podcast_feature", "influencer_promo", "tiktok_clip", "viral_moment",
    ],
}

RAW_TEMPLATES = ["parquet_ingestion_template", "csv_ingestion_template", "json_ingestion_template"]
SCHEMA_FILES = ["customer_schema", "lineitem_schema", "nation_schema", "orders_schema",
                "part_schema", "partsupp_schema", "product_schema"]


# ---------------------------------------------------------------------------
# YAML string templates — plain strings with placeholder replacement
# ---------------------------------------------------------------------------

# --- RAW LAYER: template-based ingestion ---
RAW_TMPL_BASED = """pipeline: PIPELINE
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
RAW_DIRECT = """pipeline: PIPELINE
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
BRONZE_TMPL_BASED = """pipeline: PIPELINE
flowgroup: ENTITY_bronze
presets:
  - default_delta_properties
use_template: bronze_cleanse_template
template_parameters:
  entity_name: ENTITY
  source_table: ENTITY_raw
"""

# --- BRONZE LAYER: direct multi-action ---
BRONZE_DIRECT = """pipeline: PIPELINE
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
BRONZE_MIGRATION = """pipeline: PIPELINE
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
SILVER_CDC = """pipeline: PIPELINE
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
GOLD_MV = """pipeline: PIPELINE
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
        FROM {catalog}.{silver_schema}.ENTITY_SUFFIX
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
GOLD_AGG = """pipeline: PIPELINE
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
        FROM {catalog}.{silver_schema}.ENTITY_SUFFIX
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
MODELLED_DIRECT = """pipeline: PIPELINE
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
      table: ENTITY_SUFFIX
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


def write_array_syntax_file(path, pipeline, template, presets, flowgroup_entries):
    """Write a multi-flowgroup file using array syntax (shared inheritance).

    Args:
        path: Output file path
        pipeline: Shared pipeline name
        template: Shared use_template name (or None)
        presets: List of shared preset names (or None)
        flowgroup_entries: List of dicts with per-flowgroup fields
    """
    lines = [f"pipeline: {pipeline}"]
    if template:
        lines.append(f"use_template: {template}")
    if presets:
        lines.append("presets:")
        for p in presets:
            lines.append(f"  - {p}")
    lines.append("")
    lines.append("flowgroups:")

    for entry in flowgroup_entries:
        lines.append(f"  - flowgroup: {entry['flowgroup']}")
        if "template_parameters" in entry:
            lines.append("    template_parameters:")
            for k, v in entry["template_parameters"].items():
                lines.append(f"      {k}: {v}")
        if "actions" in entry:
            lines.append("    actions:")
            # Indent the raw action YAML by 6 spaces
            for action_line in entry["actions"].strip().split("\n"):
                lines.append(f"      {action_line}")
        lines.append("")

    path.write_text("\n".join(lines))


def write_multidoc_file(path, documents):
    """Write a multi-flowgroup file using --- multi-document syntax.

    Args:
        path: Output file path
        documents: List of complete YAML document strings
    """
    path.write_text("---\n".join(documents))


def make_raw_direct_actions(entity):
    """Return raw action block for a direct (non-template) raw ingestion."""
    return RAW_DIRECT.replace("ENTITY", entity)


def make_bronze_direct_actions(entity):
    """Return the actions: block only for a bronze direct flowgroup."""
    full = BRONZE_DIRECT.replace("ENTITY", entity)
    # Extract just the actions block (everything after "actions:\n")
    idx = full.index("actions:\n")
    return full[idx + len("actions:\n"):]


def make_bronze_migration_actions(entity):
    """Return the actions: block only for a bronze migration flowgroup."""
    full = BRONZE_MIGRATION.replace("ENTITY", entity)
    idx = full.index("actions:\n")
    return full[idx + len("actions:\n"):]


def make_silver_doc(entity, suffix):
    """Return a complete YAML document string for a silver CDC flowgroup."""
    return SILVER_CDC.replace("ENTITY", entity).replace("SUFFIX", suffix)


def make_modelled_doc(entity):
    """Return a complete YAML document string for a modelled flowgroup."""
    return MODELLED_DIRECT.replace("ENTITY", entity)


def main():
    total_flowgroups = 0
    total_files = 0
    template_count = 0
    direct_count = 0
    array_syntax_fgs = 0
    multidoc_syntax_fgs = 0
    single_file_fgs = 0

    # We'll collect multi-fg groups across domains then write them.
    # Strategy for 30% multi-flowgroup (targeting ~1200 of 4000):
    #
    # RAW layer:   First 4 domains (a-d) → template-based ones grouped into array syntax
    #              ~24 entities per domain that are template-based, across 4 domains = ~96 per domain-pair
    #              Group pairs of domains: (a,b) and (c,d) → ~48 fgs per file → 2 array files per pair
    #
    # BRONZE layer: First 4 domains (a-d) → template-based ones into array syntax
    #               ~16 template entities per domain, 4 domains → groups of ~32
    #
    # SILVER layer: First 6 domains (a-f) → multi-doc syntax, 40 entities per domain
    #               Group into files of ~40 each
    #
    # The rest stays as individual files.

    # Domains whose flowgroups get bundled into multi-fg files
    RAW_ARRAY_DOMAINS = set(DOMAINS[:8])        # a-h — template-based raw into array files
    BRONZE_ARRAY_DOMAINS = set(DOMAINS[:8])      # a-h — template-based bronze into array files
    SILVER_MULTIDOC_DOMAINS = set(DOMAINS[:12])  # a-l — CDC silver into multi-doc files
    MODELLED_MULTIDOC_DOMAINS = set(DOMAINS[:12])  # a-l — modelled into multi-doc files

    for domain in DOMAINS:
        entities = ENTITIES_PER_DOMAIN[domain]
        # Pipeline per domain+layer: e.g. "domain_a_raw", "domain_a_bronze", etc.
        # 20 domains × 5 layers = 100 pipelines
        pip_raw = f"{domain}_raw"
        pip_bronze = f"{domain}_bronze"
        pip_silver = f"{domain}_silver"
        pip_gold = f"{domain}_gold"
        pip_modelled = f"{domain}_modelled"

        # =================================================================
        # RAW LAYER (40 entities per domain)
        # =================================================================
        raw_tmpl_entries = []  # for array syntax bundling
        for i, entity in enumerate(entities):
            if i % 5 in (0, 1):
                # Direct CloudFiles — always individual file
                content = RAW_DIRECT.replace("ENTITY", entity).replace("PIPELINE", pip_raw)
                path = BASE / "01_raw" / domain / f"{entity}_ingestion.yaml"
                path.write_text(content)
                direct_count += 1
                single_file_fgs += 1
                total_files += 1
            else:
                # Template-based
                tmpl = RAW_TEMPLATES[i % len(RAW_TEMPLATES)]
                schema = SCHEMA_FILES[i % len(SCHEMA_FILES)]
                template_count += 1

                if domain in RAW_ARRAY_DOMAINS:
                    # Collect for array syntax file
                    raw_tmpl_entries.append({
                        "flowgroup": f"{entity}_ingestion",
                        "template": tmpl,
                        "schema": schema,
                        "template_parameters": {
                            "table_name": f"{entity}_raw",
                            "landing_folder": entity,
                            "schema_file": schema,
                        },
                    })
                else:
                    # Individual file
                    content = (RAW_TMPL_BASED
                               .replace("ENTITY", entity)
                               .replace("TMPL_NAME", tmpl)
                               .replace("SCHEMA_NAME", schema)
                               .replace("PIPELINE", pip_raw))
                    path = BASE / "01_raw" / domain / f"{entity}_ingestion.yaml"
                    path.write_text(content)
                    single_file_fgs += 1
                    total_files += 1
            total_flowgroups += 1

        # Write array syntax files for raw template-based (grouped by template type)
        if raw_tmpl_entries:
            # Group by template name so inheritance works
            by_tmpl = {}
            for e in raw_tmpl_entries:
                by_tmpl.setdefault(e["template"], []).append(e)

            for tmpl_name, entries in by_tmpl.items():
                # Chunk into files of 20-50
                chunk_size = min(len(entries), 40)
                for chunk_idx in range(0, len(entries), chunk_size):
                    chunk = entries[chunk_idx:chunk_idx + chunk_size]
                    suffix = f"_{chunk_idx // chunk_size + 1}" if len(entries) > chunk_size else ""
                    short_tmpl = tmpl_name.replace("_ingestion_template", "")
                    fname = f"{domain}_{short_tmpl}_batch{suffix}.yaml"
                    fpath = BASE / "01_raw" / domain / fname
                    write_array_syntax_file(
                        fpath, pip_raw, tmpl_name,
                        ["default_delta_properties"],
                        chunk,
                    )
                    array_syntax_fgs += len(chunk)
                    total_files += 1

        # =================================================================
        # BRONZE LAYER (40 entities per domain)
        # =================================================================
        bronze_tmpl_entries = []  # for array syntax
        bronze_direct_docs = []   # for multi-doc bundling (not used here, keep individual)
        for i, entity in enumerate(entities):
            if i < 16:
                # Template-based bronze
                template_count += 1
                if domain in BRONZE_ARRAY_DOMAINS:
                    bronze_tmpl_entries.append({
                        "flowgroup": f"{entity}_bronze",
                        "template_parameters": {
                            "entity_name": entity,
                            "source_table": f"{entity}_raw",
                        },
                    })
                else:
                    content = BRONZE_TMPL_BASED.replace("ENTITY", entity).replace("PIPELINE", pip_bronze)
                    path = BASE / "02_bronze" / domain / f"{entity}_bronze.yaml"
                    path.write_text(content)
                    single_file_fgs += 1
                    total_files += 1
            elif i % 5 == 4:
                # Direct with migration — always individual (too complex to bundle)
                content = BRONZE_MIGRATION.replace("ENTITY", entity).replace("PIPELINE", pip_bronze)
                path = BASE / "02_bronze" / domain / f"{entity}_bronze.yaml"
                path.write_text(content)
                direct_count += 1
                single_file_fgs += 1
                total_files += 1
            else:
                # Direct multi-action — always individual
                content = BRONZE_DIRECT.replace("ENTITY", entity).replace("PIPELINE", pip_bronze)
                path = BASE / "02_bronze" / domain / f"{entity}_bronze.yaml"
                path.write_text(content)
                direct_count += 1
                single_file_fgs += 1
                total_files += 1
            total_flowgroups += 1

        # Write array syntax files for bronze template-based
        if bronze_tmpl_entries:
            # All use same template, write as one or two files
            chunk_size = min(len(bronze_tmpl_entries), 40)
            for chunk_idx in range(0, len(bronze_tmpl_entries), chunk_size):
                chunk = bronze_tmpl_entries[chunk_idx:chunk_idx + chunk_size]
                suffix = f"_{chunk_idx // chunk_size + 1}" if len(bronze_tmpl_entries) > chunk_size else ""
                fname = f"{domain}_bronze_batch{suffix}.yaml"
                fpath = BASE / "02_bronze" / domain / fname
                write_array_syntax_file(
                    fpath, pip_bronze, "bronze_cleanse_template",
                    ["default_delta_properties"],
                    chunk,
                )
                array_syntax_fgs += len(chunk)
                total_files += 1

        # =================================================================
        # SILVER LAYER (40 entities per domain)
        # =================================================================
        silver_docs = []  # for multi-doc bundling
        for i, entity in enumerate(entities):
            suffix = "dim" if i % 3 != 0 else "fct"
            direct_count += 1

            if domain in SILVER_MULTIDOC_DOMAINS:
                silver_docs.append(
                    SILVER_CDC.replace("ENTITY", entity).replace("SUFFIX", suffix).replace("PIPELINE", pip_silver)
                )
            else:
                content = SILVER_CDC.replace("ENTITY", entity).replace("SUFFIX", suffix).replace("PIPELINE", pip_silver)
                path = BASE / "03_silver" / domain / f"{entity}_silver_{suffix}.yaml"
                path.write_text(content)
                single_file_fgs += 1
                total_files += 1
            total_flowgroups += 1

        # Write multi-doc files for silver (chunks of ~20)
        if silver_docs:
            chunk_size = 20
            for chunk_idx in range(0, len(silver_docs), chunk_size):
                chunk = silver_docs[chunk_idx:chunk_idx + chunk_size]
                part = chunk_idx // chunk_size + 1
                fname = f"{domain}_silver_batch_{part}.yaml"
                fpath = BASE / "03_silver" / domain / fname
                write_multidoc_file(fpath, chunk)
                multidoc_syntax_fgs += len(chunk)
                total_files += 1

        # =================================================================
        # GOLD LAYER (40 entities per domain) — always individual files
        # =================================================================
        for i, entity in enumerate(entities):
            suffix = "dim" if i % 3 != 0 else "fct"
            if i < 20:
                mv_type = ["summary", "analysis", "report", "dashboard"][i % 4]
                content = GOLD_MV.replace("ENTITY", entity).replace("MVTYPE", mv_type).replace("SUFFIX", suffix).replace("PIPELINE", pip_gold)
                path = BASE / "04_gold" / domain / f"{entity}_{mv_type}_mv.yaml"
            else:
                agg_type = ["daily", "weekly", "monthly", "quarterly"][i % 4]
                agg_trunc = ["day", "week", "mon", "quarter"][i % 4]
                content = (GOLD_AGG
                           .replace("ENTITY", entity)
                           .replace("AGGTYPE", agg_type)
                           .replace("AGGTRUNC", agg_trunc)
                           .replace("SUFFIX", suffix)
                           .replace("PIPELINE", pip_gold))
                path = BASE / "04_gold" / domain / f"{entity}_{agg_type}_agg_mv.yaml"
            path.write_text(content)
            direct_count += 1
            single_file_fgs += 1
            total_files += 1
            total_flowgroups += 1

        # =================================================================
        # MODELLED LAYER (40 entities per domain)
        # =================================================================
        modelled_docs = []
        for i, entity in enumerate(entities):
            suffix = "dim" if i % 3 != 0 else "fct"
            direct_count += 1
            if domain in MODELLED_MULTIDOC_DOMAINS:
                modelled_docs.append(MODELLED_DIRECT.replace("ENTITY", entity).replace("SUFFIX", suffix).replace("PIPELINE", pip_modelled))
            else:
                content = MODELLED_DIRECT.replace("ENTITY", entity).replace("SUFFIX", suffix).replace("PIPELINE", pip_modelled)
                path = BASE / "05_modelled" / domain / f"{entity}_modelled.yaml"
                path.write_text(content)
                single_file_fgs += 1
                total_files += 1
            total_flowgroups += 1

        # Write multi-doc files for modelled (chunks of ~20)
        if modelled_docs:
            chunk_size = 20
            for chunk_idx in range(0, len(modelled_docs), chunk_size):
                chunk = modelled_docs[chunk_idx:chunk_idx + chunk_size]
                part = chunk_idx // chunk_size + 1
                fname = f"{domain}_modelled_batch_{part}.yaml"
                fpath = BASE / "05_modelled" / domain / fname
                write_multidoc_file(fpath, chunk)
                multidoc_syntax_fgs += len(chunk)
                total_files += 1

    multi_total = array_syntax_fgs + multidoc_syntax_fgs
    print(f"Generated {total_flowgroups} flowgroups in {total_files} YAML files")
    print(f"  Template-based: {template_count} ({template_count*100//total_flowgroups}%)")
    print(f"  Direct actions:  {direct_count} ({direct_count*100//total_flowgroups}%)")
    print(f"  Single-file flowgroups:    {single_file_fgs} ({single_file_fgs*100//total_flowgroups}%)")
    print(f"  Multi-flowgroup (array):   {array_syntax_fgs} ({array_syntax_fgs*100//total_flowgroups}%)")
    print(f"  Multi-flowgroup (multidoc):{multidoc_syntax_fgs} ({multidoc_syntax_fgs*100//total_flowgroups}%)")
    print(f"  Total multi-fg:            {multi_total} ({multi_total*100//total_flowgroups}%)")
    print()
    for layer in ["01_raw", "02_bronze", "03_silver", "04_gold", "05_modelled"]:
        files = sum(1 for _ in (BASE / layer).rglob("*.yaml"))
        print(f"  {layer}: {files} files")
    print()
    for domain in DOMAINS:
        files = sum(
            1
            for layer in ["01_raw", "02_bronze", "03_silver", "04_gold", "05_modelled"]
            for _ in (BASE / layer / domain).rglob("*.yaml")
        )
        print(f"  {domain}: {files} files")


if __name__ == "__main__":
    main()
