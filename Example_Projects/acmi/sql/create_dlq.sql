CREATE TABLE IF NOT EXISTS acme_edw_dev.edw_raw.universal_dlq (
    _dlq_sk            STRING    NOT NULL,
    _dlq_source_table  STRING    NOT NULL,
    _dlq_status        STRING    NOT NULL,  -- 'quarantined' | 'fixed'
    _dlq_timestamp     TIMESTAMP NOT NULL,
    _dlq_failed_rules  ARRAY<STRUCT<name: STRING, rule: STRING>>,
    _dlq_rescued_data  STRING,
    _row_data          VARIANT   NOT NULL
)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 90 days'
);

CREATE TABLE IF NOT EXISTS acme_edw_dev.edw_raw.universal_dlq_outbox (
    _dlq_sk            STRING    NOT NULL,
    _dlq_source_table  STRING    NOT NULL,
    _row_data          VARIANT   NOT NULL,
    _dlq_recycled_at   TIMESTAMP NOT NULL
)
TBLPROPERTIES (
    'delta.enableRowTracking' = 'true'
);
