SELECT
  _source_pipeline,
  event_type,
  date_trunc('DAY', timestamp) AS event_day,
  count(*) AS daily_event_count
FROM all_pipelines_event_log
WHERE event_type IN ('FLOW_PROGRESS', 'DATASET_CREATED')
GROUP BY _source_pipeline, event_type, date_trunc('DAY', timestamp)
