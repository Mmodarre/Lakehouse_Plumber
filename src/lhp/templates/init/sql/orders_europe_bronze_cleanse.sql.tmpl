-- This is a sample SQL transform file referenced by a SQL transform action
-- Used to cleanse and transform raw data from bronze layer using Spark SQL
-- Uses stream() function for streaming datasets to enable incremental processing


SELECT 
  o_orderkey as order_id,
  o_custkey as customer_id,
  o_orderstatus as order_status, 
  o_totalprice as total_price,
  o_orderdate as order_date,
  o_orderpriority as order_priority,
  o_clerk as clerk,
  o_shippriority as ship_priority,
  o_comment as comment,
  cast(last_modified_dt as TIMESTAMP) as last_modified_dt,
  * EXCEPT(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, last_modified_dt,_rescued_data)
FROM stream(v_orders_europe_raw)

