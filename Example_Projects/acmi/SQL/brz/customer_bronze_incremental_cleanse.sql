SELECT
    xxhash64(
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment,
        last_modified_dt
    ) as customer_key,
    c_custkey as customer_id,
    c_name as name,
    c_address as address,
    c_nationkey as nation_id,
    c_phone as phone,
    c_acctbal as account_balance,
    c_mktsegment as market_segment,
    c_comment as comment,
    last_modified_dt,
    *
EXCEPT
(
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment,
        last_modified_dt,
        _rescued_data
    )
FROM
    stream(v_customer_raw)