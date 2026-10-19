SELECT
  market_segment,
  COUNT(*) AS customers
FROM v_{{ entity }}_raw
WHERE market_segment IS NOT NULL
GROUP BY market_segment
