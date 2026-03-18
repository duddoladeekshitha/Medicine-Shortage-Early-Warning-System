SELECT
  prediction,
  COUNT(*) AS cnt
FROM pharma_catalog.raw.predicted_shortages
GROUP