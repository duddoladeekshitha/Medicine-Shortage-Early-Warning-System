SELECT
  drug_name,
  facility_name,
  stock_coverage_days,
  quantity_in_stock
FROM pharma_catalog.raw.predicted_shortages
ORDER BY stock_coverage_days ASC, quantity_in_stock ASC
LIMIT 20;