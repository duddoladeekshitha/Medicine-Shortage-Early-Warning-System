SELECT
  drug_name,
  facility_name,
  quantity_in_stock,
  risk_score,
  prediction,
  probability
FROM pharma_catalog.raw.predicted_shortages
ORDER BY risk_score DESC, quantity_in_stock ASC;
