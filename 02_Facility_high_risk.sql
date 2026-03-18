SELECT
  facility_name,
  COUNT(*) AS total_medicines,
  SUM(CASE WHEN prediction = 2 THEN 1 ELSE 0 END) AS high_risk_meds
FROM pharma_catalog.raw.predicted_shortages
GROUP BY facility_name
ORDER BY high_risk_meds DESC, total_medicines DESC