SELECT
  CASE
    WHEN supplier_reliability_score >= 0.9 THEN 'A - Very Reliable'
    WHEN supplier_reliability_score >= 0.7 THEN 'B - Reliable'
    WHEN supplier_reliability_score >= 0.5 THEN 'C - Moderate'
    ELSE 'D - Low / Unknown'
  END AS reliability_band,
  COUNT(*) AS medicines
FROM pharma_catalog.raw.predicted_shortages
GROUP BY 1
ORDER BY reliability_band;