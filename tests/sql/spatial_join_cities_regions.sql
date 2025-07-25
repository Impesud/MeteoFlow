-- Verifica che ogni città abbia una regione assegnata
SELECT COUNT(*) AS unmatched
FROM cities c
LEFT JOIN regions r ON c.region_istat = r.region_istat
WHERE c.region_istat IS NULL;