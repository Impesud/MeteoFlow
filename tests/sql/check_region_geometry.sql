-- Controlla che tutte le regioni abbiano la colonna geom popolata
SELECT COUNT(*) AS null_geom
FROM regions
WHERE geom IS NULL;