-- Controlla che tutte le province abbiano la geometria popolata
SELECT COUNT(*) AS null_geom
FROM provinces
WHERE geom IS NULL;