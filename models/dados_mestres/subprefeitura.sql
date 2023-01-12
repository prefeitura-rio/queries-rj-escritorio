SELECT 
    SAFE_CAST(REGEXP_REPLACE(TRIM(objectid), r'\.0$', '') AS STRING) AS id_subprefeitura,
    SAFE_CAST(TRIM(subprefeitura) AS STRING) AS subprefeitura,
    SAFE_CAST(REGEXP_REPLACE(st_areashape, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(st_perimetershape, r',', '.') AS FLOAT64) AS perimetro,
    SAFE_CAST(c AS STRING) geometry_wkt,
    SAFE.ST_GEOGFROMTEXT(geometry) AS geometria,
FROM rj-escritorio-dev.dados_mestres_staging.subprefeitura AS t