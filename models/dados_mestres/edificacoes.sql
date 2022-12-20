SELECT 
    SAFE_CAST(TRIM(fid) AS STRING) AS id,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cod_unico), r'\.0$', '') AS STRING) AS id_unico,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cod_projec), r'\.0$', '') AS STRING) AS id_projecao,
    SAFE_CAST(REGEXP_REPLACE(TRIM(cod_lote), r'\.0$', '') AS STRING) AS id_lote,
    SAFE_CAST(REGEXP_REPLACE(TRIM(clnp), r'\.0$', '') AS STRING) AS id_logradouro_numero_porta,
    SAFE_CAST(TRIM(tipo) AS STRING) AS tipo,
    SAFE_CAST(REGEXP_REPLACE(topo, r',', '.') AS FLOAT64) AS altitude_topo,
    SAFE_CAST(REGEXP_REPLACE(base, r',', '.') AS FLOAT64) AS altitude_base,
    SAFE_CAST(REGEXP_REPLACE(altura, r',', '.') AS FLOAT64) AS altura,
    SAFE_CAST(REGEXP_REPLACE(shape__area, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(shape__length, r',', '.') AS FLOAT64) AS comprimento,
    SAFE_CAST(geometry_wkt AS STRING) AS geometry_wkt, 
    SAFE.ST_GEOGFROMTEXT(geometry) AS geometry, #TODO: CONVERT TO GEOGRAPHY
FROM rj-escritorio-dev.dados_mestres_staging.edificacoes AS t