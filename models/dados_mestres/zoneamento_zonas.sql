SELECT 
    SAFE_CAST(REGEXP_REPLACE(TRIM(codigo), r'\.0$', '') AS STRING) AS id_zona,
    SAFE_CAST(TRIM(sigla) AS STRING) AS sigla_zona,
    SAFE_CAST(TRIM(tipo) AS STRING) AS tipo_zona,
    SAFE_CAST(TRIM(nome) AS STRING) AS nome,
    SAFE_CAST(TRIM(legislacao) AS STRING) AS legislacao,
    SAFE_CAST(TRIM(abrev) AS STRING) AS abreviacao_zona,
    SAFE_CAST(TRIM(info_compl) AS STRING) AS informacao_complementar,
    SAFE_CAST(REGEXP_REPLACE(TRIM(codap), r'\.0$', '') AS STRING) AS id_area_planejamento,
    SAFE_CAST(REGEXP_REPLACE(st_areashape, r',', '.') AS FLOAT64) AS area,
    SAFE_CAST(REGEXP_REPLACE(shape_leng, r',', '.') AS FLOAT64) AS comprimento,
    SAFE_CAST(REGEXP_REPLACE(st_perimetershape, r',', '.') AS FLOAT64) AS perimetro,
    ST_GEOGFROMTEXT(geometry) AS geometria,
FROM rj-escritorio-dev.dados_mestres_staging.zoneamento_zonas AS t