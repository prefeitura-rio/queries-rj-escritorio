SELECT
    SAFE_CAST(endereco_completo AS STRING) endereco_completo,
    SAFE_CAST(pais AS STRING) pais,
    SAFE_CAST(estado AS STRING) estado,
    SAFE_CAST(municipio AS STRING) municipio,
    SAFE_CAST(bairro AS STRING) bairro,
    SAFE_CAST(REGEXP_REPLACE(id_logradouro, r'\.0$', '') AS STRING) id_logradouro,
    SAFE_CAST(logradouro AS STRING) logradouro,
    SAFE_CAST(numero_porta AS INT64) numero_porta,
    SAFE_CAST(latitude AS FLOAT64) latitude,
    SAFE_CAST(longitude AS FLOAT64) longitude,
    ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64), SAFE_CAST(latitude AS FLOAT64)) AS geometry
FROM rj-escritorio-dev.dados_mestres_staging.enderecos_geolocalizados AS t