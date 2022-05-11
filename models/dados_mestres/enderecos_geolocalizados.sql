{{
    config(
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

SELECT
    SAFE_CAST(endereco_completo AS STRING) endereco_completo,
    SAFE_CAST(pais AS STRING) pais,
    SAFE_CAST(estado AS STRING) estado,
    SAFE_CAST(municipio AS STRING) municipio,
    SAFE_CAST(bairro AS STRING) bairro,
    LPAD(SAFE_CAST(REGEXP_REPLACE(id_logradouro, r'\.0$', '') AS STRING), 6, '0') id_logradouro,
    SAFE_CAST(logradouro AS STRING) logradouro,
    SAFE_CAST(SAFE_CAST(numero_porta AS FLOAT64) AS INT64) numero_porta,
    SAFE_CAST(latitude AS FLOAT64) latitude,
    SAFE_CAST(longitude AS FLOAT64) longitude,
    ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64), SAFE_CAST(latitude AS FLOAT64)) AS geometry,
    SAFE_CAST(DATE_TRUNC(DATE(data_particao), month) AS DATE) data_particao,
FROM rj-escritorio-dev.dados_mestres_staging.enderecos_geolocalizados_temp AS t


{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE data_particao > (SELECT max(data_particao) FROM {{ this }})

{% endif %}