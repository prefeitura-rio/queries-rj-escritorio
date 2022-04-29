{{
    config(
        materialized='incremental',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        post_hook='CREATE OR REPLACE TABLE `rj-escritorio-dev.meio_ambiente_clima_staging.taxa_precipitacao_alertario_last_partition` AS (SELECT CURRENT_DATETIME("America/Sao_Paulo") AS data_particao)'
    )
}}

SELECT 
    SAFE_CAST(
        REGEXP_REPLACE(id_estacao, r'\.0$', '') AS STRING
    ) id_estacao,
    SAFE_CAST(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_medicao) AS DATETIME
    ) AS data_medicao,
    SAFE_CAST(acumulado_chuva_15_min AS FLOAT64) acumulado_chuva_15_min,
    SAFE_CAST(acumulado_chuva_1_h AS FLOAT64) acumulado_chuva_1_h,
    SAFE_CAST(acumulado_chuva_4_h AS FLOAT64) acumulado_chuva_4_h,
    SAFE_CAST(acumulado_chuva_24_h AS FLOAT64) acumulado_chuva_24_h,
    SAFE_CAST(acumulado_chuva_96_h AS FLOAT64) acumulado_chuva_96_h,
    SAFE_CAST(DATE_TRUNC(DATE(data_medicao), month) AS DATE) data_particao,
FROM `rj-escritorio-dev.meio_ambiente_clima_staging.taxa_precipitacao_alertario`


{% if is_incremental() %}

-- this filter will only be applied on an incremental run
WHERE 
    ano = EXTRACT(YEAR FROM CURRENT_DATE('America/Sao_Paulo')) AND
    mes = EXTRACT(MONTH FROM CURRENT_DATE('America/Sao_Paulo')) AND
    dia = EXTRACT(DAY FROM CURRENT_DATE('America/Sao_Paulo')) AND
    data_medicao > (SELECT 
                        max(data_particao) 
                    FROM 
                        `rj-escritorio-dev.meio_ambiente_clima_staging.taxa_precipitacao_alertario_last_partition`
                    )

{% endif %}
