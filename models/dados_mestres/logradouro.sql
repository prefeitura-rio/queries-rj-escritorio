{{ config(
  persist_docs={"relation": true, "columns": true}
) }}

SELECT 
    SAFE_CAST(REGEXP_REPLACE(CL, r'\.0$', '') AS STRING) id_logradouro,
    SAFE_CAST(Nome AS STRING) nome,
    SAFE_CAST(NOME_PARCIAL AS STRING) nome_parcial,
    SAFE_CAST(COMPLETO AS STRING) nome_completo,
    SAFE_CAST(REGEXP_REPLACE(COD_TRECHO, r'\.0$', '') AS STRING) id_trecho,
    SAFE_CAST(REGEXP_REPLACE(COD_SIT_TRECHO, r'\.0$', '') AS STRING) id_situacao_trecho,
    SAFE_CAST(SIT_TRECHO AS STRING) situacao_trecho,
    SAFE_CAST(REGEXP_REPLACE(COD_TIPO_LOGRA, r'\.0$', '') AS STRING) id_tipo,
    SAFE_CAST(TIPO_LOGRA_EXT AS STRING) tipo,
    SAFE_CAST(REGEXP_REPLACE(Cod_Bairro, r'\.0$', '') AS STRING) id_bairro,
    SAFE_CAST(NP_INI_PAR AS INT64) inicio_numero_porta_par,
    SAFE_CAST(NP_FIN_PAR AS INT64) final_numero_porta_par,
    SAFE_CAST(NP_INI_IMP AS INT64) inicio_numero_porta_impar,
    SAFE_CAST(NP_FIN_IMP AS INT64) final_numero_porta_impar,
    SAFE_CAST(HIERARQUIA AS STRING) hierarquia,
    SAFE_CAST(REGEXP_REPLACE(CHAVEGEO_TR, r'\.0$', '') AS STRING) id_chavegeo,
    ST_GEOGFROMTEXT(geometry) AS geometry,
FROM `rj-escritorio-dev.dados_mestres_staging.logradouro` 