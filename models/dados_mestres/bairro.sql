SELECT 
    SAFE_CAST(REGEXP_REPLACE(LTRIM(CODBAIRRO ,'0') , r'\.0$', '') AS STRING) id_bairro,
    SAFE_CAST(RTRIM(NOME) AS STRING) nome,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(AREA_PLANE ,'0'), r'\.0$', '') AS STRING) id_area_planejamento,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(Cod_RP ,'0'), r'\.0$', '') AS STRING) id_regiao_planejamento,
    SAFE_CAST(RP AS STRING) nome_regiao_planejamento,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(CODRA ,'0'), r'\.0$', '') AS STRING) id_regiao_administrativa,
    SAFE_CAST(REGIAO_ADM AS STRING) nome_regiao_administrativa,
    SAFE_CAST(Area AS FLOAT64) area,
    SAFE_CAST(SHAPESTLength AS FLOAT64) perimetro, 
    SAFE_CAST(geometry AS STRING) geometry, # TODO, resolver id_bairro = '004' e converter para GEOGRAPHY
FROM `rj-escritorio-dev.dados_mestres_staging.bairro`

