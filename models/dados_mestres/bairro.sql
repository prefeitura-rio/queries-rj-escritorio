WITH t as (
SELECT 
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codbairro ,'0') , r'\.0$', '') AS STRING) id_bairro,
    SAFE_CAST(RTRIM(nome) AS STRING) nome,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(area_plane ,'0'), r'\.0$', '') AS STRING) id_area_planejamento,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(cod_rp ,'0'), r'\.0$', '') AS STRING) id_regiao_planejamento,
    SAFE_CAST(rp AS STRING) nome_regiao_planejamento,
    SAFE_CAST(REGEXP_REPLACE(LTRIM(codra ,'0'), r'\.0$', '') AS STRING) id_regiao_administrativa,
    SAFE_CAST(INITCAP(RTRIM(regiao_adm)) AS STRING) nome_regiao_administrativa,
    SAFE_CAST(area AS FLOAT64) area,
    SAFE_CAST(shapestlength AS FLOAT64) perimetro, 
    SAFE_CAST(geometry AS STRING) geometria_wkt,
    SAFE.ST_GEOGFROMTEXT(geometry) geometria # TODO, resolver id_bairro = '004' e converter para GEOGRAPHY
FROM `rj-escritorio-dev.dados_mestres_staging.bairro`
)
SELECT 
  id_bairro,
  t.nome,
  t.id_area_planejamento,
  t.id_regiao_planejamento,
  t.nome_regiao_planejamento,
  t.id_regiao_administrativa,
  t.nome_regiao_administrativa,
  t2.nome subprefeitura,
  area,
  perimetro,
  geometria_wkt,
  geometria
FROM t
LEFT JOIN `rj-escritorio-dev.dados_mestres.subprefeituras_regiao_adm` t2
  ON t.id_regiao_administrativa = cast(t2.id_regiao_administrativa as string)
