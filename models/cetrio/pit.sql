SELECT
  SAFE_CAST(cod_cet AS STRING) codigo_cet,
  SAFE_CAST(logradouro AS STRING) logradouro,
  SAFE_CAST(bairro AS STRING) bairro,
  SAFE_CAST(codigo_equipamento AS STRING) codigo_equipamento,
  SAFE_CAST(local_equipamento AS STRING) local_equipamento,
  SAFE_CAST(latitude AS FLOAT64) latitude,
  SAFE_CAST(longitude AS FLOAT64) longitude,
  ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64), SAFE_CAST(latitude AS FLOAT64)) as coordenada_geografica,
  SAFE_CAST(ap AS STRING) area_planejamento,
  SAFE_CAST(data as DATE FORMAT "YYYY-MM-DD") data,
  SAFE_CAST(periodo AS STRING) periodo,
  SAFE_CAST(hora_inicio AS TIME FORMAT "HH24:MI:SS") horario_inicio,
  SAFE_CAST(hora_termino AS TIME FORMAT "HH24:MI:SS") horario_fim,
  SAFE_CAST(total_registro AS INT64) quantidade_veiculos_registrados,
  SAFE_CAST(total_placas AS INT64) quantidade_placas_registradas,
FROM `rj-escritorio-dev.cetrio_staging.pit`