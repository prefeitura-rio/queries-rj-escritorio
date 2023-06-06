SELECT 
  MAX(data) as data_atualizacao,
  "subsisio_sppo" as tela
FROM `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
WHERE data >= '2021-01-01'

UNION ALL

SELECT 
  DATE(creation_time) AS data_atualizacao,
  "obras" as tela
FROM rj-smi.infraestrutura_siscob_obras_dashboard.INFORMATION_SCHEMA.TABLES
WHERE table_name = 'obra'

UNION ALL 

SELECT
  MAX(dashboard_data_referencia_ultimo_resultado) as data_atualizacao,
  "metas_pe" as tela
FROM `rj-smfp.planejamento_gestao_dashboard_metas.todos_detalhes`
WHERE origem_meta = "Plano Estratégico"

UNION ALL

SELECT 
  LAST_DAY(MAX(ano_mes)) as data_atualizacao,
  "1746" as tela
FROM `rj-escritorio-dev.comunicacao_executiva.performance_subprefeituras_1746`

UNION ALL

SELECT
  MAX(data) as data_atualizacao,
  "aeroportos" as tela
FROM `rj-setur.turismo_fluxo_visitantes.aeroportos` 
WHERE passageiros_total IS NOT NULL

UNION ALL

SELECT
  MAX(data) as data_atualizacao,
  "rodoviarias" as tela
FROM `rj-setur.turismo_fluxo_visitantes.rodoviarias` 
WHERE passageiros_total IS NOT NULL

UNION ALL

SELECT
  MAX(data) as data_atualizacao,
  "iss_turistico" as tela
FROM `rj-setur.turismo_fluxo_visitantes.iss_turistico`
WHERE total_arrecadado IS NOT NULL AND total_arrecadado != 0 AND NOT IS_NAN(total_arrecadado)

UNION ALL

(WITH pt_ultima_atualizacao AS (
  SELECT
    ferramenta_turistica,
    MAX(data) data_atualizacao
  FROM `rj-setur.turismo_fluxo_visitantes.pontos_turisticos` 
  WHERE metrica_valor IS NOT NULL AND metrica_valor != 0
  GROUP BY ferramenta_turistica
)
  SELECT
    MIN(data_atualizacao) as data_atualizacao,
    "pontos_turisticos" as tela
  FROM pt_ultima_atualizacao 
)

UNION ALL

SELECT
  MAX(data) as data_atualizacao,
  "ocupacao_rede_hoteleira" as tela
FROM `rj-setur.turismo_fluxo_visitantes.rede_hoteleira_ocupacao_geral` 
WHERE taxa_ocupacao IS NOT NULL AND taxa_ocupacao != 0

UNION ALL 

SELECT
  DATE "2023-03-31" as data_atualizacao,
  "twitter" as tela