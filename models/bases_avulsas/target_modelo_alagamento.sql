CREATE OR REPLACE TABLE `rj-escritorio-dev.bases_avulsas.target` AS 
WITH 
  dim_timestamp AS (
    SELECT timestamp_truncado FROM
    UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('2016-01-01 00:00:00'), TIMESTAMP('2022-08-01 00:00:00'), INTERVAL 1 HOUR)) timestamp_truncado 
    ),
  contagem_eventos_hora AS (
    SELECT
      d.timestamp_truncado, 
      SUM( 
        CASE WHEN id_pop IN ('33', '32', '31', '6', '5') THEN 1 ELSE 0 END) as eventos_alag_hora,
      COUNT(DISTINCT id_evento) AS eventos_hora
    FROM `rj-cor.administracao_servicos_publicos.eventos` ev
    RIGHT JOIN dim_timestamp d
      on d.timestamp_truncado= TIMESTAMP_TRUNC(TIMESTAMP(ev.data_inicio), HOUR) AND ev.data_particao >= '2016-01-01'
    GROUP BY 
      d.timestamp_truncado
    ORDER BY 
      d.timestamp_truncado
  )
-- Quantos eventos irão acontecer dali 2h?
SELECT *,
  LEAD(eventos_alag_hora, 2) OVER (ORDER BY timestamp_truncado ASC) AS eventos_alag_2h,
  LEAD(eventos_alag_hora, 6) OVER (ORDER BY timestamp_truncado ASC) AS eventos_alag_6h,
  LEAD(eventos_alag_hora, 12) OVER (ORDER BY timestamp_truncado ASC) AS eventos_alag_12h,
  LEAD(eventos_alag_hora, 24) OVER (ORDER BY timestamp_truncado ASC) AS eventos_alag_24h,
  LEAD(eventos_hora, 2) OVER (ORDER BY timestamp_truncado ASC) AS eventos_2h,
  LEAD(eventos_hora, 6) OVER (ORDER BY timestamp_truncado ASC) AS eventos_6h,
  LEAD(eventos_hora, 12) OVER (ORDER BY timestamp_truncado ASC) AS eventos_12h,
  LEAD(eventos_hora, 24) OVER (ORDER BY timestamp_truncado ASC) AS eventos_24h
FROM contagem_eventos_hora
ORDER BY 
    timestamp_truncado
