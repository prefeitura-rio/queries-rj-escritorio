WITH data_maxima_com_dados AS (
  SELECT
    MAX(data) data_maxima
  FROM `rj-setur.turismo_fluxo_visitantes.pontos_turisticos`
  WHERE data_referencia_dashboard = TRUE
)

SELECT
  pt.*
FROM `rj-setur.turismo_fluxo_visitantes.pontos_turisticos` as pt
LEFT JOIN data_maxima_com_dados ON TRUE
WHERE pt.data <= data_maxima_com_dados.data_maxima