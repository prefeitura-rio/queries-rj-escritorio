WITH data_maxima_com_dados AS (
  SELECT
    MAX(data) data_maxima
  FROM `rj-setur.turismo_fluxo_visitantes.aeroportos`
  WHERE data_referencia_dashboard = TRUE
)

SELECT
  pt.*
FROM `rj-setur.turismo_fluxo_visitantes.aeroportos` as pt
LEFT JOIN data_maxima_com_dados ON TRUE
WHERE pt.data <= data_maxima_com_dados.data_maxima