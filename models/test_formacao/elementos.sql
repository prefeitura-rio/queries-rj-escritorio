SELECT 
  SAFE_CAST(data as DATE) as data,
  SAFE_CAST(number as INT64) as numero,
  element as elemento,
   {% for ELEMENTO in ELEMENTOS_LISTA %}
    CASE WHEN element = '{{ELEMENTO}}' then TRUE ELSE FALSE end) as indicador_{{ELEMENTO}},
   {% endfor %}
FROM `rj-escritorio-dev.test_formacao_staging.test_table_2`
WHERE data < "{{var('ELEMENTOS_DATA_INICIAL')}}"