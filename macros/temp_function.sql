{% macro bigquery__create_table_as(temporary, relation, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}
  {%- set udf = config.get('udf', none) -%} # I added this part
  {{ udf if udf is not none  }}  # I added this part

  create or replace table {{ relation }}
  {{ partition_by(raw_partition_by) }}
  {{ cluster_by(raw_cluster_by) }}
  as (
    {{ sql }}
  );
{% endmacro %}