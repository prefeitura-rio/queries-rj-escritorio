SELECT
    * except(
        geometria,
        geometry
    ) 
    SAFE_CAST(geometry_wkt AS STRING) AS geometry_wkt, 
    SAFE.ST_GEOGFROMTEXT(geometry) AS geometry, #TODO: CONVERT TO GEOGRAPHY
FROM `rj-escritorio-dev.dados_mestres_staging.aeis_bairro_maravilha` 