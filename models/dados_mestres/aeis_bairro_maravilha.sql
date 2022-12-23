
SELECT 
    SAFE_CAST(objectid_1 AS STRING) id_object_1,
    SAFE_CAST(objectid AS STRING) id_object,
    SAFE_CAST(oid_ AS STRING) oid, # somente valor 0
    SAFE_CAST(name AS STRING) nome_aies_bairro_maravilha,
    SAFE_CAST(folderpath AS STRING) folderpath, # verificar metadado
    SAFE_CAST(symbolid AS STRING) symbolid, # verificar metadado
    SAFE_CAST(altmode AS STRING) altmode, # somente valor 0
    SAFE_CAST(base AS STRING) base, # somente valor 0
    SAFE_CAST(clamped AS STRING) clamped, # verificar metadado
    SAFE_CAST(extruded AS STRING) extruded, # somente valor 0
    SAFE_CAST(snippet AS STRING) snippet, # sem valores
    SAFE_CAST(popupinfo AS STRING) popuinfo, # sem valores
    SAFE_CAST(shape_leng AS STRING) shape_leng,
    SAFE_CAST(bairro AS STRING) nome_bairro, 
    SAFE_CAST(sequencia AS STRING) sequencia,
    SAFE_CAST(shape_le_1 AS STRING) comprimento_1,
    SAFE_CAST(legislacao AS STRING) legislacao,
    SAFE_CAST(codigo AS STRING) codigo, # valor único: AEIS
    SAFE_CAST(tipo AS STRING) tipo, # valor único: AEI
    SAFE_CAST(shape__length AS STRING) comprimento,
    SAFE_CAST(geometry_wkt AS STRING) geometry_wkt, 
    SAFE.ST_GEOGFROMTEXT(geometry) AS geometry, #TODO: CONVERT TO GEOGRAPHY
FROM `rj-escritorio-dev.dados_mestres.aeis_bairro_maravilha`

