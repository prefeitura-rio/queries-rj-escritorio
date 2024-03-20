{{
    config(
        materialized='view'
    )
}}

WITH last_partition AS (
  SELECT MAX(data_particao) AS data_particao
  FROM rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa
  WHERE data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
)

,dados_api_cadunico as (  
SELECT
  DISTINCT
  documento_pessoa.cpf,
  identificacao_primeira_pessoa.nis,
  -- identificacao_primeira_pessoa.id_parentesco_responsavel_familia,
  identificacao_primeira_pessoa.parentesco_responsavel_familia,
  identificacao_primeira_pessoa.nome,
  identificacao_primeira_pessoa.apelido,
  identificacao_primeira_pessoa.data_nascimento,
  -- identificacao_primeira_pessoa.id_sexo,
  identificacao_primeira_pessoa.sexo,
  -- identificacao_primeira_pessoa.id_raca_cor,
  identificacao_primeira_pessoa.raca_cor,
  identificacao_primeira_pessoa.nome_pai,
  identificacao_primeira_pessoa.nome_mae,
  -- documento_pessoa.id_certidao_civil,
  documento_pessoa.certidao_civil,
  identificacao_primeira_pessoa.pais_nascimento,
  identificacao_primeira_pessoa.sigla_uf_municipio_nascimento,
  identificacao_primeira_pessoa.municipio_nascimento,
  renda.renda_emprego_ultimo_mes,
  condicao_rua.dorme_rua,
  seguranca_alimentar.snas_bpc_deficiente,
  seguranca_alimentar.snas_bpc_idoso,
  -- familia.id_familia_indigena,
  familia.familia_indigena,
  -- familia.id_familia_quilombola,
  familia.familia_quilombola,
  -- pessoa_deficiencia.id_tem_deficiencia,
  pessoa_deficiencia.tem_deficiencia,
  pessoa_deficiencia.deficiencia_cegueira,
  pessoa_deficiencia.deficiencia_baixa_visao,
  pessoa_deficiencia.deficiencia_surdez_profunda,
  pessoa_deficiencia.deficiencia_surdez_leve,
  pessoa_deficiencia.deficiencia_fisica,
  pessoa_deficiencia.deficiencia_mental,
  pessoa_deficiencia.deficiencia_sindrome_down,
  pessoa_deficiencia.deficiencia_transtorno_mental,
  documento_pessoa.livro_certidao_obito_excluido,
  documento_pessoa.folha_certidao,
  documento_pessoa.data_emissao_certidao,
  documento_pessoa.sigla_uf_certidao,
  documento_pessoa.municipio_certidao,
  -- documento_pessoa.id_municipio_certidao,
  -- documento_pessoa.id_cartorio_certidao,
  documento_pessoa.cartorio_certidao,
  documento_pessoa.id_complemento_rg,
  documento_pessoa.data_emissao_rg,
  documento_pessoa.sigla_uf_rg,
  documento_pessoa.orgao_emissor_rg,
  documento_pessoa.id_carteira_trabalho,
  documento_pessoa.id_serie_carteira_trabalho,
  documento_pessoa.data_emissao_carteira_trabalho,
  documento_pessoa.sigla_uf_carteira_trabalho,
  documento_pessoa.id_termi_matricula_certidao,
  documento_pessoa.rg,
  familia.cras_creas,
  identificacao_controle.cep,
  identificacao_controle.tipo_logradouro,
  identificacao_controle.titulo_logradouro,
  identificacao_controle.logradouro,
  identificacao_controle.numero_logradouro,
  identificacao_controle.complemento,
  identificacao_controle.complemento_adicional,
  identificacao_controle.unidade_territorial,
  identificacao_controle.refencia_logradouro,
  -- domicilio.id_especie_domicilio,
  domicilio.especie_domicilio,
  domicilio.quantidade_comodos_domicilio,
  domicilio.quantidade_comodos_dormitorio,
  familia.pessoas_domicilio,
  familia.familias_domicilio,
  -- domicilio.id_material_piso_domicilio,
  domicilio.material_piso_domicilio,
  -- domicilio.id_material_domicilio,
  domicilio.material_domicilio,
  contato.contato_ddd,
  contato.contato_telefone,
  -- contato.id_contato_tipo,
  contato.contato_tipo,
  contato.contato_2_ddd,
  contato.contato_2_telefone,
  -- contato.id_contato_2_tipo,
  contato.contato_2_tipo,
  contato.email,
  escolaridade.frequenta_escola,
  -- escolaridade.id_curso_frequenta,
  -- escolaridade.id_curso_mais_elevado_frequentou,
  -- escolaridade.id_ano_serie_frequenta,
  escolaridade.curso_frequenta,
  escolaridade.curso_mais_elevado_frequentou,
  escolaridade.ano_serie_frequenta,
  familia.despesa_aluguel,
  familia.nao_tem_despesa_aluguel,
  -- domicilio.id_possui_agua_encanada_domicilio,
  -- domicilio.id_forma_abatecimento_agua_domicilio,
  -- domicilio.id_possui_banheiro_domicilio,
  -- domicilio.id_escoamento_sanitario_domicilio,
  -- domicilio.id_destino_lixo_domicili o,
  -- domicilio.id_iluminacao_domicilio,
  -- domicilio.id_calcamento_domicilio,
  domicilio.possui_agua_encanada_domicilio,
  domicilio.forma_abatecimento_agua_domicilio,
  domicilio.possui_banheiro_domicilio,
  domicilio.escoamento_sanitario_domicilio,
  domicilio.destino_lixo_domicilio,
  domicilio.iluminacao_domicilio,
  domicilio.calcamento_domicilio,
  familia.estabelecimento_saude,
  -- identificacao_controle.id_condicao_cadastro,
  identificacao_controle.condicao_cadastro

FROM
  `rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa` identificacao_primeira_pessoa
INNER JOIN last_partition 
  ON identificacao_primeira_pessoa.data_particao = last_partition.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.documento_pessoa` documento_pessoa
  ON identificacao_primeira_pessoa.id_familia = documento_pessoa.id_familia
  AND identificacao_primeira_pessoa.id_membro_familia = documento_pessoa.id_membro_familia
  AND identificacao_primeira_pessoa.data_particao = documento_pessoa.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.escolaridade` escolaridade
  ON identificacao_primeira_pessoa.id_familia = escolaridade.id_familia
  AND identificacao_primeira_pessoa.id_membro_familia = escolaridade.id_membro_familia
  AND identificacao_primeira_pessoa.data_particao = escolaridade.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.pessoa_deficiencia` pessoa_deficiencia
  ON identificacao_primeira_pessoa.id_familia = pessoa_deficiencia.id_familia
  AND identificacao_primeira_pessoa.id_membro_familia = pessoa_deficiencia.id_membro_familia
  AND identificacao_primeira_pessoa.data_particao = pessoa_deficiencia.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.condicao_rua` condicao_rua
  ON identificacao_primeira_pessoa.id_familia = condicao_rua.id_familia
  AND identificacao_primeira_pessoa.id_membro_familia = condicao_rua.id_membro_familia
  AND identificacao_primeira_pessoa.data_particao = condicao_rua.data_particao
LEFT JOIN rj-smas.protecao_social_cadunico.renda renda
  ON identificacao_primeira_pessoa.id_familia = renda.id_familia
  AND identificacao_primeira_pessoa.data_particao = renda.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.seguranca_alimentar` seguranca_alimentar
  ON identificacao_primeira_pessoa.id_familia = seguranca_alimentar.id_familia
  AND identificacao_primeira_pessoa.data_particao = seguranca_alimentar.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.domicilio` domicilio
  ON identificacao_primeira_pessoa.id_familia = domicilio.id_familia
  AND identificacao_primeira_pessoa.data_particao = domicilio.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.familia` familia
  ON identificacao_primeira_pessoa.id_familia = familia.id_familia
  AND identificacao_primeira_pessoa.data_particao = familia.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.identificacao_controle` identificacao_controle
  ON identificacao_primeira_pessoa.id_familia = identificacao_controle.id_familia
  AND identificacao_primeira_pessoa.data_particao = identificacao_controle.data_particao
LEFT JOIN `rj-smas.protecao_social_cadunico.contato` contato
  ON identificacao_primeira_pessoa.id_familia = contato.id_familia
  AND identificacao_primeira_pessoa.data_particao = contato.data_particao
  WHERE identificacao_primeira_pessoa.data_particao = last_partition.data_particao
  AND identificacao_primeira_pessoa.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
  AND identificacao_primeira_pessoa.estado_cadastral = "Cadastrado"

)
SELECT 
    cpf,
    JSON_OBJECT(
        'parentesco', JSON_OBJECT(
            -- 'id_parentesco_responsavel_familia', id_parentesco_responsavel_familia,
            'parentesco_responsavel_familia', parentesco_responsavel_familia,
            'nome_pai', nome_pai,
            'nome_mae', nome_mae
        ),
        'identidade', JSON_OBJECT(
            'data_nascimento', data_nascimento,
            'nome', nome,
            'apelido', apelido,
            'sexo', sexo,
            'raca_cor', raca_cor,
            'certidao_civil', certidao_civil
            -- 'id_sexo', id_sexo,
            -- 'id_raca_cor', id_raca_cor,
            -- 'id_certidao_civil', id_certidao_civil
        ),
        'origem_nascimento', JSON_OBJECT(
            'pais_nascimento', pais_nascimento,
            'sigla_uf_municipio_nascimento', sigla_uf_municipio_nascimento,
            'municipio_nascimento', municipio_nascimento
        ),
        'renda', JSON_OBJECT(
            'renda_emprego_ultimo_mes', renda_emprego_ultimo_mes,
            'snas_bpc_deficiente', snas_bpc_deficiente,
            'snas_bpc_idoso', snas_bpc_idoso
        ),
        'identificacao_etnica', JSON_OBJECT(
            'familia_indigena', familia_indigena,
            'familia_quilombola', familia_quilombola
            -- 'id_familia_indigena', id_familia_indigena,
            -- 'id_familia_quilombola', id_familia_quilombola
        ),
        'deficiencia', JSON_OBJECT(
            -- 'id_tem_deficiencia', id_tem_deficiencia,
            'tem_deficiencia', tem_deficiencia,
            'deficiencia_cegueira', deficiencia_cegueira,
            'deficiencia_baixa_visao', deficiencia_baixa_visao,
            'deficiencia_surdez_profunda', deficiencia_surdez_profunda,
            'deficiencia_surdez_leve', deficiencia_surdez_leve,
            'deficiencia_fisica', deficiencia_fisica,
            'deficiencia_mental', deficiencia_mental,
            'deficiencia_sindrome_down', deficiencia_sindrome_down,
            'deficiencia_transtorno_mental', deficiencia_transtorno_mental
        ),
        'documentos', JSON_OBJECT(
            'certidao', JSON_OBJECT(
                'cartorio_certidao', cartorio_certidao,
                'livro_certidao_obito_excluido', livro_certidao_obito_excluido,
                'folha_certidao', folha_certidao,
                'data_emissao_certidao', data_emissao_certidao,
                'sigla_uf_certidao', sigla_uf_certidao,
                'municipio_certidao', municipio_certidao,
                -- 'id_municipio_certidao', id_municipio_certidao,
                -- 'id_cartorio_certidao', id_cartorio_certidao,
                'cartorio_certidao', cartorio_certidao,
                'termi_matricula_certidao', id_termi_matricula_certidao
            ),
            'ctps', JSON_OBJECT(
                'carteira_trabalho', id_carteira_trabalho,
                'serie_carteira_trabalho', id_serie_carteira_trabalho,
                'data_emissao_carteira_trabalho', data_emissao_carteira_trabalho,
                'sigla_uf_carteira_trabalho', sigla_uf_carteira_trabalho,
                'nis', nis
            ),
            'rg', JSON_OBJECT(
                'complemento_rg', id_complemento_rg,
                'data_emissao_rg', data_emissao_rg,
                'sigla_uf_rg', sigla_uf_rg,
                'orgao_emissor_rg', orgao_emissor_rg,
                'rg', rg
            )
        ),
        'endereco', JSON_OBJECT(
            'cep', cep,
            'tipo_logradouro', tipo_logradouro,
            'titulo_logradouro', titulo_logradouro,
            'logradouro', logradouro,
            'numero_logradouro', numero_logradouro,
            'complemento', complemento,
            'complemento_adicional', complemento_adicional,
            'unidade_territorial', unidade_territorial,
            'refencia_logradouro', refencia_logradouro
        ),
        'domicilio', JSON_OBJECT(
            -- 'id_especie_domicilio', id_especie_domicilio,
            -- 'id_material_piso_domicilio', id_material_piso_domicilio,
            -- 'id_material_domicilio', id_material_domicilio,
            'material_piso_domicilio', material_piso_domicilio,
            'material_domicilio', material_domicilio,
            'especie_domicilio', especie_domicilio,
            'dorme_rua', dorme_rua,
            'quantidade_comodos_domicilio', quantidade_comodos_domicilio,
            'quantidade_comodos_dormitorio', quantidade_comodos_dormitorio,
            'pessoas_domicilio', pessoas_domicilio,
            'familias_domicilio', familias_domicilio,
            'despesa_aluguel', despesa_aluguel,
            'nao_tem_despesa_aluguel', nao_tem_despesa_aluguel
        ),
        'infraestrutura_domiciliar', JSON_OBJECT(
            'possui_agua_encanada_domicilio', possui_agua_encanada_domicilio,
            'forma_abatecimento_agua_domicilio', forma_abatecimento_agua_domicilio,
            'possui_banheiro_domicilio', possui_banheiro_domicilio,
            'escoamento_sanitario_domicilio', escoamento_sanitario_domicilio,
            'destino_lixo_domicilio', destino_lixo_domicilio,
            'iluminacao_domicilio', iluminacao_domicilio,
            'calcamento_domicilio', calcamento_domicilio
            -- 'id_possui_agua_encanada_domicilio', id_possui_agua_encanada_domicilio,
            -- 'id_forma_abatecimento_agua_domicilio', id_forma_abatecimento_agua_domicilio,
            -- 'id_possui_banheiro_domicilio', id_possui_banheiro_domicilio,
            -- 'id_escoamento_sanitario_domicilio', id_escoamento_sanitario_domicilio,
            -- 'id_destino_lixo_domicilio', id_destino_lixo_domicilio,
            -- 'id_iluminacao_domicilio', id_iluminacao_domicilio,
            -- 'id_calcamento_domicilio', id_calcamento_domicilio
        ),
        'educacao', JSON_OBJECT(
            'frequenta_escola', frequenta_escola,
            'curso_frequenta', curso_frequenta,
            'curso_mais_elevado_frequentou', curso_mais_elevado_frequentou,
            'ano_serie_frequenta', ano_serie_frequenta
            -- 'id_curso_frequenta', id_curso_frequenta,
            -- 'id_curso_mais_elevado_frequentou', id_curso_mais_elevado_frequentou,
            -- 'id_ano_serie_frequenta', id_ano_serie_frequenta
        ),
        'contato', JSON_OBJECT(
            'contato_ddd', contato_ddd,
            'contato_telefone', contato_telefone,
            'contato_2_ddd', contato_2_ddd,
            'contato_2_telefone', contato_2_telefone,
            'contato_tipo', contato_tipo,
            'contato_2_tipo', contato_2_tipo,
            -- 'id_contato_tipo', id_contato_tipo,
            -- 'id_contato_2_tipo', id_contato_2_tipo,
            'email', email
        ),
        'saude', JSON_OBJECT(
            'estabelecimento_saude', estabelecimento_saude,
            'condicao_cadastro', condicao_cadastro,
            -- 'id_condicao_cadastro', id_condicao_cadastro,
            'cras_crea', cras_creas
        )
    ) AS data
FROM
    dados_api_cadunico;
