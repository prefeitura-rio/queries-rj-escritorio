WITH chamados_tratada AS (
    WITH chamado_basico AS (
        SELECT 
            ch.*,
        CASE 
            WHEN ns.id_servico_new IS NOT NULL
            THEN ns.id_servico_new
            ELSE ns.id_servico
        END id_servico,
            concat(split(id_unidade_organizacional_mae, ' ')[offset(0)], ' - ', ns.servico) servico,
            ns.natureza,
            ns.is_critico,
            b.subprefeitura,
            b.nome AS bairro,
            case 
                WHEN ch.id_tipo = '280' and ch.id_subtipo = '1319'
                THEN date_trunc(data_inicio, MONTH)
                ELSE date_trunc(data_alvo_finalizacao, MONTH)
            END data_alvo_mes,
            date_trunc(data_fim, MONTH) data_fim_mes,
            date_trunc(data_alvo_finalizacao, WEEK(MONDAY)) data_alvo_semana,
            date_trunc(data_fim, WEEK(MONDAY)) data_fim_semana,
            case 
                WHEN ch.id_tipo = '280' and ch.id_subtipo = '1319'
                THEN date_trunc(data_inicio, MONTH)
                ELSE data_alvo_finalizacao
            END data_alvo,
            CASE 
                WHEN data_fim IS NULL
                THEN DATE_DIFF(CURRENT_DATETIME(),data_inicio, HOUR) 
                ELSE DATE_DIFF(data_fim,data_inicio, HOUR) 
            END AS tempo_atendimento_horas,
            DATE_DIFF(data_alvo_finalizacao, data_inicio, HOUR) AS tempo_atendimento_estimado_horas,
    FROM `rj-segovi.adm_central_atendimento_1746.chamado` ch
    LEFT OUTER JOIN `rj-segovi.adm_central_atendimento_1746.servico` ns
        ON ns.id_servico = CONCAT(ch.id_tipo,"_", ch.id_subtipo) 
    LEFT OUTER JOIN `rj-escritorio-dev.dados_mestres.bairro` b
        ON b.id_bairro = ch.id_bairro
    WHERE data_particao >= "2021-01-01"
    and data_inicio >= "2021-01-01"
    ),
    chamado_prazos as (
    SELECT *,
        CASE
            WHEN 
                data_fim IS NOT NULL AND
                data_fim <= data_alvo 
            THEN 'EDP'
            WHEN data_fim IS NOT NULL 
            AND data_fim > data_alvo 
            THEN 'EFP'
            WHEN  data_fim IS NULL 
            AND CURRENT_DATE() > data_alvo
            THEN 'AFP'
            WHEN data_fim IS NULL
            AND CURRENT_DATE() <= data_alvo 
            THEN 'ADP'
            ELSE NULL
        END AS situacao_atendimento,
        CASE
            WHEN data_fim IS NOT NULL 
            AND data_fim <= data_alvo 
            THEN 1
            ELSE 0
        END AS edp,
        CASE
            WHEN data_fim IS NOT NULL 
            AND data_fim > data_alvo
            and EXTRACT(MONTH FROM data_alvo_mes) = EXTRACT(MONTH FROM data_fim) 
            and EXTRACT(YEAR FROM data_alvo_mes) = EXTRACT(YEAR FROM data_fim)
            THEN 1
            ELSE 0 
        END AS efp,
        CASE
            WHEN data_fim IS NOT NULL 
                AND data_fim > data_alvo
                AND date_trunc(data_alvo, WEEK(MONDAY)) = date_trunc(data_fim, WEEK(MONDAY))
                AND EXTRACT(YEAR FROM data_alvo_mes) = EXTRACT(YEAR FROM data_fim)
            THEN 1
            ELSE 0
        END AS efp_week,
        CASE
            WHEN data_fim IS NOT NULL 
            AND data_fim > data_alvo
            and data_fim_mes > data_alvo_mes
            THEN 1
            ELSE 0 
        END AS efpe,
        CASE
            WHEN data_fim IS NULL  
            OR data_fim_mes > data_alvo_mes
            THEN 1
            ELSE 0
        END AS afp,
        CASE
            WHEN data_fim IS NULL 
            AND CURRENT_DATE() <= data_alvo 
            THEN 1
            ELSE 0 
        END AS adp
    FROM
        chamado_basico
    ),

    final_tb as (
        SELECT 
        *,
        FROM chamado_prazos
        WHERE date_trunc(date(data_alvo), MONTH) != "2021-07-01"
    )

    SELECT * FROM final_tb
),

chamados_historico as (
    select
        data_alvo_mes ano_mes,
        bairro,
        is_critico,
        id_servico,
        servico,
        tipo,
        subtipo,
        subprefeitura,
        id_unidade_organizacional_mae,
        count(*) tc, 
        sum(edp) edp,
        sum(afp) afp,
        sum(adp) adp,
        sum(efp) efp,
        sum(tempo_atendimento_horas) tempo_atendimento_horas,
        sum(tempo_atendimento_estimado_horas) tempo_atendimento_estimado_horas
    from chamados_tratada
    where data_alvo_mes < date_sub(current_date(), interval 1 month)
    group by data_alvo_mes, bairro, is_critico,id_servico, servico, tipo, subtipo, subprefeitura, id_unidade_organizacional_mae
),

chamados_agg AS (
    SELECT 
        ano_mes,
        is_critico,
        SUM(tc) tc,
        SUM(edp) edp,
        SUM(afp) afp ,
        SUM(efp) efp,
    FROM chamados_historico
    GROUP BY 1, 2
)

SELECT 
    ano_mes,
    is_critico,
    tc,
    tc - (LAG(tc,1) OVER (ORDER BY ano_mes)) variacao_total_chamados,
    edp / tc - (LAG(edp,1) OVER (ORDER BY ano_mes)/(LAG(tc,1) OVER (ORDER BY ano_mes))) variacao_taxa_edp,
    edp,
    edp / tc tx_edp,
    afp,
    afp / tc tx_afp,
    efp,
    efp / tc tx_efp,
    edp + efp encerrados,
    (edp + efp) / tc tx_encerrados
FROM chamados_agg
WHERE is_critico
ORDER BY ano_mes DESC
LIMIT 2 -- Apenas os 2 ultimos meses