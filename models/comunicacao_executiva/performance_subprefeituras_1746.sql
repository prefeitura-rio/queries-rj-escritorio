    -- Ranking -> ORDER BY EDP CRES
-- Nome [+ órgão se serviços] 
-- Variação no ranking (comparado a mes anterior)
-- Total de Chamados -> TC
-- Estoque desde jan/2021 [Estoque]
-- % Encerrado em Geral [(EFP + EDP) / TC] > 100%
-- % Encerrado Dentro do Prazo [EDP / TC] 
-- Ultimos 5 meses de EDP/TC

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
        subprefeitura,
        SUM(tc) tc,
        SUM(edp) edp,
        SUM(afp) afp ,
        SUM(efp) efp,
    FROM chamados_historico
    where is_critico
    GROUP BY 1, 2
),
-- !!! LEMBRE-SE DE FAZER OS FILTROS CORRETOS QUANDO TROCAR OS FILTROS DO chamados_agg
estoque as (
    select subprefeitura, sum(afp + adp) estoque
    from chamados_tratada
    where subprefeitura is not null
    and data_alvo <= DATE_SUB(DATE_TRUNC(DATE(CURRENT_DATE()), MONTH), INTERVAL 1 DAY)
    and is_critico
    group by subprefeitura
),
taxas as (
    SELECT 
        ano_mes,
        subprefeitura,
        tc,
        edp,
        edp / tc tx_edp,
        afp,
        afp / tc tx_afp,
        efp,
        efp / tc tx_efp,
        edp + efp encerrados,
        (edp + efp) / tc tx_encerrados
    FROM chamados_agg),
ranking as (
    select 
        ano_mes,
        subprefeitura,
        - ranking + lag(ranking) over (partition by subprefeitura order by ano_mes) variacao_ranking
    from (
        select
            ano_mes,
            row_number() over(partition by ano_mes order by tx_edp desc) ranking,
            subprefeitura,
            tx_edp
        from taxas
        where subprefeitura is not null)
    order by ano_mes
),
classificao as (
    select subprefeitura, array_agg(classificao) classificao
    from (
        select 
            ano_mes,
            subprefeitura,
            tx_edp,
            case
                when tx_edp >= 0.95 then 'Excelente'
                when tx_edp between 0.7 and 0.95 then 'Bom'
                when tx_edp between 0.5 and 0.7 then 'Médio'
                else 'Ruim'
            end classificao
        from taxas
        where ano_mes between date_trunc(date_sub(DATE(CURRENT_DATE()), interval 6 month), month) and date_trunc(date_sub(DATE(CURRENT_DATE()), interval 1 month), month)
        order by ano_mes
    )
    group by subprefeitura 
)
select  
    t.ano_mes,
    row_number() over(order by tx_edp desc) ranking,
    variacao_ranking,
    CASE 
        WHEN variacao_ranking > 0 THEN CONCAT("↑ +", variacao_ranking)
        WHEN variacao_ranking < 0 THEN CONCAT("↓ -", variacao_ranking * -1)
        ELSE CONCAT("   ", variacao_ranking)
    END variacao_ranking_formatada,
    t.subprefeitura,
    tc,
    estoque,
    tx_encerrados,
    tx_edp,
    classificao,
    edp,
    afp,
    tx_afp,
    efp,
    tx_efp,
    encerrados,
from taxas t
join ranking r
on t.ano_mes = r.ano_mes and t.subprefeitura = r.subprefeitura
join estoque e
on t.subprefeitura = e.subprefeitura
join classificao c
on t.subprefeitura = c.subprefeitura
where t.ano_mes = date_trunc(date_sub(DATE(CURRENT_DATE()), interval 1 month), month)
order by tx_edp desc

