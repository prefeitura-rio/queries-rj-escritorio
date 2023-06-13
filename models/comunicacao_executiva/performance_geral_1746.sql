WITH chamados_tratada AS (
WITH chamado_basico AS (
    SELECT 
        ch.* except(data_fim, data_inicio),
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
        data_inicio data_inicio,
        date(date_trunc(date_add(data_inicio, interval 1 month), month) - interval 1 day) data_inicio_mes,
        
        -- Data Alvo 
        date(date_trunc(date_add(
            case 
                WHEN ch.id_tipo = '280' and ch.id_subtipo = '1319'
                THEN date_add(data_inicio, interval 240 day)
                ELSE data_alvo_finalizacao
            END
        , interval 1 month), month) - interval 1 day) data_alvo_mes,
        date(case 
            WHEN ch.id_tipo = '280' and ch.id_subtipo = '1319'
            THEN date_trunc(date_add(data_inicio, interval 240 day), WEEK(MONDAY))
            ELSE date_trunc(data_alvo_finalizacao, WEEK(MONDAY))
        END) data_alvo_semana,
        case 
            WHEN ch.id_tipo = '280' and ch.id_subtipo = '1319'
            THEN date_add(data_inicio, interval 240 day)
            ELSE data_alvo_finalizacao
        END data_alvo,
        
        -- Data Fim
        data_fim data_fim,
        date(date_trunc(date_add(data_fim, interval 1 month), month) - interval 1 day) data_fim_mes,
        date(date_trunc(data_fim, WEEK(MONDAY))) data_fim_semana,
        
        -- Tempo Atendimento
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
        -- Encerrados
        CASE
          WHEN data_fim IS NOT NULL 
          THEN 1
          ELSE 0
        END AS encerrado, 
        
        -- Encerrado Dentro do Prazo
        CASE
          WHEN data_fim IS NOT NULL 
            AND data_fim <= data_alvo 
          THEN 1
          ELSE 0
        END AS edp,
        
        -- Encerrado Fora do Prazo
        -- Definição: Chamados encerrados depois da data alvo e antes do final do mês
        CASE
          WHEN data_fim IS NOT NULL 
            AND data_fim > data_alvo
            and data_fim <= data_alvo_mes
          THEN 1
          ELSE 0 
        END AS efp,
        
        -- Encerrado do Estoque
        -- Definição: Chamados encerrados depois da data alvo e depois do final do mês da data alvo
        CASE
          WHEN data_fim IS NOT NULL 
            AND data_fim > data_alvo
            and data_fim_mes > data_alvo_mes
          THEN 1
          ELSE 0 
        END AS efpe,
        
        -- Encerrado Fora do Prazo Semana
        -- Definição: Chamados encerrados depois da data alvo e antes do final da semana
        CASE
            WHEN data_fim IS NOT NULL 
              AND data_fim > data_alvo
              AND date_trunc(data_alvo, WEEK(MONDAY)) = date_trunc(data_fim, WEEK(MONDAY))
              AND EXTRACT(YEAR FROM data_alvo_mes) = EXTRACT(YEAR FROM data_fim)
            THEN 1
            ELSE 0
        END AS efp_week,
        
        -- Aberto Fora do Prazo
        CASE
          WHEN data_fim IS NULL  
            AND CURRENT_DATE() > data_alvo 
          THEN 1
          ELSE 0
        END AS afp,
        
        -- Aberto Dentro do Prazo
        CASE
          WHEN data_fim IS NULL 
            AND CURRENT_DATE() <= data_alvo 
          THEN 1
          ELSE 0 
        END AS adp,
        
        -- Data Entrada Estoque
        CASE
          WHEN 
            (data_fim IS NOT NULL and data_fim_mes > data_alvo_mes) or (data_fim IS NULL and current_date() > data_alvo_mes)
          THEN DATE(DATE_TRUNC(DATE_ADD(data_alvo_mes, INTERVAL 2 MONTH), MONTH) - INTERVAL 1 DAY) 
          ELSE null
        END AS data_entrada_estoque,
                
        -- Estoque 2023
        CASE 
          WHEN (
          data_fim is null
          AND CURRENT_DATE() > data_alvo)
          and data_inicio >= '2023-01-01'
          then 1
          else 0
        end estoque_2023,
        
        -- Estoque 2021
        CASE 
          WHEN (
          data_fim is null
          AND CURRENT_DATE() > data_alvo)
          and data_inicio >= '2021-01-01'
          then 1
          else 0
        end estoque_2021
        
    FROM chamado_basico
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
with gp_alvo_mes as (
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
        sum(case when data_inicio >= '2023-01-01' then 1 else 0 end) tc_2023,
        sum(edp) edp,
        sum(afp) afp,
        sum(adp) adp,
        sum(efp) efp,
        sum(tempo_atendimento_horas) tempo_atendimento_horas,
        sum(tempo_atendimento_estimado_horas) tempo_atendimento_estimado_horas
    from chamados_tratada
    where data_alvo_mes <= DATE(DATE_TRUNC(DATE_ADD(current_date(), INTERVAL 1 MONTH), MONTH) - INTERVAL 1 DAY)
    group by data_alvo_mes, bairro, is_critico,id_servico, servico, tipo, subtipo, subprefeitura, id_unidade_organizacional_mae
),

gp_fim_mes as (
    select
        data_fim_mes ano_mes,
        bairro,
        is_critico,
        id_servico,
        servico,
        tipo,
        subtipo,
        subprefeitura,
        id_unidade_organizacional_mae,
        sum(encerrado) encerrado,
        sum(case when data_inicio >= '2023-01-01' then encerrado else 0 end) encerrado_2023,
        sum(efpe) efpe
    from chamados_tratada
    where data_fim_mes <= DATE(DATE_TRUNC(DATE_ADD(current_date(), INTERVAL 1 MONTH), MONTH) - INTERVAL 1 DAY)
    group by data_fim_mes, bairro, is_critico,id_servico, servico, tipo, subtipo, subprefeitura, id_unidade_organizacional_mae
),


date_range AS (
    SELECT 
        date AS month_start
    FROM 
    UNNEST(GENERATE_DATE_ARRAY('2021-01-01', current_date(), INTERVAL 1 DAY)) AS date
),

months as (
    SELECT 
      date(date_trunc(date_add(month_start, interval 1 month), month) - interval 1 day) as month
    FROM 
      date_range
    GROUP BY
      month
),

gp_estoque as (
    select 
        month ano_mes,
        bairro, 
        is_critico,
        id_servico, 
        servico, 
        tipo, 
        subtipo, 
        subprefeitura, 
        id_unidade_organizacional_mae,
        count(*) estoque_total,
        sum(case when data_inicio between '2021-01-01' and '2021-12-31' then 1 else 0 end) estoque_2021,
        sum(case when data_inicio between '2022-01-01' and '2022-12-31' then 1 else 0 end) estoque_2022,
        sum(case when data_inicio between '2023-01-01' and '2023-12-31' then 1 else 0 end) estoque_2023
    from chamados_tratada t
    CROSS JOIN months
    where (month >= data_entrada_estoque and month < data_fim_mes)
        or (month >= data_entrada_estoque and data_fim_mes is null)
    group by month, bairro, is_critico,id_servico, servico, tipo, subtipo, subprefeitura, id_unidade_organizacional_mae
),

gp_final AS (
    select  
        a.ano_mes,
        a.bairro,
        a.is_critico,
        a.id_servico,
        a.servico,
        a.tipo,
        a.subtipo,
        a.subprefeitura,
        a.id_unidade_organizacional_mae,
        tc, 
        edp,
        afp,
        adp,
        efp,
        encerrado,
        efpe,
        estoque_total,
        estoque_2021,
        estoque_2022,
        estoque_2023,
        tc - encerrado diff,
        tc_2023 - encerrado_2023 diff_2023,
        tempo_atendimento_horas,
        tempo_atendimento_estimado_horas
    from gp_alvo_mes a
     full outer join  gp_fim_mes f
        on  f.ano_mes                       = a.ano_mes and
            f.bairro                        = a.bairro and
            f.is_critico                    = a.is_critico and
            f.id_servico                    = a.id_servico and
            f.servico                       = a.servico and
            f.tipo                          = a.tipo and
            f.subtipo                       = a.subtipo and
            f.subprefeitura                 = a.subprefeitura and
            f.id_unidade_organizacional_mae = a.id_unidade_organizacional_mae
      full outer join gp_estoque e
        on  e.ano_mes                       = a.ano_mes and
            e.bairro                        = a.bairro and
            e.is_critico                    = a.is_critico and
            e.id_servico                    = a.id_servico and
            e.servico                       = a.servico and
            e.tipo                          = a.tipo and
            e.subtipo                       = a.subtipo and
            e.subprefeitura                 = a.subprefeitura and
            e.id_unidade_organizacional_mae = a.id_unidade_organizacional_mae
)

SELECT * FROM gp_final
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
LIMIT 3 -- Apenas os 2 ultimos meses