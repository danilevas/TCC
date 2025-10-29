-- USUÁRIOS ATIVOS
WITH totais AS (
    SELECT 
        academic_affiliation,
        COUNT(*) AS usuarios_totais
    FROM dim_user
    GROUP BY academic_affiliation
),
usuarios_ativos AS (
    -- Usuários que fizeram pedidos (passageiros)
    SELECT DISTINCT frr.user_sk
    FROM fact_ride_request frr
    
    UNION
    
    -- Usuários que ofereceram caronas (motoristas)
    SELECT DISTINCT fr.driver_user_sk
    FROM fact_ride fr
)
SELECT 
    du.academic_affiliation, 
    COUNT(DISTINCT ua.user_sk) AS usuarios_ativos,
    t.usuarios_totais,
    ROUND(COUNT(DISTINCT ua.user_sk) * 100.0 / t.usuarios_totais, 2) AS percentual_ativos
FROM dim_user du
LEFT JOIN usuarios_ativos ua ON du.user_sk = ua.user_sk
JOIN totais t ON du.academic_affiliation = t.academic_affiliation
GROUP BY du.academic_affiliation, t.usuarios_totais
ORDER BY percentual_ativos DESC;


-- USUÁRIOS IMPACTADOS
WITH totais_por_perfil AS (
    -- Total de usuários cadastrados por perfil
    SELECT 
        academic_affiliation,
        COUNT(*) AS usuarios_totais
    FROM dim_user
    GROUP BY academic_affiliation
),
usuarios_impactados AS (
    -- Usuários que foram passageiros com pedidos aceitos em caronas concluídas
    SELECT DISTINCT frr.user_sk
    FROM fact_ride_request frr
    JOIN fact_ride fr ON frr.ride_sk = fr.ride_sk
    JOIN dim_ride_flags drf ON fr.ride_flags_sk = drf.ride_flags_sk
    JOIN dim_request_status drs ON frr.status_sk = drs.status_sk
    WHERE drf.done = TRUE
    AND drs.status_name = 'accepted'
    
    UNION
    
    -- Usuários que foram motoristas em caronas concluídas COM passageiros aceitos
    SELECT DISTINCT fr.driver_user_sk
    FROM fact_ride fr
    JOIN dim_ride_flags drf ON fr.ride_flags_sk = drf.ride_flags_sk
    WHERE drf.done = TRUE
    AND fr.accepted_requests_count > 0
)
SELECT 
    du.academic_affiliation,
    COUNT(DISTINCT ui.user_sk) AS usuarios_impactados,
    t.usuarios_totais,
    ROUND(COUNT(DISTINCT ui.user_sk) * 100.0 / t.usuarios_totais, 2) AS percentual
FROM dim_user du
LEFT JOIN usuarios_impactados ui ON du.user_sk = ui.user_sk
JOIN totais_por_perfil t ON du.academic_affiliation = t.academic_affiliation
GROUP BY du.academic_affiliation, t.usuarios_totais
ORDER BY usuarios_impactados DESC;

-- PASSAGEIROS IMPACTADOS (SEM PERFIL)
-- Usuários que foram passageiros com pedidos aceitos em caronas concluídas
SELECT COUNT (DISTINCT frr.user_sk) AS passageiros_impactados
FROM fact_ride_request frr
JOIN fact_ride fr ON frr.ride_sk = fr.ride_sk
JOIN dim_ride_flags drf ON fr.ride_flags_sk = drf.ride_flags_sk
JOIN dim_request_status drs ON frr.status_sk = drs.status_sk
WHERE drf.done = TRUE
AND drs.status_name = 'accepted'
ORDER BY passageiros_impactados DESC

-- PASSAGEIROS IMPACTADOS (COM PERFIL)
-- Usuários que foram passageiros com pedidos aceitos em caronas concluídas
SELECT du.academic_affiliation, COUNT (DISTINCT frr.user_sk) AS passageiros_impactados
FROM fact_ride_request frr
JOIN fact_ride fr ON frr.ride_sk = fr.ride_sk
JOIN dim_ride_flags drf ON fr.ride_flags_sk = drf.ride_flags_sk
JOIN dim_request_status drs ON frr.status_sk = drs.status_sk
JOIN dim_user du ON frr.user_sk = du.user_sk
WHERE drf.done = TRUE
AND drs.status_name = 'accepted'
GROUP BY du.academic_affiliation
ORDER BY passageiros_impactados DESC