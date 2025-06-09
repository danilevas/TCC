SELECT
    pid,
    datname,
    usename,
    application_name,
    client_addr,
    backend_start,
    state,
    query_start,
    query,
    wait_event_type,
    wait_event
FROM
    pg_stat_activity
WHERE
    datname = 'caronae_dw' -- Substitua pelo nome do seu DW
    -- AND usename = 'your_dw_username' -- Substitua pelo seu usuário
    -- AND application_name = 'psycopg2' -- Ou o nome que seu script está usando
    -- AND state = 'active'
ORDER BY
    query_start DESC;