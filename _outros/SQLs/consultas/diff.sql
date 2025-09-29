-- Enable extension (once per DB)
CREATE EXTENSION IF NOT EXISTS dblink;

-- Linhas só na rides 2019-10
-- SELECT *
-- FROM rides r1
-- LEFT JOIN dblink(
--     'dbname=caronae_oltp_2020 user=postgres password=mcpostgresnosanos80 host=localhost',
--     'SELECT id FROM rides'
-- ) AS r2(id INT)
-- USING (id)
-- WHERE r2.id IS NULL;

-- Linhas só na rides 2020-03
SELECT r2.id, r2.created_at, r2.description
FROM dblink(
    'dbname=caronae_oltp_2020 user=postgres password=mcpostgresnosanos80 host=localhost',
    'SELECT id, created_at, description FROM rides'
) AS r2(id INT, created_at DATE, description TEXT)
LEFT JOIN rides r1
USING (id)
WHERE r1.id IS NULL;
