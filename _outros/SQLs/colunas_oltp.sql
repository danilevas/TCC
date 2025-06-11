SELECT table_name, ordinal_position, column_name, column_default, is_nullable, data_type, udt_name
FROM information_schema.columns
WHERE table_schema = 'public'
AND table_name = 'neighborhoods'
OR table_name = 'zones'
ORDER BY table_name, ordinal_position;