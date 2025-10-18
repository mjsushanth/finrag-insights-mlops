-- ============================================================================
-- Author: Joel Markapudi
-- Date: October, 2025
-- ============================================================================


SELECT version();
PRAGMA version;

SELECT 1 + 1;


-- show schemas/tables via information_schema
SELECT table_schema, table_name
FROM information_schema.tables
ORDER BY 1,2
LIMIT 20;
