-- ==============================================================================
-- Clear DuckLake PostgreSQL Metadata
--
-- This script completely clears out all DuckLake metadata and drops all user
-- tables in the PostgreSQL database between experiment runs.
--
-- Usage:
--   psql -h <host> -p <port> -U <user> -d <ducklake_db> -f clear_ducklake_metadata.sql
-- Example:
--   psql -h 127.0.0.1 -p 5432 -U postgres -d ducklakedb -f clear_ducklake_metadata.sql
-- ==============================================================================

-- 1. Drop all views in non-system schemas
DO $$
DECLARE
    v RECORD;
BEGIN
    FOR v IN (
        SELECT quote_ident(table_schema) || '.' || quote_ident(table_name) AS view_name
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ) LOOP
        EXECUTE 'DROP VIEW IF EXISTS ' || v.view_name || ' CASCADE';
    END LOOP;
END $$;

-- 2. Drop all tables across all user/metadata schemas
DO $$
DECLARE
    t RECORD;
BEGIN
    FOR t IN (
        SELECT quote_ident(schemaname) || '.' || quote_ident(tablename) AS table_name
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || t.table_name || ' CASCADE';
    END LOOP;
END $$;

-- 3. Drop all sequences in non-system schemas
DO $$
DECLARE
    sq RECORD;
BEGIN
    FOR sq IN (
        SELECT quote_ident(sequence_schema) || '.' || quote_ident(sequence_name) AS seq_name
        FROM information_schema.sequences
        WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
    ) LOOP
        EXECUTE 'DROP SEQUENCE IF EXISTS ' || sq.seq_name || ' CASCADE';
    END LOOP;
END $$;

-- 4. Drop and recreate the public schema to ensure a clean slate
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;

-- 5. Drop any custom/scoped DuckLake metadata schemas created during testing
DO $$
DECLARE
    s RECORD;
BEGIN
    FOR s IN (
        SELECT quote_ident(nspname) AS schema_name
        FROM pg_namespace
        WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
          AND nspname NOT LIKE 'pg_temp_%'
          AND nspname NOT LIKE 'pg_toast_temp_%'
    ) LOOP
        EXECUTE 'DROP SCHEMA IF EXISTS ' || s.schema_name || ' CASCADE';
    END LOOP;
END $$;
