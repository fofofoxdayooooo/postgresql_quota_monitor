-- -----------------------------------------------------------
-- PostgreSQL Real-time Storage Monitor Trigger Setup Script
-- -----------------------------------------------------------
-- This script sets up the necessary functions and triggers for
-- real-time storage monitoring using the LISTEN/NOTIFY mechanism.
--
-- Features:
-- - Dynamically syncs monitored users from 'user_limits.json'
--   using a Foreign Data Wrapper (FDW) to avoid manual updates.
-- - Automatically attaches DML triggers to new tables created by
--   monitored users, as well as on schema changes.
-- - Provides a block to manually apply triggers to all existing tables.
--
-- Usage:
-- 1. Connect to PostgreSQL as a superuser.
-- 2. Ensure 'file_fdw' extension is installed.
-- 3. Run this script once to set up the event triggers.
-- 4. Uncomment and run the final DO block to apply triggers to
--    existing tables.
-- -----------------------------------------------------------

-- 1. Install and configure the Foreign Data Wrapper (FDW)
-- This allows reading the JSON configuration file from within PostgreSQL.
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- Create an FDW server for JSON files
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'json_server') THEN
        CREATE SERVER json_server
        FOREIGN DATA WRAPPER file_fdw;
    END IF;
END
$$;

-- Create a foreign table that maps to the user_limits.json file.
-- Note: You may need to grant read permissions to the 'postgres' user
-- on the file system.
CREATE OR REPLACE FOREIGN TABLE user_limits_ft (
    json_data json
) SERVER json_server
OPTIONS (filename '/etc/postgres_monitor/user_limits.json');


-- 2. Function to send a notification to the daemon
-- This function is called by a trigger after a DML operation.
-- 'SECURITY DEFINER' ensures it runs with the privileges of the
-- function's creator (superuser), not the trigger's caller.
CREATE OR REPLACE FUNCTION storage_monitor_notify()
RETURNS TRIGGER AS $$
DECLARE
    current_user_name TEXT := current_user;
BEGIN
    PERFORM pg_notify('storage_limit_check', current_user_name);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- 3. Event trigger function to automatically apply DML triggers
-- This function is invoked by an event trigger after DDL commands.
CREATE OR REPLACE FUNCTION storage_monitor_event_trigger_function()
RETURNS event_trigger AS $$
DECLARE
    obj_record RECORD;
    monitored_user TEXT;
    trigger_name TEXT;
    table_name TEXT;
    monitored_users_array TEXT[];
BEGIN
    -- Dynamically get the list of monitored users from the FDW table
    SELECT array_agg(limits ->> 'user')
    INTO monitored_users_array
    FROM user_limits_ft, json_array_elements(json_data -> 'user_limits') AS limits;

    -- Iterate over all objects created or altered in the current transaction
    FOR obj_record IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        -- Check if the object is a table and its owner is in the monitored list
        IF obj_record.object_type = 'table' THEN
            SELECT rolname INTO monitored_user FROM pg_roles WHERE oid = obj_record.object_identity_oid;

            IF monitored_user = ANY(monitored_users_array) THEN
                table_name := obj_record.object_identity;
                trigger_name := 'trigger_monitor_' || REPLACE(monitored_user, '.', '_') || '_' || REPLACE(table_name, '.', '_');

                -- Create a DML trigger for INSERT, UPDATE, and DELETE
                EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH STATEMENT EXECUTE FUNCTION storage_monitor_notify();',
                               trigger_name, table_name);

                RAISE NOTICE 'Trigger % created for new table % owned by %', trigger_name, table_name, monitored_user;
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- 4. Create the event trigger for DDL commands
-- This trigger fires after DDL commands like CREATE TABLE.
CREATE EVENT TRIGGER storage_monitor_ddl_end
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'CREATE SCHEMA')
EXECUTE FUNCTION storage_monitor_event_trigger_function();


-- 5. DO block to manually apply triggers to existing tables
-- This block should be run once to set up monitoring on tables that
-- existed before the event trigger was created.
DO $$
DECLARE
    monitored_user TEXT;
    table_name TEXT;
    trigger_name TEXT;
    monitored_users_array TEXT[];
BEGIN
    -- Dynamically get the list of monitored users from the FDW table
    SELECT array_agg(limits ->> 'user')
    INTO monitored_users_array
    FROM user_limits_ft, json_array_elements(json_data -> 'user_limits') AS limits;

    -- Loop through each monitored user and their tables
    FOREACH monitored_user IN ARRAY monitored_users_array LOOP
        FOR table_name IN SELECT c.relname
                          FROM pg_class c
                          JOIN pg_namespace n ON c.relnamespace = n.oid
                          WHERE c.relowner = (SELECT oid FROM pg_roles WHERE rolname = monitored_user)
                          AND c.relkind = 'r' -- 'r' for regular table
        LOOP
            trigger_name := 'trigger_monitor_' || REPLACE(monitored_user, '.', '_') || '_' || REPLACE(table_name, '.', '_');
            EXECUTE format('CREATE TRIGGER %I
                            AFTER INSERT OR UPDATE OR DELETE ON %I.%I
                            FOR EACH STATEMENT EXECUTE FUNCTION storage_monitor_notify();',
                           trigger_name, monitored_user, table_name);

            RAISE NOTICE 'Trigger % created for existing table % owned by %', trigger_name, table_name, monitored_user;
        END LOOP;
    END LOOP;
END;
$$;
