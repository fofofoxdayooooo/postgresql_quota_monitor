-- -----------------------------------------------------------
-- PostgreSQL Real-time Storage Monitor Trigger Setup Script
-- (FDW dependency removed)
-- -----------------------------------------------------------

-- 1. Create a dedicated table for user limits
-- This table replaces the FDW approach. You can sync data from JSON
-- into this table using COPY or an external script.
CREATE TABLE IF NOT EXISTS user_limits (
    username TEXT PRIMARY KEY,
    soft_limit BIGINT,
    hard_limit BIGINT,
    hard_grace_period INT,
    unlock_grace_period INT,
    manual_unlock BOOLEAN
);

-- Example of loading JSON externally:
-- COPY user_limits FROM '/etc/postgres_monitor/user_limits.csv' WITH CSV HEADER;

-- 2. Function to send a notification to the daemon
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
CREATE OR REPLACE FUNCTION storage_monitor_event_trigger_function()
RETURNS event_trigger AS $$
DECLARE
    obj_record RECORD;
    monitored_user TEXT;
    trigger_name TEXT;
    table_name TEXT;
    monitored_users_array TEXT[];
BEGIN
    -- Get the monitored users from the local table
    SELECT array_agg(username)
    INTO monitored_users_array
    FROM user_limits;

    FOR obj_record IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj_record.object_type = 'table' THEN
            SELECT rolname INTO monitored_user
            FROM pg_roles WHERE oid = obj_record.object_identity_oid;

            IF monitored_user = ANY(monitored_users_array) THEN
                table_name := obj_record.object_identity;
                trigger_name := 'trigger_monitor_' ||
                                REPLACE(monitored_user, '.', '_') || '_' ||
                                REPLACE(table_name, '.', '_');

                EXECUTE format('CREATE TRIGGER %I
                                AFTER INSERT OR UPDATE OR DELETE
                                ON %s
                                FOR EACH STATEMENT
                                EXECUTE FUNCTION storage_monitor_notify();',
                               trigger_name, table_name);

                RAISE NOTICE 'Trigger % created for new table % owned by %',
                             trigger_name, table_name, monitored_user;
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 4. Create the event trigger for DDL commands
CREATE EVENT TRIGGER storage_monitor_ddl_end
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'CREATE SCHEMA')
EXECUTE FUNCTION storage_monitor_event_trigger_function();

-- 5. Manually apply triggers to existing tables
DO $$
DECLARE
    monitored_user TEXT;
    table_name TEXT;
    trigger_name TEXT;
    monitored_users_array TEXT[];
BEGIN
    SELECT array_agg(username)
    INTO monitored_users_array
    FROM user_limits;

    FOREACH monitored_user IN ARRAY monitored_users_array LOOP
        FOR table_name IN
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relowner = (SELECT oid FROM pg_roles WHERE rolname = monitored_user)
            AND c.relkind = 'r'
        LOOP
            trigger_name := 'trigger_monitor_' ||
                            REPLACE(monitored_user, '.', '_') || '_' ||
                            REPLACE(table_name, '.', '_');
            EXECUTE format('CREATE TRIGGER %I
                            AFTER INSERT OR UPDATE OR DELETE
                            ON %I.%I
                            FOR EACH STATEMENT
                            EXECUTE FUNCTION storage_monitor_notify();',
                           trigger_name, monitored_user, table_name);

            RAISE NOTICE 'Trigger % created for existing table % owned by %',
                         trigger_name, table_name, monitored_user;
        END LOOP;
    END LOOP;
END;
$$;