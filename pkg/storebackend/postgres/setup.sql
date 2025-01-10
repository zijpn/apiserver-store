CREATE OR REPLACE PROCEDURE check_and_create_schema(schema_name TEXT, OUT schema_created BOOLEAN)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if the schema exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.schemata WHERE schema_name = schema_name
    ) THEN
        -- Create the schema if it doesn't exist
        EXECUTE format('CREATE SCHEMA %I', schema_name);
        schema_created := TRUE;  -- Schema was created
    ELSE
        schema_created := FALSE;  -- Schema already exists
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE create_table_in_schema(schema_name TEXT, table_name TEXT, OUT table_created BOOLEAN)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = schema_name
        AND table_name = table_name
    ) THEN
        -- Construct the SQL statement to create the table
        EXECUTE format('CREATE TABLE %I.%I (
            namespace TEXT,
            name TEXT NOT NULL,
            data BYTEA
            PRIMARY KEY (namespace, name)
        )', schema_name, table_name);

        -- Construct the SQL statement to create the index on namespace and name
        EXECUTE format('CREATE INDEX idx_%I_%I_namespace_name ON %I.%I (namespace, name)',
            schema_name, table_name, schema_name, table_name);

        table_created := TRUE;  -- Table was created
    ELSE
        table_created := TRUE;  -- Table already exists, just return true
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE retrieve_data(schema_name TEXT, table_name TEXT, ns TEXT, name TEXT, OUT data BYTEA)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Construct the SQL statement to retrieve the data
    EXECUTE format('SELECT data FROM %I.%I WHERE namespace = $1 AND name = $2', schema_name, table_name)
    INTO data
    USING ns, name;

    -- Handle case where no data is found
    IF data IS NULL THEN
        RAISE NOTICE 'No data found for namespace % and name %', ns, name;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE retrieve_data_list(schema_name TEXT, table_name TEXT, OUT result_set SETOF RECORD)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Construct the SQL statement to retrieve the data list
    RETURN QUERY EXECUTE format('SELECT namespace, name, data FROM %I.%I', schema_name, table_name)
END;
$$;

CREATE OR REPLACE PROCEDURE insert_entry(schema_name TEXT, table_name TEXT, p_namespace TEXT, p_name TEXT, p_data BYTEA)
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        EXECUTE format('INSERT INTO %I.%I (namespace, name, data) VALUES ($1, $2, $3)', schema_name, table_name)
        USING p_namespace, p_name, p_data;
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION 'Unique key violation: An entry with namespace=% and resource=% already exists.', p_namespace, p_name
            USING ERRCODE = 'U0001'; -- Custom error code for unique key violation
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error occurred: %', SQLERRM
            USING ERRCODE = 'P0001'; -- Custom error code for other errors
    END;
END;
$$;


CREATE OR REPLACE PROCEDURE update_entry(schema_name TEXT, table_name TEXT, p_namespace TEXT, p_name TEXT, p_data BYTEA)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('INSERT INTO %I.%I (namespace, name, data)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (namespace, name)
                    DO UPDATE SET data = EXCLUDED.data', schema_name, table_name)
    USING p_namespace, p_name, p_data;
END;
$$;

CREATE OR REPLACE PROCEDURE delete_entry(schema_name TEXT, table_name TEXT, p_namespace TEXT, p_name TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('DELETE FROM %I.%I WHERE namespace = $1 AND name = $2', schema_name, table_name)
    USING p_namespace, p_name;
END;
$$;










