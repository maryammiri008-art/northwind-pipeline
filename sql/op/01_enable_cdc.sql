CREATE TABLE IF NOT EXISTS cdc_event_log (
    event_id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS etl_control (
    pipeline_name TEXT PRIMARY KEY,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO etl_control (pipeline_name, last_event_id) 
VALUES ('northwind_kafka', 0) 
ON CONFLICT (pipeline_name) DO NOTHING;
CREATE OR REPLACE FUNCTION fn_capture_cdc() 
RETURNS TRIGGER AS $$ 
DECLARE 
    row_data JSONB;
BEGIN 
    IF TG_OP = 'DELETE' THEN 
        row_data := to_jsonb(OLD);
    ELSE 
        row_data := to_jsonb(NEW);
    END IF;
    
    INSERT INTO cdc_event_log(table_name, operation, payload) 
    VALUES (TG_TABLE_NAME, TG_OP, row_data);
    
    IF TG_OP = 'DELETE' THEN 
        RETURN OLD;
    END IF;
    RETURN NEW;
END; 
$$ LANGUAGE plpgsql;
DO $$ 
DECLARE 
    r RECORD;
    trigger_name TEXT;
BEGIN 
    FOR r IN 
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND lower(table_name) IN (
            'customers', 'employees', 'suppliers', 'products',
            'categories', 'orders', 'order_details', 'shippers'
        )
    LOOP 
        trigger_name := 'trg_cdc_' || replace(lower(r.table_name), ' ', '');
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I', trigger_name, r.table_schema, r.table_name);
        EXECUTE format(
            'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
            'FOR EACH ROW EXECUTE FUNCTION fn_capture_cdc()',
            trigger_name, r.table_schema, r.table_name
        );
    END LOOP;
END $$;
