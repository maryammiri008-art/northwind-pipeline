CREATE TABLE IF NOT EXISTS cdc_events (
    event_id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS current_rows (
    table_name TEXT NOT NULL,
    row_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, row_key)
);
CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    last_processed_event BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO pipeline_state (pipeline_name, last_processed_event) 
VALUES ('clickhouse_incremental', 0) 
ON CONFLICT (pipeline_name) DO NOTHING;
