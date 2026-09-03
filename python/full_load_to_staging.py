import json
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, Json

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import canonical_payload, resolve_tables, build_row_key, norm_name, env

def op_conn():
    return psycopg2.connect(
        host=env("OP_HOST", "localhost"),
        port=int(env("OP_PORT", "25432")),
        dbname=env("OP_DB", "northwind"),
        user=env("OP_USER", "admin"),
        password=env("OP_PASSWORD", "admin123"),
    )

def stg_conn():
    return psycopg2.connect(
        host=env("STG_HOST", "localhost"),
        port=int(env("STG_PORT", "35432")),
        dbname=env("STG_DB", "northwind_staging"),
        user=env("STG_USER", "admin"),
        password=env("STG_PASSWORD", "admin123"),
    )

def main():
    with op_conn() as source, stg_conn() as target:
        with source.cursor() as c:
            table_map = resolve_tables(c)
        
        for canonical, actual in table_map.items():
            print(f"[FULL LOAD] {canonical} <- {actual}")
            with source.cursor(cursor_factory=RealDictCursor) as src:
                src.execute(f'SELECT * FROM "{actual}"')
                rows = src.fetchall()
            
            prepared = []
            for row in rows:
                payload = canonical_payload(dict(row))
                row_key = build_row_key(canonical, payload)
                prepared.append((canonical, row_key, Json(payload), False))
            
            with target.cursor() as dst:
                if prepared:
                    execute_values(
                        dst,
                        """
                        INSERT INTO current_rows(table_name, row_key, payload, is_deleted)
                        VALUES %s
                        ON CONFLICT(table_name, row_key)
                        DO UPDATE SET
                            payload = EXCLUDED.payload,
                            is_deleted = FALSE,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        prepared,
                    )
                    target.commit()
                    print(f"  Loaded {len(prepared)} row(s)")
                else:
                    print(f"  No rows found for {canonical}")

if __name__ == "__main__":
    main()
