import sys
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import clickhouse_connect

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import env

def stg_conn():
    return psycopg2.connect(
        host=env("STG_HOST", "localhost"),
        port=int(env("STG_PORT", "35432")),
        dbname=env("STG_DB", "northwind_staging"),
        user=env("STG_USER", "admin"),
        password=env("STG_PASSWORD", "admin123"),
    )

def ch_client():
    return clickhouse_connect.get_client(
        host=env("CLICKHOUSE_HOST", "localhost"),
        port=int(env("CLICKHOUSE_HTTP_PORT", "28123")),
        username=env("CLICKHOUSE_USER", "admin"),
        password=env("CLICKHOUSE_PASSWORD", "admin123"),
        database=env("CLICKHOUSE_DB", "northwind_dw"),
    )

def fetch_rows(conn, table):
    sql = f'SELECT * FROM "{table}"'
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]

def insert_rows(client, ch_table, rows):
    if not rows:
        print(f"[CLICKHOUSE] {ch_table}: no rows to insert")
        return 0
    now = datetime.now().replace(microsecond=0)
    columns = list(rows[0].keys()) + ["version", "is_deleted"]
    data = []
    for row in rows:
        data.append([row.get(c) for c in rows[0].keys()] + [now, 0])
    client.insert(ch_table, data, column_names=columns)
    print(f"[CLICKHOUSE] {ch_table}: inserted {len(rows)} row(s)")
    return len(rows)

def main():
    conn = stg_conn()
    client = ch_client()
    
    tables = [
        ("customers", "dim_customers", ["customer_id", "company_name", "contact_name", "contact_title", "phone", "fax"]),
        ("employees", "dim_employees", ["employee_id", "first_name", "last_name", "title"]),
        ("suppliers", "dim_suppliers", ["supplier_id", "company_name", "contact_name", "contact_title", "phone", "fax"]),
        ("products", "dim_products", ["product_id", "product_name", "unit_price"]),
        ("categories", "dim_categories", ["category_id", "category_name"]),
        ("shippers", "dim_shippers", ["shipper_id", "company_name", "phone"]),
    ]
    
    for table, ch_table, columns in tables:
        rows = fetch_rows(conn, table)
        if rows:
            simplified = []
            for row in rows:
                new_row = {}
                for col in columns:
                    new_row[col] = row.get(col)
                simplified.append(new_row)
            insert_rows(client, ch_table, simplified)
    
    print("[SUCCESS] Data loaded to ClickHouse successfully!")
    conn.close()

if __name__ == "__main__":
    main()
