import argparse
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import clickhouse_connect
import psycopg2
from psycopg2.extras import RealDictCursor

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import canonical_payload, norm_name, env

READY_TO_CH = {
    "ready_dim_geography": "dim_geography",
    "ready_dim_suppliers": "dim_suppliers",
    "ready_dim_products": "dim_products",
    "ready_dim_customers": "dim_customers",
    "ready_dim_employees": "dim_employees",
    "ready_dim_shippers": "dim_shippers",
    "ready_dim_ship_name": "dim_ship_name",
    "ready_dim_date": "dim_date",
    "ready_fact_orders": "fact_orders",
}

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

def fetch_rows(conn, table, where_sql=None, params=None):
    sql = f'SELECT * FROM "{table}"'
    if where_sql:
        sql += " WHERE " + where_sql
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or ())
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

def full_load(conn, client):
    for ready_table, ch_table in READY_TO_CH.items():
        rows = fetch_rows(conn, ready_table)
        insert_rows(client, ch_table, rows)
    
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(event_id), 0) FROM cdc_events")
        max_event = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE pipeline_state
            SET last_processed_event=%s, updated_at=CURRENT_TIMESTAMP
            WHERE pipeline_name='clickhouse_incremental'
            """,
            (max_event,)
        )
        conn.commit()
    print(f"[CLICKHOUSE] Full load complete. Incremental state set to event_id={max_event}")

def incremental_load(conn, client):
    """ CDC events"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT last_processed_event FROM pipeline_state WHERE pipeline_name='clickhouse_incremental'")
        row = cur.fetchone()
        last_event = int(row["last_processed_event"]) if row else 0
        
        cur.execute(
            """
            SELECT event_id, table_name, operation, payload
            FROM cdc_events
            WHERE event_id > %s
            ORDER BY event_id
            """,
            (last_event,)
        )
        events = cur.fetchall()
        
        if not events:
            print("[CLICKHOUSE] No new CDC events. Nothing to load.")
            return
        
        changed = defaultdict(set)
        for e in events:
            table = norm_name(e["table_name"])
            payload = canonical_payload(e["payload"])
            if e["operation"].upper() == "DELETE":
                if table == "customers" and payload.get("customerid"):
                    changed["customer_ids"].add(str(payload["customerid"]))
                elif table == "employees" and payload.get("employeeid"):
                    changed["employee_ids"].add(int(payload["employeeid"]))
                elif table == "suppliers" and payload.get("supplierid"):
                    changed["supplier_ids"].add(int(payload["supplierid"]))
                elif table == "products" and payload.get("productid"):
                    changed["product_ids"].add(int(payload["productid"]))
                elif table == "categories" and payload.get("categoryid"):
                    changed["category_ids"].add(int(payload["categoryid"]))
                elif table == "shippers" and payload.get("shipperid"):
                    changed["shipper_ids"].add(int(payload["shipperid"]))
                elif table in ["orders", "orderdetails"] and payload.get("orderid"):
                    changed["order_ids"].add(int(payload["orderid"]))
        
        if any(k in changed for k in ["customer_ids", "employee_ids", "supplier_ids", "order_ids"]):
            insert_rows(client, "dim_geography", fetch_rows(conn, "ready_dim_geography"))
        
        if changed["supplier_ids"]:
            vals = sorted(changed["supplier_ids"])
            insert_rows(client, "dim_suppliers", fetch_rows(conn, "ready_dim_suppliers", "supplier_alternate_key = ANY(%s)", (vals,)))
        
        product_rows = []
        if changed["product_ids"]:
            vals = sorted(changed["product_ids"])
            product_rows += fetch_rows(conn, "ready_dim_products", "product_alternate_key = ANY(%s)", (vals,))
        if changed["category_ids"]:
            vals = sorted(changed["category_ids"])
            product_rows += fetch_rows(conn, "ready_dim_products", "category_alternate_key = ANY(%s)", (vals,))
        if product_rows:
            dedup = {r["product_key"]: r for r in product_rows}
            insert_rows(client, "dim_products", list(dedup.values()))
        
        if changed["customer_ids"]:
            vals = sorted(changed["customer_ids"])
            insert_rows(client, "dim_customers", fetch_rows(conn, "ready_dim_customers", "customer_alternate_key = ANY(%s)", (vals,)))
        
        if changed["employee_ids"]:
            vals = sorted(changed["employee_ids"])
            insert_rows(client, "dim_employees", fetch_rows(conn, "ready_dim_employees", "employee_alternate_key = ANY(%s)", (vals,)))
        
        if changed["shipper_ids"]:
            vals = sorted(changed["shipper_ids"])
            insert_rows(client, "dim_shippers", fetch_rows(conn, "ready_dim_shippers", "shipper_alternate_key = ANY(%s)", (vals,)))
        
        if changed["order_ids"]:
            vals = sorted(changed["order_ids"])
            insert_rows(client, "dim_ship_name", fetch_rows(conn, "ready_dim_ship_name"))
            insert_rows(client, "dim_date", fetch_rows(conn, "ready_dim_date"))
            insert_rows(client, "fact_orders", fetch_rows(conn, "ready_fact_orders", "order_id = ANY(%s)", (vals,)))
        
        max_event = max(int(e["event_id"]) for e in events)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_state
                SET last_processed_event=%s, updated_at=CURRENT_TIMESTAMP
                WHERE pipeline_name='clickhouse_incremental'
                """,
                (max_event,)
            )
            conn.commit()
        print(f"[CLICKHOUSE] Incremental load complete through event_id={max_event}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Load all curated rows once")
    args = parser.parse_args()
    
    with stg_conn() as conn:
        client = ch_client()
        if args.full:
            full_load(conn, client)
        else:
            incremental_load(conn, client)

if __name__ == "__main__":
    main()
