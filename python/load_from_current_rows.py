import sys
import json
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

def get_table_data(conn, table_name):
    """ current_rows"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT payload 
            FROM current_rows 
            WHERE table_name = %s AND is_deleted = FALSE
            """,
            (table_name,)
        )
        rows = cur.fetchall()
        result = []
        for row in rows:
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            result.append(payload)
        return result

def insert_rows(client, ch_table, data, column_names):
    """ClickHouse"""
    if not data:
        print(f"[CLICKHOUSE] {ch_table}: no rows to insert")
        return 0
    
    now = datetime.now().replace(microsecond=0)
    
    final_data = []
    for row in data:
        row.append(now)
        row.append(0)
        final_data.append(row)
    
    final_columns = column_names + ["version", "is_deleted"]
    
    client.insert(ch_table, final_data, column_names=final_columns)
    print(f"[CLICKHOUSE] {ch_table}: inserted {len(data)} row(s)")
    return len(data)

def main():
    print("[INFO] Connecting to Staging...")
    conn = stg_conn()
    
    print("[INFO] Connecting to ClickHouse...")
    client = ch_client()
    
    # 1. DimCustomers
    customers = get_table_data(conn, "customers")
    if customers:
        data = []
        for row in customers:
            data.append([
                row.get("customerid"),           # customer_alternate_key
                row.get("companyname"),          # company_name
                row.get("contactname"),          # contact_name
                row.get("contacttitle"),         # contact_title
                row.get("phone"),                # phone
                row.get("fax"),                  # fax
            ])
        insert_rows(client, "dim_customers", data, 
                   ["customer_alternate_key", "company_name", "contact_name", 
                    "contact_title", "phone", "fax"])
    
    # 2. DimEmployees
    employees = get_table_data(conn, "employees")
    if employees:
        data = []
        for row in employees:
            data.append([
                row.get("employeeid"),           # employee_alternate_key
                row.get("firstname"),            # first_name
                row.get("lastname"),             # last_name
                row.get("title"),                # title
                f"http://localhost:28088/{row.get('employeeid')}.jpg"  # photo_url
            ])
        insert_rows(client, "dim_employees", data,
                   ["employee_alternate_key", "first_name", "last_name", "title", "photo_url"])
    
    # 3. DimSuppliers
    suppliers = get_table_data(conn, "suppliers")
    if suppliers:
        data = []
        for row in suppliers:
            data.append([
                row.get("supplierid"),           # supplier_alternate_key
                row.get("companyname"),          # company_name
                row.get("contactname"),          # contact_name
                row.get("contacttitle"),         # contact_title
                row.get("phone"),                # phone
                row.get("fax"),                  # fax
                row.get("homepage"),             # home_page
            ])
        insert_rows(client, "dim_suppliers", data,
                   ["supplier_alternate_key", "company_name", "contact_name", 
                    "contact_title", "phone", "fax", "home_page"])
    
    # 4. DimProducts
    products = get_table_data(conn, "products")
    if products:
        data = []
        for row in products:
            data.append([
                row.get("productid"),            # product_alternate_key
                row.get("productname"),          # product_name
                row.get("supplierid"),           # supplier_key 
                row.get("categoryid"),           # category_alternate_key
                row.get("quantityperunit"),      # quantity_per_unit
                float(row.get("unitprice") or 0), # unit_price
                row.get("unitsinstock"),         # units_in_stock
                row.get("unitsonorder"),         # units_on_order
                row.get("reorderlevel"),         # reorder_level
                1 if row.get("discontinued") in [1, "1", True] else 0, # discontinued
            ])
        insert_rows(client, "dim_products", data,
                   ["product_alternate_key", "product_name", "supplier_key", 
                    "category_alternate_key", "quantity_per_unit", "unit_price",
                    "units_in_stock", "units_on_order", "reorder_level", "discontinued"])
    
    # 5. DimCategories
    categories = get_table_data(conn, "categories")
    if categories:
        data = []
        for row in categories:
            data.append([
                row.get("categoryid"),           # category_key (به عنوان surrogate)
                row.get("categoryname"),         # category_name
                row.get("description"),          # description
            ])
        print(f"[INFO] Categories data: {len(data)} rows (skipped - no dim_categories table)")
    
    # 6. DimShippers
    shippers = get_table_data(conn, "shippers")
    if shippers:
        data = []
        for row in shippers:
            data.append([
                row.get("shipperid"),            # shipper_alternate_key
                row.get("companyname"),          # company_name
                row.get("phone"),                # phone
            ])
        insert_rows(client, "dim_shippers", data,
                   ["shipper_alternate_key", "company_name", "phone"])
    
    print("\n[SUCCESS] ✅ All data loaded to ClickHouse successfully!")
    conn.close()

if __name__ == "__main__":
    main()
