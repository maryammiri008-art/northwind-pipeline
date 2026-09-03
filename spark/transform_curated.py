import os
from functools import reduce
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
PROJECT_HOME = Path(os.getenv("PROJECT_HOME", Path.home() / "de_final_project"))
load_dotenv(PROJECT_HOME / ".env")

STG_HOST = os.getenv("STG_HOST", "localhost")
STG_PORT = os.getenv("STG_PORT", "35432")
STG_DB = os.getenv("STG_DB", "northwind_staging")
STG_USER = os.getenv("STG_USER", "admin")
STG_PASSWORD = os.getenv("STG_PASSWORD", "admin123")

JDBC_URL = f"jdbc:postgresql://{STG_HOST}:{STG_PORT}/{STG_DB}"
JDBC_PROPS = {
    "user": STG_USER,
    "password": STG_PASSWORD,
    "driver": "org.postgresql.Driver",
}
spark = (
    SparkSession.builder
    .appName("NorthwindCuratedTransform")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
def json_col(df, name, data_type="string"):
    c = F.get_json_object(F.col("payload"), f"$.{name}")
    return c.cast(data_type) if data_type else c
def source_table(rows, name, columns):
    df = rows.filter((F.col("table_name") == name) & (~F.col("is_deleted")))
    selects = [json_col(df, src, dtype).alias(alias) for src, alias, dtype in columns]
    return df.select(*selects)
def hkey(prefix, *cols):
    safe = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return F.abs(F.hash(F.lit(prefix), *safe)).cast("long")
def geo_key(country, region, city, postal, address):
    return F.abs(
        F.hash(
            F.lit("GEO"),
            F.coalesce(F.col(country).cast("string"), F.lit("")),
            F.coalesce(F.col(region).cast("string"), F.lit("")),
            F.coalesce(F.col(city).cast("string"), F.lit("")),
            F.coalesce(F.col(postal).cast("string"), F.lit("")),
            F.coalesce(F.col(address).cast("string"), F.lit("")),
        )
    ).cast("long")

def clean_str(c):
    return F.when(F.length(F.trim(F.col(c))) == 0, None).otherwise(F.trim(F.col(c)))
def write_ready(df, table):
    print(f"[SPARK] writing {table}: {df.count()} row(s)")
    df.write.mode("overwrite").jdbc(JDBC_URL, table, properties=JDBC_PROPS)
print("[SPARK] Reading from current_rows...")
rows = spark.read.jdbc(JDBC_URL, "current_rows", properties=JDBC_PROPS)
customers = source_table(rows, "customers", [
    ("customerid", "customer_id", "string"),
    ("companyname", "company_name", "string"),
    ("contactname", "contact_name", "string"),
    ("contacttitle", "contact_title", "string"),
    ("address", "address", "string"),
    ("city", "city", "string"),
    ("region", "region", "string"),
    ("postalcode", "postal_code", "string"),
    ("country", "country", "string"),
    ("phone", "phone", "string"),
    ("fax", "fax", "string"),
])

employees = source_table(rows, "employees", [
    ("employeeid", "employee_id", "int"),
    ("lastname", "last_name", "string"),
    ("firstname", "first_name", "string"),
    ("title", "title", "string"),
    ("titleofcourtesy", "title_of_courtesy", "string"),
    ("birthdate", "birth_date", "date"),
    ("hiredate", "hire_date", "date"),
    ("address", "address", "string"),
    ("city", "city", "string"),
 ("region", "region", "string"),
    ("postalcode", "postal_code", "string"),
    ("country", "country", "string"),
    ("homephone", "home_phone", "string"),
    ("extension", "extension", "string"),
    ("reportsto", "reports_to", "int"),
])

suppliers = source_table(rows, "suppliers", [
    ("supplierid", "supplier_id", "int"),
    ("companyname", "company_name", "string"),
    ("contactname", "contact_name", "string"),
    ("contacttitle", "contact_title", "string"),
    ("address", "address", "string"),
    ("city", "city", "string"),
    ("region", "region", "string"),
    ("postalcode", "postal_code", "string"),
    ("country", "country", "string"),
    ("phone", "phone", "string"),
    ("fax", "fax", "string"),
    ("homepage", "home_page", "string"),
])
products = source_table(rows, "products", [
    ("productid", "product_id", "int"),
    ("productname", "product_name", "string"),
    ("supplierid", "supplier_id", "int"),
    ("categoryid", "category_id", "int"),
    ("quantityperunit", "quantity_per_unit", "string"),
    ("unitprice", "unit_price", "double"),
    ("unitsinstock", "units_in_stock", "int"),
    ("unitsonorder", "units_on_order", "int"),
    ("reorderlevel", "reorder_level", "int"),
    ("discontinued", "discontinued_raw", "string"),
])

categories = source_table(rows, "categories", [
    ("categoryid", "category_id", "int"),
    ("categoryname", "category_name", "string"),
    ("description", "description", "string"),
])

orders = source_table(rows, "orders", [
    ("orderid", "order_id", "int"),
    ("customerid", "customer_id", "string"),
    ("employeeid", "employee_id", "int"),
    ("orderdate", "order_date_raw", "date"),
    ("requireddate", "required_date_raw", "date"),
    ("shippeddate", "shipped_date_raw", "date"),
    ("shipvia", "ship_via", "int"),
("freight", "freight", "double"),
    ("shipname", "ship_name", "string"),
    ("shipaddress", "ship_address", "string"),
    ("shipcity", "ship_city", "string"),
    ("shipregion", "ship_region", "string"),
    ("shippostalcode", "ship_postal_code", "string"),
    ("shipcountry", "ship_country", "string"),
])

order_details = source_table(rows, "orderdetails", [
    ("orderid", "order_id", "int"),
    ("productid", "product_id", "int"),
    ("unitprice", "unit_price", "double"),
    ("quantity", "quantity", "int"),
    ("discount", "discount", "double"),
])

shippers = source_table(rows, "shippers", [
    ("shipperid", "shipper_id", "int"),
    ("companyname", "company_name", "string"),
    ("phone", "phone", "string"),
])
print("[SPARK] Building dimensions...")

# 1. DimGeography
dim_geography = (
    customers.select("country", "region", "city", "postal_code", "address")
    .unionByName(employees.select("country", "region", "city", "postal_code", "address"))
    .unionByName(suppliers.select("country", "region", "city", "postal_code", "address"))
    .unionByName(orders.select(
        F.col("ship_country").alias("country"),
        F.col("ship_region").alias("region"),
        F.col("ship_city").alias("city"),
        F.col("ship_postal_code").alias("postal_code"),
        F.col("ship_address").alias("address")
    ))
    .dropDuplicates()
    .filter(F.col("country").isNotNull())
    .withColumn("geography_key", geo_key("country", "region", "city", "postal_code", "address"))
    .select("geography_key", "country", "region", "city", "postal_code", "address")
)

# 2. DimSuppliers
dim_suppliers = (
    suppliers
    .withColumn("supplier_key", hkey("SUP", "supplier_id"))
    .withColumn("geography_key", geo_key("country", "region", "city", "postal_code", "address"))
 .select(
        "supplier_key",
        F.col("supplier_id").alias("supplier_alternate_key"),
        "geography_key",
        "company_name",
        "contact_name",
        "contact_title",
        "phone",
        "fax",
        "home_page",
    )
)

# 3. DimProducts
dim_products = (
    products.alias("p")
    .join(categories.alias("c"), F.col("p.category_id") == F.col("c.category_id"), "left")
    .withColumn("product_key", hkey("PROD", "p.product_id"))
    .withColumn("supplier_key", hkey("SUP", "p.supplier_id"))
    .withColumn("discontinued", 
        F.when(F.lower(F.col("p.discontinued_raw")).isin("1", "true", "t", "yes"), F.lit(1))
        .otherwise(F.lit(0))
    )
    .select(
        "product_key",
        F.col("p.product_id").alias("product_alternate_key"),
        "supplier_key",
        F.col("p.category_id").alias("category_alternate_key"),
        F.col("p.product_name").alias("product_name"),
        F.col("c.category_name").alias("category_name"),
        F.col("p.quantity_per_unit").alias("quantity_per_unit"),
        F.col("p.unit_price").alias("unit_price"),
        F.col("p.units_in_stock").alias("units_in_stock"),
        F.col("p.units_on_order").alias("units_on_order"),
        F.col("p.reorder_level").alias("reorder_level"),
        "discontinued",
    )
)  
# 4. DimCustomers
dim_customers = (
    customers
    .withColumn("customer_key", hkey("CUST", "customer_id"))
    .withColumn("geography_key", geo_key("country", "region", "city", "postal_code", "address"))
    .select(
        "customer_key",
        F.col("customer_id").alias("customer_alternate_key"),
        "geography_key",
        "company_name",
        "contact_name",
        "contact_title",
        "phone",
        "fax",
    )
)

# 5. DimEmployees
dim_employees = (
    employees
    .withColumn("employee_key", hkey("EMP", "employee_id"))
    .withColumn("geography_key", geo_key("country", "region", "city", "postal_code", "address"))
    .withColumn("photo_url", F.concat(F.lit("http://localhost:28088/"), F.col("employee_id").cast("string"), F.lit(".jpg"))
    )
    .select(
        "employee_key",
        F.col("employee_id").alias("employee_alternate_key"),
        "geography_key",
        "first_name",
        "last_name",
        "title",
        "photo_url",
    )
)

# 6. DimShippers
dim_shippers = (
    shippers
    .withColumn("shipper_key", hkey("SHIPPER", "shipper_id"))
    .select(
        "shipper_key",
        F.col("shipper_id").alias("shipper_alternate_key"),
        "company_name",
        "phone",
    )
)
# 7. DimShipName
dim_ship_name = (
    orders.select(clean_str("ship_name").alias("ship_name"))
    .filter(F.col("ship_name").isNotNull())
    .dropDuplicates()
    .withColumn("ship_name_key", hkey("SHIPNAME", "ship_name"))
    .select("ship_name_key", "ship_name")
)

# 8. DimDate
orders_dates = orders.select(
    F.to_date("order_date_raw").alias("order_date"),
    F.to_date("required_date_raw").alias("required_date"),
    F.to_date("shipped_date_raw").alias("shipped_date"),
)

all_dates = (
    orders_dates.select(F.col("order_date").alias("full_date"))
    .unionByName(orders_dates.select(F.col("required_date").alias("full_date")))
    .unionByName(orders_dates.select(F.col("shipped_date").alias("full_date")))
    .filter(F.col("full_date").isNotNull())
    .dropDuplicates()
)

dim_date = (
    all_dates.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date").cast("short"))
    .withColumn("quarter", F.quarter("full_date").cast("byte"))
    .withColumn("month", F.month("full_date").cast("byte"))
    .withColumn("month_name", F.date_format("full_date", "MMM"))
    .withColumn("day", F.dayofmonth("full_date").cast("byte"))
    .withColumn("day_of_week", F.dayofweek("full_date").cast("byte"))
    .select("date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week")
)
print("[SPARK] Building fact_orders...")

fact_orders = (
    orders.alias("o")
    .join(order_details.alias("d"), F.col("o.order_id") == F.col("d.order_id"), "inner")
    .withColumn("product_key", hkey("PROD", "d.product_id"))
    .withColumn("customer_key", hkey("CUST", "o.customer_id"))
    .withColumn("employee_key", hkey("EMP", "o.employee_id"))
    .withColumn("shipper_key", hkey("SHIPPER", "o.ship_via"))
    .withColumn("ship_name_key", hkey("SHIPNAME", "o.ship_name"))
    .withColumn("geography_key", geo_key("o.ship_country", "o.ship_region", "o.ship_city", "o.ship_postal_code", "o.ship_address"))
    .withColumn("order_date_key", F.date_format("o.order_date_raw", "yyyyMMdd").cast("int"))
    .withColumn("required_date_key", F.date_format("o.required_date_raw", "yyyyMMdd").cast("int"))
    .withColumn("shipped_date_key", F.date_format("o.shipped_date_raw", "yyyyMMdd").cast("int"))
    .withColumn("discount", F.coalesce(F.col("d.discount"), F.lit(0.0)))
    .withColumn("freight", F.coalesce(F.col("o.freight"), F.lit(0.0)))
    .withColumn("line_amount", 
        F.round(F.col("d.unit_price") * F.col("d.quantity") * (F.lit(1.0) - F.col("discount")), 2)
    )
    .select(
        F.col("o.order_id").alias("order_id"),
        "product_key",
        "customer_key",
        "employee_key",
        "required_date_key",
        "shipped_date_key",
        "order_date_key",
        "shipper_key",
        "geography_key",
        "ship_name_key",
        F.col("d.unit_price").alias("unit_price"),
        F.col("d.quantity").alias("quantity"),
        "discount",
        "freight",
        "line_amount",
    )
)
print("[SPARK] Writing curated tables to staging...")
write_ready(dim_geography, "ready_dim_geography")
write_ready(dim_suppliers, "ready_dim_suppliers")
write_ready(dim_products, "ready_dim_products")
write_ready(dim_customers, "ready_dim_customers")
write_ready(dim_employees, "ready_dim_employees")
write_ready(dim_shippers, "ready_dim_shippers")
write_ready(dim_ship_name, "ready_dim_ship_name")
write_ready(dim_date, "ready_dim_date")
write_ready(fact_orders, "ready_fact_orders")

spark.stop()
print("[SPARK] Curated transformation completed successfully.")
