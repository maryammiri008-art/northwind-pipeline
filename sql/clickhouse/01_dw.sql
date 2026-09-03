
CREATE DATABASE IF NOT EXISTS northwind_dw;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_geography (
    geography_key Int64,
    country Nullable(String),
    region Nullable(String),
    city Nullable(String),
    postal_code Nullable(String),
    address Nullable(String),
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY geography_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_suppliers (
    supplier_key Int64,
    supplier_alternate_key Int32,
    geography_key Nullable(Int64),
    company_name String,
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    home_page Nullable(String),
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY supplier_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_products (
    product_key Int64,
    product_alternate_key Int32,
    supplier_key Nullable(Int64),
    category_alternate_key Nullable(Int32),
    product_name String,
    category_name Nullable(String),
    quantity_per_unit Nullable(String),
    unit_price Nullable(Float64),
    units_in_stock Nullable(Int32),
    units_on_order Nullable(Int32),
    reorder_level Nullable(Int32),
    discontinued UInt8,
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY product_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_customers (
    customer_key Int64,
    customer_alternate_key String,
    geography_key Nullable(Int64),
    company_name String,
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY customer_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_employees (
    employee_key Int64,
    employee_alternate_key Int32,
    geography_key Nullable(Int64),
    first_name String,
    last_name String,
    title Nullable(String),
    photo_url Nullable(String),
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY employee_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_shippers (
    shipper_key Int64,
    shipper_alternate_key Int32,
    company_name String,
    phone Nullable(String),
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY shipper_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_ship_name (
    ship_name_key Int64,
    ship_name String,
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY ship_name_key;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_date (
    date_key Int32,
    full_date Date,
    year Int16,
    quarter Int8,
    month Int8,
    month_name String,
    day Int8,
    day_of_week Int8,
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY date_key;

CREATE TABLE IF NOT EXISTS northwind_dw.fact_orders (
    order_id Int32,
    product_key Int64,
    customer_key Nullable(Int64),
    employee_key Nullable(Int64),
    required_date_key Nullable(Int32),
    shipped_date_key Nullable(Int32),
    order_date_key Nullable(Int32),
    shipper_key Nullable(Int64),
    geography_key Int64,
    ship_name_key Nullable(Int64),
    unit_price Float64,
    quantity Int32,
    discount Float64,
    freight Float64,
    line_amount Float64,
    version DateTime,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY (order_id, product_key);
