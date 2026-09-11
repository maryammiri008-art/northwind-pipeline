# 🌊 Northwind Real-Time Data Pipeline

پروژه نهایی مهندسی داده: تبدیل یک ETL بچ به یک پایپ‌لاین **نزدیک به Real-Time** با معماری مدرن.

---

## 🏗️ معماری
Northwind (PostgreSQL)
↓ CDC (Trigger)
CDC Event Log (PostgreSQL)
↓ Kafka Producer
Kafka: northwind_changes
↓ Kafka Consumer
PostgreSQL Staging (current_rows)
↓ Spark Transform
ready_* Tables (Staging)
↓ Load ClickHouse
ClickHouse Data Warehouse
↓
Grafana Dashboard

**ارکستراسیون**: Apache Airflow
**کانتینریزیشن**: Docker

---

## 🛠️ تکنولوژی‌ها

| تکنولوژی | نسخه | کاربرد |
|----------|-------|--------|
| Docker | latest | کانتینریزیشن |
| PostgreSQL | 15 | دیتابیس OP + Staging |
| Apache Kafka | 4.0.0 | انتقال تغییرات |
| PySpark | 3.5.8 | پردازش و تبدیل |
| ClickHouse | latest | Data Warehouse |
| Grafana | 11.6.0 | داشبورد |
| Airflow | 2.10.5 | ارکستراسیون |
| Nginx | alpine | سرور تصاویر |

---

## 📂 ساختار پروژه
de_final_project/
├── docker/
│ ├── docker-compose.yml
│ ├── Dockerfile
│ └── clickhouse-datasource-4.21.1.zip
├── sql/
│ ├── op/
│ ├── staging/
│ └── clickhouse/
├── kafka/
├── spark/
├── python/
├── airflow/dags/
├── scripts/
├── data_lake/employees/
├── screenshots/
├── postgresql-42.7.3.jar
├── .env.example
├── .gitignore
└── README.md

---

## 🚀 نحوه اجرا

### پیش‌نیازها
- Docker Desktop (با WSL Integration)
- Python 3.10
- Spark 3.5.8
- JDK 17

