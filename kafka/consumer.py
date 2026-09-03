import json
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json
from confluent_kafka import Consumer, KafkaError

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import canonical_payload, build_row_key, norm_name, env

def get_conn():
    return psycopg2.connect(
        host=env("STG_HOST", "localhost"),
        port=int(env("STG_PORT", "35432")),
        dbname=env("STG_DB", "northwind_staging"),
        user=env("STG_USER", "admin"),
        password=env("STG_PASSWORD", "admin123"),
    )

def main():
    topic = env("KAFKA_TOPIC", "northwind_changes")
    consumer = Consumer({
        "bootstrap.servers": env("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id": env("KAFKA_GROUP", "northwind_staging_group"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    conn = get_conn()
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            
            event = json.loads(msg.value().decode("utf-8"))
            table_name = norm_name(event["table_name"])
            payload = canonical_payload(event["payload"])
            row_key = build_row_key(table_name, payload)
            is_deleted = event["operation"].upper() == "DELETE"
            
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO cdc_events(event_id, table_name, operation, event_time, payload)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT(event_id) DO NOTHING
                        """,
                        (int(event["event_id"]), table_name, event["operation"], event["event_time"], Json(payload))
                    )
                    
                       cur.execute(
                        """
                        INSERT INTO current_rows(table_name, row_key, payload, is_deleted, updated_at)
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT(table_name, row_key)
                        DO UPDATE SET
                            payload=EXCLUDED.payload,
                            is_deleted=EXCLUDED.is_deleted,
                            updated_at=CURRENT_TIMESTAMP
                        """,
                        (table_name, row_key, Json(payload), is_deleted)
                    )
                    conn.commit()
                    consumer.commit(message=msg, asynchronous=False)
                    print(f"Consumed event={event['event_id']} table={table_name} op={event['operation']} key={row_key}", flush=True)
                    
            except Exception:
                conn.rollback()
                raise
                
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
