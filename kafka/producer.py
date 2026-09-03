import json
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import env

def get_conn():
    return psycopg2.connect(
        host=env("OP_HOST", "localhost"),
        port=int(env("OP_PORT", "25432")),
        dbname=env("OP_DB", "northwind"),
        user=env("OP_USER", "admin"),
        password=env("OP_PASSWORD", "admin123"),
    )

def main():
    topic = env("KAFKA_TOPIC", "northwind_changes")
    producer = Producer({"bootstrap.servers": env("KAFKA_BOOTSTRAP", "localhost:9092")})
    
    while True:
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT last_event_id FROM etl_control WHERE pipeline_name='northwind_kafka'"
                    )
                    row = cur.fetchone()
                    last_id = int(row["last_event_id"]) if row else 0
                    
                    cur.execute(
                        """
                        SELECT event_id, table_name, operation, event_time, payload
                        FROM cdc_event_log
                        WHERE event_id > %s
                        ORDER BY event_id
                        LIMIT 500
                        """,
                        (last_id,)
                    )
                    events = cur.fetchall()
                    
                    if not events:
                        time.sleep(2)
                        continue
                    
                    max_id = last_id
                    for event in events:
                        message = {
                            "event_id": int(event["event_id"]),
                            "table_name": event["table_name"],
                            "operation": event["operation"],
                            "event_time": event["event_time"].isoformat() if event["event_time"] else None,
                            "payload": event["payload"],
                        }
                        producer.produce(
                            topic,
                            key=str(event["event_id"]),
                            value=json.dumps(message, default=str).encode("utf-8"),
                        )
                        producer.poll(0)
                        max_id = max(max_id, int(event["event_id"]))
                    
                    remaining = producer.flush(10)
                    if remaining != 0:
                        raise RuntimeError(f"Kafka flush timed out; {remaining} message(s) remain")
                    
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE etl_control
                            SET last_event_id=%s, updated_at=CURRENT_TIMESTAMP
                            WHERE pipeline_name='northwind_kafka'
                            """,
                            (max_id,)
                        )
                        conn.commit()
                    
                    print(f"Published {len(events)} event(s); last_event_id={max_id}", flush=True)
                    
        except KeyboardInterrupt:
            print("Producer stopped by user.")
            break
        except Exception as exc:
            print(f"[PRODUCER ERROR] {exc}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
