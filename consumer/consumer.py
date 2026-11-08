import json
import os
import time
from datetime import datetime
from decimal import Decimal

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickhouse-loader")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_USER = os.getenv("CLICKHOUSE_USER", "dwh")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "dwh")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
FLUSH_INTERVAL = float(os.getenv("FLUSH_INTERVAL", "5"))  # секунд

COLUMNS = [
    "transaction_id",
    "client_id",
    "merchant",
    "category",
    "amount",
    "currency",
    "event_time",
]


def parse_message(raw):
    """Разбирает JSON из Kafka в строку для вставки. Если сообщение битое - вернёт None."""
    try:
        data = json.loads(raw)
        return [
            str(data["transaction_id"]),
            int(data["client_id"]),
            str(data["merchant"]),
            str(data["category"]),
            Decimal(str(data["amount"])),
            str(data["currency"]),
            datetime.fromisoformat(data["event_time"]),
        ]
    except (ValueError, KeyError, TypeError, json.JSONDecodeError, ArithmeticError):
        return None


def connect_clickhouse():
    # ClickHouse может стартовать дольше, чем мы, поэтому пробуем несколько раз
    for attempt in range(1, 11):
        try:
            return clickhouse_connect.get_client(
                host=CH_HOST, username=CH_USER, password=CH_PASSWORD
            )
        except Exception as e:
            print(f"не удалось подключиться к ClickHouse (попытка {attempt}): {e}")
            time.sleep(3)
    raise RuntimeError("ClickHouse недоступен")


def flush(client, consumer, rows):
    if not rows:
        return
    client.insert("raw.transactions", rows, column_names=COLUMNS)
    # коммитим оффсеты только после успешной вставки,
    # поэтому при падении возможны дубли, но не потери
    consumer.commit(asynchronous=False)
    print(f"записано {len(rows)} строк")


def main():
    client = connect_clickhouse()
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    rows = []
    skipped = 0
    last_flush = time.time()

    print(f"читаю топик {TOPIC}")
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"ошибка Kafka: {msg.error()}")
                else:
                    row = parse_message(msg.value())
                    if row is None:
                        skipped += 1
                        print(f"пропущено битое сообщение (всего {skipped})")
                    else:
                        rows.append(row)

            time_is_up = time.time() - last_flush >= FLUSH_INTERVAL
            if len(rows) >= BATCH_SIZE or (rows and time_is_up):
                flush(client, consumer, rows)
                rows = []
                last_flush = time.time()
            elif time_is_up:
                last_flush = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        flush(client, consumer, rows)
        consumer.close()


if __name__ == "__main__":
    main()
