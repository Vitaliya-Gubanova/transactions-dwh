import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))

# мерчанты по категориям
MERCHANTS = {
    "food": ["Pyaterochka", "Magnit", "VkusVill", "Burger Point"],
    "transport": ["Yandex Go", "Metro", "Aeroexpress"],
    "entertainment": ["Kinopoisk", "Cinema Park", "Steam"],
    "health": ["Rigla", "Gorzdrav"],
    "clothes": ["Lamoda", "Wildberries", "Zara"],
}

# в разных категориях разные типичные суммы (мин, макс)
AMOUNT_RANGE = {
    "food": (80, 3500),
    "transport": (30, 1500),
    "entertainment": (150, 4000),
    "health": (200, 6000),
    "clothes": (700, 15000),
}

BAD_RECORD_SHARE = 0.01  # доля "битых" записей, чтобы было что фильтровать в dbt


def generate_transaction(n_clients=500):
    category = random.choice(list(MERCHANTS))
    low, high = AMOUNT_RANGE[category]
    amount = round(random.uniform(low, high), 2)

    if random.random() < BAD_RECORD_SHARE:
        amount = -amount

    return {
        "transaction_id": str(uuid.uuid4()),
        "client_id": random.randint(1, n_clients),
        "merchant": random.choice(MERCHANTS[category]),
        "category": category,
        "amount": amount,
        "currency": "RUB",
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def on_delivery(err, msg):
    if err is not None:
        print(f"ошибка доставки: {err}")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    delay = 1 / EVENTS_PER_SECOND
    sent = 0

    print(f"отправляю в топик {TOPIC}, {EVENTS_PER_SECOND} событий/сек")
    try:
        while True:
            tx = generate_transaction()
            producer.produce(
                TOPIC,
                key=str(tx["client_id"]),
                value=json.dumps(tx),
                callback=on_delivery,
            )
            producer.poll(0)
            sent += 1
            if sent % 100 == 0:
                print(f"отправлено {sent}")
            time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
