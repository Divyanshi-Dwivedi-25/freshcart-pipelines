"""Publishes a food/ride request every 2 s to the topic exchange 'rapido-dispatch'.

    AMQP_URL=amqp://guest:guest@localhost:5672/%2F python stream/dispatcher.py

Every 10th message is deliberately bad (no "zone" field) for the D3 ACK/NACK demo.
"""
import json
import os
import random
import time
from datetime import datetime, timezone

import pika

EXCHANGE = "rapido-dispatch"
INTERVAL = 2.0
BAD_EVERY = 10
CITIES = ["bangalore", "chennai"]
ZONES = ["koramangala", "indiranagar", "whitefield", "tnagar"]
TYPES = ["ride", "food"]


def build_request(req_id: int) -> tuple[str, dict]:
    city, zone, typ = random.choice(CITIES), random.choice(ZONES), random.choice(TYPES)
    msg = {"id": req_id, "city": city, "zone": zone, "type": typ,
           "ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    key = f"{city}.{zone}.{typ}"
    if req_id % BAD_EVERY == 0:
        del msg["zone"]  # bad message: routing key is fine, body is not
    return key, msg


def main():
    url = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
    conn = pika.BlockingConnection(pika.URLParameters(url))
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic")
    req_id = 0
    try:
        while True:
            req_id += 1
            key, msg = build_request(req_id)
            ch.basic_publish(exchange=EXCHANGE, routing_key=key, body=json.dumps(msg))
            flag = "  (BAD: no zone)" if "zone" not in msg else ""
            print(f"[dispatcher] #{req_id} key={key}{flag}", flush=True)
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
