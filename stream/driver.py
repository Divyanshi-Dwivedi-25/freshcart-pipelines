"""A driver: private queue bound to PATTERN on 'rapido-dispatch'.

    AMQP_URL=... python stream/driver.py Ravi "bangalore.koramangala.*"

Messages without a "zone" field are rejected with basic_nack(requeue=False);
all others are acknowledged with basic_ack.
"""
import json
import os
import sys

import pika

EXCHANGE = "rapido-dispatch"


def is_valid(msg: dict) -> bool:
    return "zone" in msg


def main(name: str, pattern: str):
    url = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
    conn = pika.BlockingConnection(pika.URLParameters(url))
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic")
    queue = ch.queue_declare(queue="", exclusive=True).method.queue  # private queue
    ch.queue_bind(exchange=EXCHANGE, queue=queue, routing_key=pattern)
    ch.basic_qos(prefetch_count=1)
    print(f"[{name}] queue={queue} bound to '{pattern}'", flush=True)

    def on_message(channel, method, props, body):
        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            msg = {}
        if not is_valid(msg):
            print(f"[{name}] REJECT (NACK) key={method.routing_key} body={body.decode()}", flush=True)
            channel.basic_nack(method.delivery_tag, requeue=False)
            return
        print(f"[{name}] #{msg['id']} {method.routing_key} {msg['ts']}", flush=True)
        channel.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=queue, on_message_callback=on_message)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: driver.py NAME PATTERN")
    main(sys.argv[1], sys.argv[2])
