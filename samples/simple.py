import sys
import time

import libipmq

def producer():
    producer = libipmq.Producer("test.queue", "/dev/shm/test.data", 2048)

    number = 1
    while True:
        message = "Hello, World #{}!".format(number)
        message_bytes = message.encode("utf-8")
        print("Sending: {}".format(message))

        # allocation = producer.allocate(len(message_bytes))
        # allocation.copy_from(0, message_bytes)
        # producer.publish("test", allocation)
        producer.publish_bytes("test", message_bytes)

        number += 1
        time.sleep(0.2)

def consumer():
    consumer = libipmq.Consumer("test.queue")
    consumer.create_queue("test", True)
    consumer.bind_queue("test", ".*")

    def callback(commands, queue_id: int, routing_key: str, message_id: int, message):
        print("{}: {}".format(message_id, message.decode("utf-8")))
        commands.acknowledge(queue_id, message_id)

        if message_id == 10:
            commands.stop_consume(queue_id)

    consumer.start_consume_queue("test", callback)

if __name__ == "__main__":
    libipmq.enable_logging()

    command = sys.argv[1]
    if command == "producer":
        producer()
    elif command == "consumer":
        consumer()
