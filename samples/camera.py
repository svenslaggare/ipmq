import struct
import sys
import time

import cv2
import numpy as np

import libipmq

def producer():
    capture = cv2.VideoCapture(0, cv2.CAP_V4L2)
    capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter.fourcc('M', 'J', 'P', 'G'))
    capture.set(cv2.CAP_PROP_FPS, 30.0)
    capture.set(cv2.CAP_PROP_FRAME_WIDTH, 1280.0)
    capture.set(cv2.CAP_PROP_FRAME_HEIGHT, 720.0)

    _, frame = capture.read()
    frame_data_size = np.product(list(frame.shape))
    frame_size = 12 + frame_data_size

    producer = libipmq.Producer("test.queue", "/dev/shm/test.data", 3 * frame_size)

    last_measurement = time.time()
    count = 0

    while True:
        allocation = producer.allocate(frame_size)
        frame = np.asarray(allocation)[12:].reshape((frame.shape[0], frame.shape[1], 3))
        capture.read(frame)

        allocation.copy_from(0, struct.pack("iii", frame.shape[1], frame.shape[0], 16))
        producer.publish("test", allocation)

        count += 1
        elapsed = (time.time() - last_measurement)
        if elapsed >= 1.0:
            print("FPS: {:.3f}".format(count / elapsed))
            last_measurement = time.time()
            count = 0

def consumer():
    consumer = libipmq.Consumer("test.queue")
    consumer.create_queue("test", True)
    consumer.bind_queue("test", ".*")

    def callback(queue_id, routing_key, message_id, message):
        width, height, img_format = struct.unpack("iii", message[:12])
        frame = np.frombuffer(message[12:], np.uint8).reshape((height, width, 3))
        cv2.imshow("Frame - {}x{}".format(width, height), frame)
        cv2.waitKey(1)

    consumer.start_consume_queue("test", callback)

if __name__ == "__main__":
    command = sys.argv[1]
    if command == "producer":
        producer()
    elif command == "consumer":
        consumer()
