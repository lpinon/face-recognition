import json
import os
import struct
import time

from kafka import KafkaConsumer, KafkaProducer
from memsql.common import database
import multiprocessing as mp


def face_processor(queue):
    print(os.getpid(), "working")
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        client_id="sender",
        max_request_size=2147483647
    )
    print(os.getpid(), "connected to kafka")
    while True:
        face = queue.get(True)
        print(os.getpid(), "got a new face")
        ts = face['ts']
        camera = face['camera']
        encoding = face['encoding']
        output_message = {
            'camera': camera,
            'ts': int(ts),
            'encoding': encoding
        }
        kafka_producer.send(topic='camera_detections', value=json.dumps(output_message).encode('utf-8'))


if __name__ ==  '__main__':
    num_cpus = 1
    queueFaces = mp.Queue()
    pool = mp.Pool(num_cpus, face_processor, (queueFaces, ))
    consumer = KafkaConsumer('faces', bootstrap_servers=['localhost:9092'])
    for message in consumer:
        ping = time.time()
        message = message.value.decode('utf-8')
        data = json.loads(message)
        queueFaces.put(data)
    pool.close()
    pool.join()
    pool.close()
    pool.join()


