import base64
import json
import os

import time
import multiprocessing as mp

import numpy as np

try:
    from cv2 import cv2
except ImportError:
    pass

from kafka import KafkaProducer

KAFKA_TOPIC = 'frames'
CAMERA_ID = 'camera.01'

camera = cv2.VideoCapture(0)

kafka_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    client_id="capturer",
    max_request_size=2147483647
)

framenum = 0
lastStamp = time.time()

def send_frame_to_kafka(frame_obj):
        t = frame_obj['time']
        frame_raw = frame_obj['frame']
        _1, buffer = cv2.imencode('.jpg', frame_raw)
        bytes64_encoded = base64.b64encode(buffer)
        bytes64_string = bytes64_encoded.decode('utf-8')
        output_message = {
            'camera': CAMERA_ID,
            'frame': bytes64_string,
            'time': t
        }
        kafka_producer.send(KAFKA_TOPIC, value=json.dumps(output_message).encode('utf-8'))

def kafka_sender(queue):
    print(os.getpid(), "working")
    while True:
        frame = queue.get(True)
        print(os.getpid(), "got a new frame")
        send_frame_to_kafka(frame)


if __name__ ==  '__main__':
    num_cpus = 3
    queueFrames = mp.Queue()
    pool = mp.Pool(num_cpus, kafka_sender, (queueFrames, ))
    while True:
        ret, frame = camera.read()
        elapsed = time.time() - lastStamp
        print('Frame {0} capture ping {1}'.format(framenum, elapsed))
        lastStamp = time.time()
        framenum += 1

        cv2.imshow(CAMERA_ID, frame)
        queueFrames.put({
            'time': lastStamp,
            'frame': frame
        })
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
        time.sleep(0.09)

    camera.release()
    cv2.destroyAllWindows()
    pool.close()
    pool.join()


