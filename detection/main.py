import json
import base64
import os
from io import StringIO
import numpy as np
import time
import dlib
from imutils import face_utils
from kafka import KafkaConsumer, KafkaProducer
import multiprocessing as mp
import face_recognition

from cv2 import cv2

def readb64_frame(base64_string):
    bytes_image = base64.b64decode(base64_string)
    return cv2.imdecode(np.fromstring(bytes_image, np.uint8), cv2.IMREAD_ANYCOLOR)

def rect_to_bb(rect):
    # take a bounding predicted by face-recognition and convert it
    # to the format (x, y, w, h) as we would normally do
    # with OpenCV
    (top, right, bottom, left) = rect
    x = left
    y = top
    w = right - x
    h = bottom - y
    # return a tuple of (x, y, w, h)
    return (x, y, w, h)


def get_face_locations(frame):
    faces = face_recognition.face_locations(frame['frame'])
    return faces


def load_base64_frame_img(frame):
    frame_time = frame['time']
    frame_camera = frame['camera']
    frame_img = readb64_frame(frame['frame'])
    frame_loaded = {
        'time': frame_time,
        'frame': frame_img,
        'camera': frame_camera
    }
    return frame_loaded


def get_faces_encoding(frame, face_locations):
    faces_out = []
    faces_encodings = face_recognition.face_encodings(frame['frame'], face_locations)
    for j, face_frame in enumerate(face_locations):
        faces_out.append({
            'encoding': faces_encodings[j],
            'ts': frame['time'],
            'camera': frame['camera']
        })
    return faces_out


def process_frame(frame):
    t0 = time.time()
    frame_loaded = load_base64_frame_img(frame)
    face_locations = get_face_locations(frame_loaded)
    faces_encoding = get_faces_encoding(frame_loaded, face_locations)
    print("Loaded face encodings in {0} seconds in process {1}.".format(time.time() - t0, mp.current_process()))
    return faces_encoding


def process_worker(queue, results):
    print(os.getpid(), "PROCESSOR: working")
    while True:
        msg = queue.get(True)
        print(os.getpid(), "PROCESSOR: got a new frame")
        faces = process_frame(msg)
        results.put(faces)


def results_sender(queue):
    print(os.getpid(), "SENDER: working")
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        client_id="detector",
        max_request_size=2147483647
    )
    print(os.getpid(), "SENDER: connected to kafka")
    while True:
        faces = queue.get(True)
        print(os.getpid(), "SENDER: got new faces", len(faces))
        for face in faces:
            face_to_send = face
            encodings = [enc for enc in face['encoding']]
            face_to_send['encoding'] = encodings
            kafka_producer.send('faces', value=json.dumps(face_to_send).encode('utf-8'))


if __name__ ==  '__main__':
    num_cpus = 20
    num_cpus_senders = 1
    queueFrames = mp.Queue()
    queueResults = mp.Queue()
    poolProcessors = mp.Pool(num_cpus, process_worker, (queueFrames, queueResults,))
    print("Created processing pool of {0} CPUs.".format(num_cpus))
    poolSenders = mp.Pool(num_cpus_senders, results_sender, (queueResults, ))
    print("Created sender pool of {0} CPUs.".format(num_cpus_senders))
    consumer = KafkaConsumer('frames', bootstrap_servers=['localhost:9092'])
    for message in consumer:
        ping = time.time()
        message = message.value.decode('utf-8')
        data = json.loads(message)
        queueFrames.put(data)
    poolProcessors.join()
    poolProcessors.close()
