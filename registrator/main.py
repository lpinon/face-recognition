from cv2 import cv2
from kafka import KafkaProducer
import base64
import json
import os
import time
import multiprocessing as mp
import face_recognition
import numpy as np

KAFKA_TOPIC = 'face_registrations'

def frame_show(queue):
    while True:
        frame = queue.get(True)
        cv2.imshow("Registration", frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break


if __name__ ==  '__main__':
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        client_id="face_register",
        max_request_size=2147483647
    )
    camera = cv2.VideoCapture(0)
    num_cpus = 1
    queueFrames = mp.Queue()
    pool = mp.Pool(num_cpus, frame_show, (queueFrames, ))
    while True:
        input('Press enter to capture image')
        ret, frame = camera.read()
        queueFrames.put(frame)
        face_locations = face_recognition.face_locations(frame)
        if len(face_locations) == 1:
            print('User face found!')
            user_name = input('User Name: ')
            face_encoding = face_recognition.face_encodings(frame, face_locations)
            if len(face_encoding) == 0:
                print("Error encoding face")
            else:
                face_encoding = face_encoding[0]
                out = {
                    'username': user_name,
                    'encoding': [enc for enc in face_encoding]
                }
                kafka_producer.send(KAFKA_TOPIC, value=json.dumps(out).encode('utf-8'))
        elif len(face_locations) == 0:
            print("Face not found")
        else:
            print("More than one faces found")


    camera.release()
    cv2.destroyAllWindows()

