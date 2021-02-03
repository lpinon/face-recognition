import threading
import os
import time
import json
import base64

try:
    from cv2 import cv2
except ImportError:
    pass


class AsyncFramesWriter(threading.Thread):
    def __init__(self, kafka_producer, kafka_topic, queue, size, base_dir, camera_id):
        threading.Thread.__init__(self)
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic
        self.queue = queue
        self.size = size
        self.base_dir = base_dir
        self.camera_id = camera_id

    def run(self):
        # out_name = '{0}_{1}.json'.format(self.camera_id, str(time.time()))
        # out_file_path = os.path.join(self.base_dir, out_name)

        frames_to_write = []
        for _ in range(self.size):
            frame_obj = self.queue.popleft()
            t = frame_obj['time']
            frame_raw = frame_obj['frame']
            _1, buffer = cv2.imencode('.jpg', frame_raw)
            bytes64_encoded = base64.b64encode(buffer)
            bytes64_string = bytes64_encoded.decode('utf-8')
            frames_to_write.append({
                'time': t,
                'frame': bytes64_string
            })

        # output_file = {
        #    'camera': self.camera_id,
        #    'frames': frames_to_write
        # }

        output_message = {
            'camera': self.camera_id,
            'frames': frames_to_write
        }

        # f = open(out_file_path, "wb")
        # f.write(json.dumps(output_file).encode('utf-8'))
        # f.close()
        self.kafka_producer.send(self.kafka_topic, value=json.dumps(output_message).encode('utf-8'))