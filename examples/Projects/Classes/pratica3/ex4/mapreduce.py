from mrjob.job import MRJob
from mrjob.protocol import BytesValueProtocol
import cv2
import base64
import mrjob
import numpy as np
from mrjob.job import MRJob #FileInputFormat

class VideoFileInputFormat(MRJob):
    def read_file(cls, file_path):
        # Implement video file reading here
        # Return (key, value) for each frame in the format (frame_number, image_frame_bytes)
        video_capture = cv2.VideoCapture(file_path)

        frame_number = 0
        while True:
            success, frame = video_capture.read()
            if not success:
                break

            # Encode the image as bytes to be used as value
            _, buffer = cv2.imencode('.jpg', frame)
            image_bytes = buffer.tobytes()

            yield str(frame_number), image_bytes
            frame_number += 1

class MROpenCVVideoProcessing(MRJob):
    INPUT_PROTOCOL = VideoFileInputFormat
    INTERNAL_PROTOCOL = BytesValueProtocol
    OUTPUT_PROTOCOL = BytesValueProtocol

    def configure_args(self):
        super(MROpenCVVideoProcessing, self).configure_args()

        self.add_passthru_arg('--cascade_classifier', default='haarcascade_frontalface_default.xml')

    def mapper_init(self):
        # Load the cascade classifier at the beginning of the mapper
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + self.options.cascade_classifier)

    def mapper(self, _, record):
        # Separate the fields of the record
        frame_number, image_bytes = record

        # Convert the bytes to a numpy array
        image_array = np.frombuffer(image_bytes, dtype=np.uint8)
        frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

        # Apply the cascade classifier to detect faces
        gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = self.face_cascade.detectMultiScale(gray_frame, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

        # Emit results (in this case, just the number of detected faces)
        yield frame_number, str(len(faces))

    def reducer(self, key, values):
        # Emit the results (in this case, just the number of detected faces)
        yield key, str(sum(int(value) for value in values))


if __name__ == '__main__':
    MROpenCVVideoProcessing.run()
