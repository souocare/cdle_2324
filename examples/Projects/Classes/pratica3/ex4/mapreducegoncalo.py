from mrjob.job import MRJob
from mrjob.protocol import BytesValueProtocol, RawValueProtocol, BytesProtocol, TextValueProtocol, RawProtocol, TextProtocol
import cv2
import numpy as np
import sys
import time

class VideoFileInputFormat(RawValueProtocol):

    def read(cls, file_path_bytes):
        # Convert the bytes-like object to a string, ignoring errors
        file_path = file_path_bytes.decode('utf-8', errors='replace')

        # Extract the actual file path from the URL-like path
        #file_path = file_path.replace('file://', '')
        with open('/home/usermr/examples/Projects/Classes/pratica3/ex4/output.txt', 'wb') as file:
            # Write content to the file
            file.write(file_path_bytes)
        
        # with open(file_path, 'rb') as video_file:
        #     video_content = video_file.read()

        time.sleep(100)

        # time.sleep(100)
        video_capture = cv2.VideoCapture(file_path)
        # Implement video file reading
        # Return (key, value) for each frame in the format (frame_number, image_frame_bytes)
        #video_capture = cv2.VideoCapture(str(file_path))

        record = np.array([])

        frame_number = 0
        while True:
            success, frame = video_capture.read()
            if not success:
                break

            frame_number += 1
            record.append(frame)
            #yield (frame_number, frame.tobytes())

        return None, record.tobytes()
    



class FaceDetector(MRJob):

    INPUT_PROTOCOL = VideoFileInputFormat
    INTERNAL_PROTOCOL = RawValueProtocol 

    def mapper_init(self):
        # Load the cascade classifier at the beginning of the mapper
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")

        


    def mapper(self, _, record):
            
        # convert the record to a numpy array from
        #record_array = np.array(record)
        # Convert the bytes to a numpy array
        #buffer_img = cv2.imencode('.jpg', image_bytes)
        #image_array = np.frombuffer(buffer_img)
        #frame = cv2.imdecode(image_array) #, cv2.IMREAD_COLOR)
        # Convert the bytes to a numpy array
        record_buffer = np.frombuffer(record, dtype=np.uint8)

        #record_array = record_buffer.reshape((record_buffer.shape[0], record_buffer.shape[1], record_buffer.shape[2])) 

        frame_number = 0

        record_array = np.array(record_buffer)
        for frame in record_array:
            #frame_buffer = np.frombuffer(frame_bytes, dtype=np.uint8)
            #frame = cv2.imdecode(frame_buffer, cv2.IMREAD_COLOR)
            frame_number += 1

            yield (frame_number, frame)

        

    def reducer(self, frame_number, frame):

        count_faces = 0
        
        faces = self.face_cascade.detectMultiScale(frame, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

        for face in faces:
            count_faces += 1

        #percentage = 100 * count_faces / count_frames

        yield (frame_number, count_faces)
       

    
if __name__ == "__main__":
    FaceDetector.run()
