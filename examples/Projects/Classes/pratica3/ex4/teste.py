import cv2
import numpy as np

def read_file(file_path):


    # Implement video file reading here
    # Return (key, value) for each frame in the format (frame_number, image_frame_bytes)
    video_capture = cv2.VideoCapture(file_path)
    print(type(video_capture))

    frame_number = 0
    while True:
        success, frame = video_capture.read()
        print(success)
        print(type(frame))
        print(frame)
        
        if not success:
            break

        # Encode the image as bytes to be used as value
        _, buffer = cv2.imencode('.jpg', frame)
        print(type(_))
        print(type(buffer))
        print(buffer)
        print(len(buffer))
        #cls.log.info(_)
        #cls.log.info(buffer)
        image_bytes = buffer.tobytes()
        print(type(image_bytes))
        

        frame_number += 1
        print(frame_number)
        print(len(image_bytes))
        break

    face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
        
    # Convert the bytes to a numpy array
    image_array = np.frombuffer(image_bytes, dtype=np.uint8)
    frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

    # Apply the cascade classifier to detect faces
    #gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(frame, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

    print(faces)

read_file('/Users/sofiafernandes/Documents/Repos/MEIM-ano1-sem2/cdle_2324/examples/input/facedetect/video.mp4')