from mrjob.job import MRJob
import cv2
import numpy as np
import io
import time
import sys

class VideoProcessingJob(MRJob):

    def mapper_init(self):
        # Load the pre-trained face cascade classifier
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

        # Open the video file
        self.cap = cv2.VideoCapture("/home/usermr/examples/input/facedetect/faces.mp4")

    def mapper(self, _, line):
        # Process each frame of the video
        loopcount = 0
        nnnt = 0
        framesss = True
        while framesss:
            ret, frame = self.cap.read()
            loopcount += 1
            if not ret or loopcount > 200:
                framesss = False
            

            with open('/home/usermr/examples/Projects/Classes/pratica3/ex4/output.txt', 'wb') as file:
                # Write content to the file
                file.write(str.encode(str(loopcount)))
            
            try:

                # Convert the frame to grayscale for face detection
                gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

                # Detect faces in the frame
                faces = self.face_cascade.detectMultiScale(gray_frame, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

                # Count the number of faces detected in the frame
                num_faces = len(faces)

                # Emit the number of faces as the key and information about face detection as the value
                yield f"faces_{num_faces}", {'has_face': num_faces > 0}
            except:
                with open('/home/usermr/examples/Projects/Classes/pratica3/ex4/outputbreak.txt', 'wb') as file:
                # Write content to the file
                    file.write(str.encode(str("break")))
                break

        # After processing all frames, yield a special key to indicate that processing is done
        with open('/home/usermr/examples/Projects/Classes/pratica3/ex4/outputdone.txt', 'wb') as file:
            # Write content to the file
                file.write(str.encode(str("done")))
        cap.release()
        yield 'processing_done', None

    def mapper_final(self):
        # Release the video capture object here
        self.cap.release()

    
    def reducer(self, key, values):
        
        count_no_faces = 0
        count_with_faces = 0

        for value in values:
            # Check if any frame has a detected face
            if value.get('has_face', False):
                count_with_faces += 1
            else:
                count_no_faces += 1

        # Save the results to a file
        with open('/home/usermr/examples/Projects/Classes/pratica3/ex4/output_results.txt', 'a') as file:
            file.write(f"{key}\tCount No Faces: {count_no_faces}\tCount With Faces: {count_with_faces}\n")

if __name__ == '__main__':
    VideoProcessingJob.run()
