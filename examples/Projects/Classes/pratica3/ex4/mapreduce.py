from mrjob.job import MRJob, MRStep
from mrjob.protocol import BytesValueProtocol, RawValueProtocol, BytesProtocol
import cv2
import numpy as np

class VideoFileInputFormat(BytesValueProtocol):
    def read(cls, file_path):

      
        # Implement video file reading
        # Return (key, value) for each frame in the format (frame_number, image_frame_bytes)
        video_capture = cv2.VideoCapture(file_path, 0)

        frame_number = 0
        while True:
            success, frame = video_capture.read()
            if not success:
                break

            # Encode the image as bytes to be used as value
            # _, buffer = cv2.imencode('.jpg', frame)
            # image_bytes = buffer.tobytes()
            image_bytes = frame.tobytes()
            
            frame_number += 1
            yield (None, image_bytes)
            

class MROpenCVVideoProcessing(MRJob):
    INPUT_PROTOCOL = VideoFileInputFormat

    def configure_args(self):
        super(MROpenCVVideoProcessing, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")
        self.add_passthru_arg('--cascade_classifier', default='haarcascade_frontalface_default.xml')

    def mapper_init(self):
        # Load the cascade classifier at the beginning of the mapper
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + self.options.cascade_classifier)

    def mapper(self, _, record):

        #log.info('Record: %s' % str(record))

        # Separate the fields of the record
        #frame_number, image_bytes = record
        image_bytes = record.decode('utf_8')
        # self.increment_counter('group', 'frames', 1)

        # Convert the bytes to a numpy array
        image_array = np.frombuffer(image_bytes, dtype=np.uint8)
        frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

        # Apply the cascade classifier to detect faces
        #gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = self.face_cascade.detectMultiScale(frame, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

        # Emit results (in this case, just the number of detected faces)
        yield _, str(len(faces))

    def combiner(self, key, values):
        # Combiner para otimizar a transferência de dados entre o Mapper e o Reducer
        # Simplesmente soma os valores (número de faces) para a mesma chave (frame_number)
        yield key, sum(map(int, values))

    def reducer(self, key, values):
        # Reducer principal que soma os valores (número de faces) para a mesma chave (frame_number)
        yield key, str(sum(map(int, values)))

    def reducer_sort(self, key, values):
        # Reducer final que classifica os resultados antes da saída
        for count, key in sorted(zip(map(int, values), key)):
            yield key, str(count)

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
                jobconf={
                    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                }
            ),
            
            MRStep(
                reducer=self.reducer_sort,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]

if __name__ == '__main__':
    MROpenCVVideoProcessing.run()
