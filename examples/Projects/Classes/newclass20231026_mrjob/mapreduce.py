from mrjob.job import MRJob
import cv2

class FaceDetectionJob(MRJob):
    def configure_args(self):
        super(FaceDetectionJob, self).configure_args()
        self.add_file_arg('--haarcascade', help='Path to Haar Cascade XML file for face detection')
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        #self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")

    def mapper_init(self):
        self.face_cascade = cv2.CascadeClassifier(self.options.haarcascade)

    def mapper(self, _, line):
        image_path = line.strip()
        image = cv2.imread(image_path)
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        faces = self.face_cascade.detectMultiScale(gray, scaleFactor=1.3, minNeighbors=5, minSize=(30, 30))

        for (x, y, w, h) in faces:
            yield None, (image_path, (x, y, x + w, y + h))

    def reducer(self, _, values):
        for value in values:
            yield None, value

def steps(self):
    return [
        self.mr(
            mapper_init=self.mapper_init, 
            mapper=self.mapper,
            reducer=self.reducer,
            jobconf={
                    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                }
                )
    ]

if __name__ == '__main__':
    FaceDetectionJob.run()
