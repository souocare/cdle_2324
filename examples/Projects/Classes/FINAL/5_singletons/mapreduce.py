import re
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")

class SingletonWords(MRJob):
    """Job to get the singletons of a json dataset called
        'Amazon Musical Instruments Reviews', available in 
        https://www.kaggle.com/datasets/eswarchandt/amazon-music-reviews.
        Also, it sorts the words by frequency.
    """
    
    def configure_args(self):
        super(SingletonWords, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, value):
        """Uses regex pattern to find all words in the json datatset field 'reviewText'.
            It cleans the text from digits and punctuation.
        """
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] #get only the review text
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        # get words
        tokens = re.findall(WORD_RE, review_text.lower())
        for token in tokens:
            yield token, 1 

    def combiner(self, word, counts):
        """Prepare data for the Reducer. 
            Sum all the values '1' for each key (word).
        """
        yield word, sum(counts)

    def reducer(self, key, values):
        """Reduce the data. Get all words count, 
            for the singleton identification step
        """
        yield None, (sum(values), key)

    def reducer_init(self):
        """Initialize the singletons list"""
        self.singletons = []

    def reducer_final(self, _, counts):
        """"""
        total_counts = 0
        total_uniques = 0
        for value, key in counts:
            # add total number of words
            total_counts += value 
            # add to total number of unique words
            total_uniques += 1 
            # if it is a singleton, add it to the list
            if value == 1:
                self.singletons.append(key)
        # yield the results
        yield "Percentage of singletons in all counts", f"{round(len(self.singletons)/total_counts*100, 3)}%"
        yield "Percentage of singletons in unique counts", f"{round(len(self.singletons)/total_uniques*100, 3)}%"
        for word in self.singletons:
            yield "Singleton", word

    def steps(self):
        """Steps to run the MapReduce job.
            The first step is to count the words.
            The second step is to get the singletons and the percentage of singletons.
        """
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
                jobconf={
                    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                }
            ),
            MRStep(
                reducer_init=self.reducer_init,
                reducer=self.reducer_final,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]

if __name__ == '__main__':
    SingletonWords.run()
