import re  
import json
from mrjob.job import MRJob  
from mrjob.step import MRStep


WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")

class WordCounterClean(MRJob):
    """Job to count the number of words of a json dataset called
        'Amazon Musical Instruments Reviews', available in 
        https://www.kaggle.com/datasets/eswarchandt/amazon-music-reviews.
        Also, it sorts the words by frequency.
    """
    
    def configure_args(self):
        super(WordCounterClean, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, value):
        """Uses regex pattern to find all words in the json datatset field 'reviewText'.
            It cleans the text from digits and punctuation.
        """
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] # get only the review text
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        # get words
        tokens = re.findall(WORD_RE, review_text.lower())
        for token in tokens:
            yield token, 1 

    
    def combiner(self, key, values):
        """Prepare data for the Reducer. 
            Sum all the values '1' for each key (word).
        """
        yield key, sum(values)
    
    def reducer(self, key, values):
        """Reduce the data. To get all words count, for the sorting step."""
        yield None, (sum(values), key)

    def reducer_sorter(self, _, values):
        """Sort the words by frequency.
            The ouput will the the word counts, 
            starting with the less frequent words (singletons).
        """
        for count, key in sorted(values):
            yield count, key

    def steps(self):
        """Steps to run the MapReduce job.
            The first step is to count the words.
            The second step is to sort the words by frequency.
        """
        return [
            MRStep(
                mapper=self.mapper, 
                combiner = self.combiner,
                reducer=self.reducer,
                jobconf={
                    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                }
            ),
            MRStep(
                reducer=self.reducer_sorter,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]


if __name__ == '__main__':
    WordCounterClean.run()