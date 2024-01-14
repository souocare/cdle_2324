import re
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")

class BigramFrequencyTable(MRJob):
    """Job to count the number of bigrams (co-occurrence of words) of the reviews 
        in the dataset 'Amazon Musical Instruments Reviews', available in 
        https://www.kaggle.com/datasets/eswarchandt/amazon-music-reviews.
        Also, it sorts the words by frequency.
    """

    def configure_args(self):
        super(BigramFrequencyTable, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, value):
        """Uses regex pattern to find all words in the json datatset field 'reviewText'.
            It cleans the text from digits and punctuation.
            It yields bigrams (co-occurrence of words) instead of singular tokens.
        """
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] #get only the review text
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        # get words
        tokens = re.findall(WORD_RE, review_text.lower())
        # yield bigrams as one word
        for i in range(len(tokens)-1):
            yield f"{tokens[i]} {tokens[i+1]}", 1


    def combiner(self, key, values):
        """Prepare data for the Reducer. 
            Sum all the values '1' for each key (bigram).
        """
        yield key, sum(values)

    def reducer(self, key, values):
        """Reduce the data. To get all bigrams count, for the sorting step."""
        yield None, (sum(values), key)

    def reducer_sorter(self, _, values):
        """Sort the bigrams by frequency.
            The ouput will the the bigram counts, 
            starting with the most frequent bigrams.
        """
        for count, key in sorted(values, reverse=True):
            yield count, key

    def steps(self):
        """Steps to run the MapReduce job.
            The first step is to count the bigrams.
            The second step is to sort the bigrams by frequency.
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
                reducer=self.reducer_sorter,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]

if __name__ == '__main__':
    BigramFrequencyTable.run()
