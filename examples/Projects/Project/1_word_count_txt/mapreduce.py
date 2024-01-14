import re
from mrjob.job import MRJob
from mrjob.step import MRStep


class WordCounter(MRJob):
    """Job to count the number of words in a text file.
        Also, it sorts the words by frequency.
    """

    def configure_args(self):
        super(WordCounter, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, value):
        """Uses regex pattern to find all words in the text.
            It doesn't clean the text from digits, but the regex pattern
            used detects automatically words with digits as one word.
        """
        # get words
        tokens = re.findall(r"\b\w+\b", value.lower()) # Regex to find all words in the line
        for token in tokens:
            yield token, 1

    def combiner(self, key, values):
        """Prepare data for the Reducer. 
            Sum all the values '1' for each key (word).
        """
        yield key, sum(values)

    def reducer(self, key, value):
        """Reduce the data. To get all words count, for the sorting step."""
        yield None, (sum(value), key)

    def reducer_sort(self, _, values): #key was None
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
                reducer=self.reducer_sort,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]
    
if __name__ == '__main__':
    WordCounter.run()
