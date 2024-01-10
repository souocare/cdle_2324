# Mapping: Hello World this is my review and it is awesome

# Reducer vai juntar tudo pela key e somar as contagens

import re   # Regex library
import json
from mrjob.job import MRJob  # MapReduce library
from mrjob.step import MRStep

class WordCounter(MRJob):

    def configure_args(self):
        super(WordCounter, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")



    def mapper(self, key, value):
        """Instance of a Mapper

        Args:
            key (_type_): _description_
            value (_type_): _description_

        Yields:
            _type_: _description_
        """
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] #get only the review text

        tokens = re.findall(r"\b\w+\b", review_text.lower()) # Regex to find all words in the review text

        for token in tokens:
            yield token, 1 # because MRJob uses generators, we use yield instead of return

    
    def combiner(self, key, values):
        """Prepare data for the Reducer

        Args:
            key (_type_): _description_
            values (_type_): _description_

        Yields:
            _type_: _description_
        """

        yield key, sum(values)
    
    def reducer(self, key, values):
        """Instance of a Reducer

        Args:
            key (_type_): _description_
            values (_type_): _description_

        Yields:
            _type_: _description_
        """
        
        # to order by alphabetical order
        #yield key, sum(values)
        
        # to order by count of words
        yield None, (sum(values), key)

    def reducer_sorter(self, key, values):

        for count, key in sorted(values):
            yield count, key

    def steps(self):
        """Steps to run the MapReduce

        Returns:
            _type_: _description_
        """
        # We don't call the functions we just pass them
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
                ),
            MRStep(
                reducer=self.reducer_sorter
                )
        ]

if __name__ == '__main__':
    WordCounter.run()