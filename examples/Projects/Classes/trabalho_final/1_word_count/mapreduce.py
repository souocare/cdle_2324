# Mapping: Hello World this is my review and it is awesome

# Reducer vai juntar tudo pela key e somar as contagens

import re   # Regex library
import json
from mrjob.job import MRJob  # MapReduce library
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol, BytesProtocol, BytesValueProtocol

WORD_RE = re.compile(r"\b\w+\b")#re.compile(r"[\w']+")

class WordCounter(MRJob):
    INPUT_PROTOCOL = BytesValueProtocol
    #OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, key, value):
        """Instance of a Mapper

        Args:
            key (_type_): _description_
            value (_type_): _description_

        Yields:
            _type_: _description_
        """
        value = value.decode('utf_8')
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] #get only the review text
    
        #tokens = re.findall(r"\b\w+\b", review_text.lower()) # Regex to find all words in the review text
        tokens = re.findall(WORD_RE, review_text.lower())
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
        return [
            MRStep(
                mapper=self.mapper, 
                combiner = self.combiner,
                reducer=self.reducer,
                #jobconf={
                #    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                #}
            ),
            MRStep(
                reducer=self.reducer_sort,
                #jobconf={
                #    'mapreduce.output.fileoutputformat.compress': 'true',
                #    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                #}
            )
        ]

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