import re
import json
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep


class WordCounter(MRJob):

    def configure_args(self):
        super(WordCounter, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, key, value):
        

        tokens = re.findall(r"\b\w+\b", value.lower()) # Regex to find all words in the review text
        for token in tokens:
            yield token, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, value):
        yield None, (sum(value), key)

    def reducer_sort(self, _, values): #key was None
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
