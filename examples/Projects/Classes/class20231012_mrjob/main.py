import re
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import os

class WordCounter(MRJob):

    def configure_args(self):
        super(WordCounter, self).configure_args()
        self.add_file_arg('--input', help='Input data path')
        self.add_file_arg('--output', help='Output data path')
        self.add_file_arg('--num-reducers', help='Number of reducers')
        self.add_file_arg('--compression-codec', help='Compression codec (e.g., gzip)')

    def load_args(self, args):
        super(WordCounter, self).load_args(args)

        # You can access the argument values using self.options
        self.input_path = self.options.input
        self.output_path = self.options.output
        self.num_reducers = int(self.options.num_reducers)
        self.compression_codec = self.options.compression_codec

    def mapper(self, key, value):
        for line in value:
            line = line.strip()
            tokens = re.findall(r"\b\w+\b", line)
            for token in tokens:
                yield token, 1

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort(self, key, values):
        for count, key in sorted(values):
            yield count, key

    def combiner(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
                jobconf={
                    'mapreduce.job.reduces': self.num_reducers  # Set the number of reducers
                }
            ),
            MRStep(
                reducer=self.reducer_sort,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': self.compression_codec  # Set compression codec
                }
            )
        ]

if __name__ == '__main__':
    WordCounter.run()
