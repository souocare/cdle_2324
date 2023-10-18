import re
import json

from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCounter(MRJob):

    def mapper(self, key, value):
        #value = value.decode('utf-8')  # Decode bytes to utf-8 string
        
        for line in value:
            line = line.strip()
            tokens = re.findall(r"\b\w+\b", line)
            for token in tokens:
                yield token, 1

    def reducer(self, key, value):
        yield None, (sum(value), key)

    def reducer_sort(self, key, values):
        for count, key in sorted(values):
            yield count, key

    def combiner(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper, 
                combiner = self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_sort
            )
        ]
    
if __name__ == '__main__':
    WordCounter.run()
