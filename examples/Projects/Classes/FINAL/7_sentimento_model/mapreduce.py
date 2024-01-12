import re
import json
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Download the VADER lexicon (if not already downloaded)
nltk.download('vader_lexicon')


class SentimentAnalysis(MRJob):

    def configure_args(self):
        super(SentimentAnalysis, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")

    def mapper_init(self): 
        # Create a SentimentIntensityAnalyzer
        self.sid = SentimentIntensityAnalyzer()      


    def mapper(self, key, value):

        sent = self.sid.polarity_scores(value)['compound'] # Get the sentiment score for the review 
        if sent > 0.5:
            yield 'positive', 1
        elif sent < 0.5:
            yield 'neutral', 1
        else:
            yield 'negative', 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, value):
        yield None, (sum(value), key)

    def reducer_final(self, _, values):
        total_counts = 0
        for count, key in values:
            total_counts += count
            yield key, count

        yield 'total', total_counts
        yield f"Percentage of {key}", f"{count/total_counts}%"

        

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper, 
                combiner = self.combiner,
                reducer=self.reducer,
                #jobconf={
                #    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                #}
            ),
            MRStep(
                reducer=self.reducer_final,
                #jobconf={
                #    'mapreduce.output.fileoutputformat.compress': 'true',
                #    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                #}
            )
        ]
    
if __name__ == '__main__':
    SentimentAnalysis.run()
