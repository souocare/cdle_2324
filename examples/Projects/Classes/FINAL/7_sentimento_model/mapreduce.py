
from mrjob.job import MRJob
from mrjob.step import MRStep

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer


class SentimentAnalysis(MRJob):
    """Job to perform sentiment analysis on the reviews, using a NLTK model."""

    def configure_options(self):
        super(SentimentAnalysis, self).configure_options()
        self.add_file_arg('--nltk-archive', help='Path to NLTK archive')

    def configure_args(self):
        super(SentimentAnalysis, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")
        self.add_file_arg('--nltk-archive')

    def mapper_init(self): 
        """Initialize the mapper, by initializing the SentimentIntensityAnalyzer."""
        # Download the VADER lexicon (if not already downloaded)
        nltk.download('vader_lexicon')
        # Create a SentimentIntensityAnalyzer
        self.sid = SentimentIntensityAnalyzer()     
        # Unpack NLTK archive and set it up in the mapper's initialization
        nltk_archive = self.options.nltk_archive
        if nltk_archive:
            # Unpack NLTK archive or perform any setup needed
            pass 


    def mapper(self, key, value):
        """Perform sentiment analysis on the review text.
            Yield the sentiment and the count of 1.
        """
        sent = self.sid.polarity_scores(value)['compound'] # Get the sentiment score for the review 
        if sent > 0.5:
            pol = 'positive'
        else:
            pol = 'negative'

        yield pol, 1

    def combiner(self, key, values):
        """Prepare data for the Reducer. 
            Sum all the values '1' for each key (polarity).
        """
        yield key, sum(values)

    def reducer(self, key, value):
        """Reduce the data. To get all polarities count."""
        yield None, (sum(value), key)

    def reducer_final(self, _, values):
        """Calculate the percentage of each polarity.
            Yield the count and the percentage of each polarity.
        """
        total_counts = 0
        for count, key in values:
            total_counts += count
            yield key, count
        yield 'total', total_counts
        yield f"Percentage of {key}", f"{count/total_counts*100}%"

        

    def steps(self):
        """Steps to run the MapReduce job.
            The first step is to classify the sentiment of each review
             and to count the polarities.
            The second step is to return polarities statistics.
        """
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper, 
                combiner = self.combiner,
                reducer=self.reducer,
                jobconf={
                    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                }
            ),
            MRStep(
                reducer=self.reducer_final,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]
    
if __name__ == '__main__':
    SentimentAnalysis.run()
