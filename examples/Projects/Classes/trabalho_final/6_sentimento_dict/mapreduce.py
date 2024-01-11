'''
 # @ Description: Word counter but exclude typos and stopwords
 '''

import re   # Regex library
import json
from mrjob.job import MRJob  # MapReduce library
from mrjob.step import MRStep
#from sentimento import sentiment_dict

WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")


class SentimentDict(MRJob):


    def configure_args(self):
        super(SentimentDict, self).configure_args()
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
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        tokens = re.findall(WORD_RE, review_text.lower()) # Regex to find all words in the review text
        sentiment_dict = {
            "positive": ["good", "nice", "excelent"],
            "negative": ["bad", "terrible", "awful"],
        }

        sentence_polarity = 0
        for token in tokens:
            if token in sentiment_dict["positive"]:
                sentence_polarity += 1
            elif token in sentiment_dict["negative"]:
                sentence_polarity -= 1

        if sentence_polarity > 0:
            yield "positive", 1
        elif sentence_polarity < 0:
            yield "negative", 1
        else:
            yield "neutral", 1
            
                
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
                reducer=self.reducer_sorter,
                #jobconf={
                #    'mapreduce.output.fileoutputformat.compress': 'true',
                #    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                #}
            )
        ]


if __name__ == '__main__':
    SentimentDict.run()