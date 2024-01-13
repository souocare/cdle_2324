import re   # Regex library
import json
from mrjob.job import MRJob  # MapReduce library
from mrjob.step import MRStep
#from sentimento import sentiment_dict

WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")


class SentimentDict(MRJob):
    """Job to perform sentiment analysis on the reviews, using a dictionarity."""

    def configure_args(self):
        super(SentimentDict, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper_init(self):
        self.sentiment_dict = {
            "positive": [],
            "negative": [],
            "neutral": []
        }
        # open json file with word polarities
        with open('words_sentiment.json') as f:
            words_sentiment = json.load(f)

        """
        Each word has this values:

        "anger": 0,
        "anticipation": 0,
        "disgust": 0,
        "fear": 0,
        "joy": 0,
        "negative": 0,
        "positive": 1,
        "sadness": 0,
        "surprise": 0,
        "trust": 1

        """

        positive_scores = [
            "anticipation",
            "joy",
            "positive",
            "surprise",
            "trust"
        ]

        negative_scores = [
            "anger",
            "disgust",
            "fear",
            "negative",
            "sadness"
        ]

        

        for word, value in words_sentiment.items():
            pos, neg = 0, 0
            for score, val in value.items():
                if score in positive_scores:
                    pos += val
                elif score in negative_scores:
                    neg += val

            if pos > neg:
                self.sentiment_dict["positive"].append(word)
            elif neg > pos:
                self.sentiment_dict["negative"].append(word)
            else:
                self.sentiment_dict["neutral"].append(word)
        #self.sentiment_dict = sentiment_dict

    def mapper(self, key, value):
        """Extract words from the review text and perform sentiment analysis.
        """
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] #get only the review text
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        tokens = re.findall(WORD_RE, review_text.lower()) # Regex to find all words in the review text

        sentence_polarity = 0
        for token in tokens:
            if token in self.sentiment_dict["positive"]:
                sentence_polarity += 1
            elif token in self.sentiment_dict["negative"]:
                sentence_polarity -= 1

        if sentence_polarity > 0:
            yield "positive", 1
        elif sentence_polarity < 0:
            yield "negative", 1
        else:
            yield "neutral", 1
            
                
    def combiner(self, key, values):
        """Prepare data for the Reducer. 
            Sum all the values '1' for each key (polarity).
        """
        yield key, sum(values)
    
    def reducer(self, key, values):
        """Reduce the data. To get all words count, for the sorting step."""
        yield None, (sum(values), key)

    def reducer_final(self, _, values):
        """Calculate the percentage of each polarity.
            Yield the count and the percentage of each polarity.
        """
        total_counts = 0
        for count, key in values:
            total_counts += count
            yield key, count
        yield 'total', total_counts
        yield f"Percentage of {key}", f"{count/total_counts}%"

    def steps(self):
        """Steps to run the MapReduce job.
            The first step is to classify the sentiment of each review,
            using a dictionary and to count the polarities.
            The second step is to return polarities statistics.
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
                reducer=self.reducer_final,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]


if __name__ == '__main__':
    SentimentDict.run()