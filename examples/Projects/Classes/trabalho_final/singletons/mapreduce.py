import re
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")

class SingletonWords(MRJob):

    def configure_args(self):
        super(SingletonWords, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, value):
        """Instance of a Mapper

        Args:
            key (_type_): _description_
            value (_type_): _description_

        Yields:
            _type_: _description_
        """
        #value = value.decode('utf_8')
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] #get only the review text
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        #tokens = re.findall(r"\b\w+\b", review_text.lower()) # Regex to find all words in the review text
        tokens = re.findall(WORD_RE, review_text.lower())
        for token in tokens:
            yield token, 1 # because MRJob uses generators, we use yield instead of return

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        total_count = sum(counts)
        if total_count == 1:
            yield "Singleton Word:", word

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    SingletonWords.run()
