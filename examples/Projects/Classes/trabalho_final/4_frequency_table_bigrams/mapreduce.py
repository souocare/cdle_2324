import re
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")

class BigramFrequencyTable(MRJob):

    def configure_args(self):
        super(BigramFrequencyTable, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, value):
        review = json.loads(value) # loads instead of load to load just a string
        review_text = review['reviewText'] #get only the review text
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        #tokens = re.findall(r"\b\w+\b", review_text.lower()) # Regex to find all words in the review text
        tokens = re.findall(WORD_RE, review_text.lower())
        # yield bigrams
        for i in range(len(tokens)-1):
            yield f"{tokens[i]} {tokens[i+1]}", 1


    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def reducer_sorter(self, key, values):
        for count, key in sorted(values):
            yield count, key

    def steps(self):
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
    BigramFrequencyTable.run()
