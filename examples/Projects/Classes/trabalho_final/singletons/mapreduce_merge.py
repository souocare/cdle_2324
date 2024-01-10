import re
from mrjob.job import MRJob
from mrjob.step import MRStep
from frequency_table import WordFrequencyTable

class SingletonWords(MRJob):

    def configure_args(self):
        super(SingletonWords, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, frequency_table):
        #tokens = re.findall(r"\b\w+\b", line)
        #for token in tokens:
        #    yield token.lower(), 1  # Convert to lowercase for case-insensitive counting
        frequency_table = eval(frequency_table)
        # get total number of words
        total_words = sum(frequency_table.values())
        for word, count in frequency_table.items():
            yield total_words (word, count)
    

    def reducer(self, total_words, word_counts):
        # get percentage of singletons
        singletons = []
        for word, count in word_counts:
            if count == 1:
                singletons.append(word)
                yield "Singleton Word:", word
                
        yield "Percentage of Singletons:", len(singletons) / total_words * 100

    def steps(self):
        return [
            MRStep(
                mapper=WordFrequencyTable.mapper,
                combiner=WordFrequencyTable.combiner,
                reducer=WordFrequencyTable.reducer
            ),
            MRStep(
                reducer=WordFrequencyTable.reducer_sort
            ),
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    SingletonWords.run()
