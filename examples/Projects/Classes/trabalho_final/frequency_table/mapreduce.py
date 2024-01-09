import re
from mrjob.job import MRJob
from mrjob.step import MRStep

class WordFrequencyTable(MRJob):

    def configure_args(self):
        super(WordFrequencyTable, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, line):
        tokens = re.findall(r"\b\w+\b", line)
        for token in tokens:
            yield token.lower(), 1  # Convert to lowercase for case-insensitive counting

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield None, (key, sum(values))

    def reducer_sort(self, _, word_count_pairs):
        frequency_table = {word: count for word, count in word_count_pairs}
        yield "Frequency Table:", frequency_table

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_sort
            )
        ]

if __name__ == '__main__':
    WordFrequencyTable.run()
