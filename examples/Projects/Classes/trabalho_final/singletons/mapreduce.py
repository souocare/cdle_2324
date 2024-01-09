import re
from mrjob.job import MRJob
from mrjob.step import MRStep

class SingletonWords(MRJob):

    def configure_args(self):
        super(SingletonWords, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, line):
        tokens = re.findall(r"\b\w+\b", line)
        for token in tokens:
            yield token.lower(), 1  # Convert to lowercase for case-insensitive counting

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
