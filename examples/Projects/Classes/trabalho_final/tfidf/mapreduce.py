import re
from mrjob.job import MRJob
from mrjob.step import MRStep
import math

class WordTFIDF(MRJob):

    def configure_args(self):
        super(WordTFIDF, self).configure_args()
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")

    def mapper_tf(self, _, line):
        tokens = re.findall(r"\b\w+\b", line)
        total_tokens = len(tokens)
        term_frequencies = {}
        for token in tokens:
            term_frequencies[token.lower()] = term_frequencies.get(token.lower(), 0) + 1

        for term, count in term_frequencies.items():
            yield term, (count / total_tokens, 1)  # Emit (term, TF, 1) for each term

    def combiner_tf(self, term, tf_counts):
        total_tf = 0
        total_docs = 0
        for tf, _ in tf_counts:
            total_tf += tf
            total_docs += 1

        yield term, (total_tf, total_docs)

    def reducer_tf(self, term, tf_counts):
        total_tf = 0
        total_docs = 0
        for tf, doc_count in tf_counts:
            total_tf += tf
            total_docs += doc_count

        yield term, (total_tf, total_docs)

    def mapper_idf(self, term, tf_doc_count):
        tf, doc_count = tf_doc_count
        yield None, (term, tf * math.log(1 + self.options.total_docs / (1 + doc_count)))  # Emit (None, (term, TF-IDF))

    def reducer_idf(self, _, term_tfidf_pairs):
        for term, tfidf in term_tfidf_pairs:
            yield term, tfidf

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_tf,
                combiner=self.combiner_tf,
                reducer=self.reducer_tf
            ),
            MRStep(
                mapper=self.mapper_idf,
                reducer=self.reducer_idf,
                jobconf={
                    'mapreduce.job.reduces': 1  # Set the number of reducers for the final step
                }
            )
        ]

if __name__ == '__main__':
    WordTFIDF.run()
