from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import math

class WordTFIDF(MRJob):

    def mapper(self, _, line):
        tokens = re.findall(r"\b\w+\b", line)
        total_tokens = len(tokens)

        term_frequencies = {}
        for token in tokens:
            term_frequencies[token.lower()] = term_frequencies.get(token.lower(), 0) + 1

        for term, term_count in term_frequencies.items():
            yield term, (term_count, total_tokens, 1)

    def combiner(self, term, term_info):
        term_count, total_tokens, doc_count = term_info
        term_tf = sum([c/t for c, t in zip(term_count, total_tokens)])
        term_df = sum(doc_count)
        yield None, (term, term_tf, term_df)
        

    def reducer(self, _, terms_tf_df):
        terms, terms_tf, terms_df = terms_tf_df
        total_docs = sum(terms_df)
        for term, term_tf, term_df in zip(terms, terms_tf, terms_df):
            term_idf = math.log(total_docs/term_df)
            term_tfidf = term_tf * term_idf

            yield term, term_tfidf


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
                jobconf={
                    'mapreduce.job.reduces': 1  # Set the number of reducers for the final step
                }
            )
        ]

if __name__ == '__main__':
    WordTFIDF.run()
