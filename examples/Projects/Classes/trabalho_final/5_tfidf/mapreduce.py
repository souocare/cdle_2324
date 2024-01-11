'''
 # @ Description: Word counter but exclude typos and stopwords
 '''



import re   # Regex library
import json
from mrjob.job import MRJob  # MapReduce library
from mrjob.step import MRStep
import math


WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")

class WordTFIDF(MRJob):

    def mapper_init(self):      
        self.df_dict = {}
        self.tf_dict = {}
        self.terms_idf = None
        self.terms_tf_idf = None



    def mapper_tf(self, _, value):
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
        review_id = review['reviewerID']
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        #tokens = re.findall(r"\b\w+\b", review_text.lower()) # Regex to find all words in the review text
        tokens = re.findall(WORD_RE, review_text.lower())
        for token in tokens:
            #yield (review_id, token), 1/len(tokens) # because MRJob uses generators, we use yield instead of return
            yield f"{review_id} {token}", 1/len(tokens) 
    
    def combiner_tf(self, key, values):
        """Prepare data for the Reducer

        Args:
            key (_type_): _description_
            values (_type_): _description_

        Yields:
            _type_: _description_
        """

        yield key, sum(values)
    
    def reducer_tf(self, key, values):
        """Instance of a Reducer

        Args:
            key (_type_): _description_
            values (_type_): _description_

        Yields:
            _type_: _description_
        """
        
        yield None, (key, sum(values))
    
    def reducer_init_tfidf(self):
        self.df_dict = {}
        self.tf_dict = {}
        self.terms_idf = {}
        self.terms_tf_idf = {}

    def reducer_tfidf(self, _, values):
        """Instance of a Mapper

        Args:
            key (_type_): _description_
            value (_type_): _description_

        Yields:
            _type_: _description_
        """

        for key, value in values:
            review_id, word = key.split()
            self.df_dict[word] = self.df_dict.get(word, [])
            self.df_dict[word].append(review_id)
            self.tf_dict[word] = self.tf_dict.get(word, 0) + value

        self.terms_idf = {k: 1/(len(v)+1) for k, v in self.df_dict.items()}
        self.terms_tf_idf = {k: v*self.terms_idf[k] for k, v in self.tf_dict.items()}

        #for k, v in self.terms_tf_idf.items():
            #yield k, v
        
        # yield by order
        for k, v in sorted(self.terms_tf_idf.items(), key=lambda item: item[1], reverse=True):
            yield k, v
        


    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_tf, 
                combiner = self.combiner_tf,
                reducer=self.reducer_tf,
                #jobconf={
                #    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                #}
            ),
            MRStep(
                reducer_init=self.reducer_init_tfidf,
                reducer=self.reducer_tfidf,
                #jobconf={
                #    'mapreduce.output.fileoutputformat.compress': 'true',
                #    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                #}
            )
        ]
    

if __name__ == '__main__':
    WordTFIDF.run()