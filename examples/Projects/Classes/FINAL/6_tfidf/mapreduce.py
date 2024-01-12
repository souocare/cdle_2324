import re   # Regex library
import json
from mrjob.job import MRJob  # MapReduce library
from mrjob.step import MRStep

WORD_RE = re.compile(r"\b\w+\b") #re.compile(r"[\w']+")

class WordTFIDF(MRJob):
    """Job to calculate TF-IDF from the words in the reviews.
        JSON dataset 'Amazon Musical Instruments Reviews', available in 
        https://www.kaggle.com/datasets/eswarchandt/amazon-music-reviews.
    """

    def mapper_init(self):   
        """Initialize the variables to calculate TF-IDF.
            This method is called before the mapper and it's needed 
            because we can't manipulate mutable objects inside the mapper.
        """   
        self.df_dict = {}
        self.tf_dict = {}
        self.terms_idf = None
        self.terms_tf_idf = None



    def mapper_tf(self, _, value):
        
        review = json.loads(value) # loads instead of load to load just a string
        # get the review text and ID
        review_text = review['reviewText'] 
        review_id = review['reviewerID']
        # remove digits and punctuation
        review_text = re.sub(r'\d+', '', review_text) # remove digits  
        review_text = re.sub(r"[!\"#$%&()*+-./:;<=>?@\[\\\]^_`{|}~\n]" , '', review_text) # remove punctuation
        # get words
        tokens = re.findall(WORD_RE, review_text.lower())
        # yeld the TF for each word, in each review
        for token in tokens:
            yield f"{review_id} {token}", 1/len(tokens) 
    
    def combiner_tf(self, key, values):
        """Prepare data for the Reducer. 
            Sum all the values '1' for each key (doc_id word).
        """
        yield key, sum(values)
    
    def reducer_tf(self, key, values):
        """Reduce the data. To get all words TF."""
        yield None, (key, sum(values))
    
    def reducer_init_tfidf(self):
        """Initialize the variables to calculate TF-IDF.
            This method is called before the reducer and it's needed 
            because we can't manipulate mutable objects inside the reducer.
        """
        self.df_dict = {} # document frequency
        self.tf_dict = {} # term frequency
        self.terms_idf = {} # inverse document frequency
        self.terms_tf_idf = {} # TF-IDF

    def reducer_tfidf(self, _, values):
        """
        Populate the dictionaries to calculate TF-IDF.
        """
        for key, value in values:
            # get the review ID and the word
            review_id, word = key.split()
            # populate the dictionaries
            self.df_dict[word] = self.df_dict.get(word, [])
            self.df_dict[word].append(review_id)
            self.tf_dict[word] = self.tf_dict.get(word, 0) + value

        # get terms IDF
        self.terms_idf = {k: 1/(len(v)+1) for k, v in self.df_dict.items()}
        # get terms TF-IDF
        self.terms_tf_idf = {k: v*self.terms_idf[k] for k, v in self.tf_dict.items()}

        # yield by order (descending)
        for k, v in sorted(self.terms_tf_idf.items(), key=lambda item: item[1], reverse=True):
            yield k, v
        
    
    def steps(self):
        """Steps to run the MapReduce job.
            The first step is to count the words.
            The second step is to calculate TF-IDF and order the 
            terms by descending order.
        """
        return [
            MRStep(
                mapper=self.mapper_tf, 
                combiner = self.combiner_tf,
                reducer=self.reducer_tf,
                jobconf={
                    'mapreduce.job.reduces': self.options.numreducers  # Set the number of reducers
                }
            ),
            MRStep(
                reducer_init=self.reducer_init_tfidf,
                reducer=self.reducer_tfidf,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]
    

if __name__ == '__main__':
    WordTFIDF.run()