'''
 # @ Description:

2. No contexto do exemplo MapReduce de Contagem de palavras (Ex10-WordCount-01) execute diferentes execuções modificando:
a) O input e o output de dados de modo que o mesmo possa ser local (file://) ou no sistema de ficheiros distribuído (hdfs://).
b) O número de reducers.
c) Modifique o exemplo para poder reconfigurar os dados da aplicação utilizando configurações passadas na linha de comando. 
Neste contexto modifique por exemplo os codecs de compressão utilizado para guardar o resultado do processamento.
 '''


from mrjob.job import MRJob
from mrjob.step import MRStep

# python mapreduce.py -r local --input local_input.txt --output local_output
# python mapreduce.py -r hadoop --input hdfs:///user/input.txt --output hdfs:///user/output --num-reducers 3


class WordCount(MRJob):

    def configure_args(self):
        super(WordCount, self).configure_args()
        self.add_passthru_arg('--output', default='output')
        self.add_passthru_arg('--input', default='input')
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")


    def mapper(self, _, line):
        for word in line.split():
            yield word, 1

    def reducer(self, word, counts):

        yield word, sum(counts)


    def reducer_sort(self, key, values):
        for count, key in sorted(values):
            yield count, key

    def combiner(self, key, values):
        yield key, sum(values)

    def steps(self):
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
                reducer=self.reducer_sort,
                jobconf={
                    'mapreduce.output.fileoutputformat.compress': 'true',
                    'mapreduce.output.fileoutputformat.compress.codec': "org.apache.hadoop.io.compress." + self.options.compressioncodec  # Set compression codec
                }
            )
        ]
    
if __name__ == '__main__':
    WordCount.run()
