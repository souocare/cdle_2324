'''
 # @ Description:

2. Considere um tipo específico de ficheiros de arquivo (em formato ZIP), onde cada ficheiro é constituído por um conjunto de registos, onde cada registo representa um artigo da Wikipédia, O formato de cada registo segue o seguinte formato:
a) 1a linha descritor do registo, na seguinte forma: <Número de palavras do artigo><TAB><Nome do Artigo>
b) 2a linha o texto do artigo contido numa única linha
3. Desenvolva uma classe derivada de FileInputFormat, e o respetivo RecordReader, e utilize estas duas classes no exemplo de contagem 
de palavras (Ex10-WordCount-01 ou Ex11-WordCount-02) de modo a poder contar as palavras que existem em nestes ficheiros de arquivo.
 '''

from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep
import mrjob

class WikiFileInputFormat(mrjob.input.FileInputFormat):
    def read_file(cls, file_path):
        # Implementa a leitura específica do formato ZIP aqui
        # Retorna (key, value) para cada registro no formato (numero_palavras, nome_artigo, texto_artigo)
        pass

class MRWordCountInWiki(MRJob):

    INPUT_PROTOCOL = WikiFileInputFormat
    INTERNAL_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, record):
        # Separar os campos do registro
        num_words, article_name, article_text = record.split('\t')

        # Dividir o texto em palavras
        words = article_text.split()

        # Emitir pares (palavra, 1) para cada palavra no artigo
        for word in words:
            yield word.lower(), 1

    def combiner(self, word, counts):
        # Somar as contagens parciais localmente no combiner
        yield word, sum(counts)

    def reducer(self, word, counts):
        # Somar as contagens finais no reducer
        yield word, sum(counts)

if __name__ == '__main__':
    MRWordCountInWiki.run()



