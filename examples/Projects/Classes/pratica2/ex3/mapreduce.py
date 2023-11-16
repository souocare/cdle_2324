'''
 # @ Description:
 
3. Modifique o exemplo de modo que possa ser utilizado para contar n-gramas de qualquer dimensão (considere que um n-grama é 
uma sequência de n palavras separadas por um espaço). Para esta modificação considere que o valor de n é passado como 
argumento/configuração na execução da aplicação MapReduce.

Os custom_counters são contadores personalizados que você pode definir e usar em um job MapReduce para acompanhar métricas 
específicas ou eventos durante o processamento dos dados. Eles são úteis para coletar informações adicionais sobre o desempenho 
ou comportamento de um job. Vamos explicar cada um dos contadores usados no código:

mapper_calls: Este contador é incrementado cada vez que a função mapper é chamada. 
                Ele ajuda a rastrear quantas vezes a função map está sendo executada.

words_processed: Este contador é incrementado para cada palavra processada pela função mapper. 
                    Ele rastreia o número total de palavras processadas durante o job.

combiner_calls: Este contador é incrementado cada vez que a função combiner é chamada. 
                    Ele registra quantas vezes o combiner está sendo executado.

reducer_calls: Este contador é incrementado cada vez que a função reducer é chamada. 
                Ele rastreia o número de chamadas da função reduce.

ngrams_processed: Este contador é incrementado para cada n-grama processado pela função mapper no exercício 3. 
                    Ele registra o número total de n-gramas processados durante o job.

Esses contadores fornecem insights adicionais sobre o comportamento do job MapReduce e podem ser visualizados nas interfaces 
de monitoramento do Hadoop, como a interface web do ResourceManager. Eles são úteis para diagnóstico, otimização e análise de 
desempenho durante a execução de tarefas MapReduce.
 '''

from mrjob.job import MRJob
from itertools import combinations

class NGramCount(MRJob):

    def configure_args(self):
        super(NGramCount, self).configure_args()
        # Configurar argumentos passados na linha de comando
        self.add_passthru_arg('--output', default='output')
        self.add_passthru_arg('--input', default='input')
        self.add_passthru_arg('--num-reducers', default=2, type=int)
        # Adicionar argumento para a dimensão do n-grama
        self.add_passthru_arg('--n', default=2, type=int)

    def mapper(self, _, line):
        # Função map: contar n-gramas e usar mecanismos de logging
        self.increment_counter('custom_counters', 'mapper_calls', 1)
        words = line.split()
        for ngram in combinations(words, self.options.n):
            self.increment_counter('custom_counters', 'ngrams_processed', 1)
            yield ' '.join(ngram), 1

    def combiner(self, ngram, counts):
        # Combiner: combinar contagens antes do shuffle
        self.increment_counter('custom_counters', 'combiner_calls', 1)
        yield ngram, sum(counts)

    def reducer(self, ngram, counts):
        # Função reduce: somar contagens finais e usar mecanismos de logging
        self.increment_counter('custom_counters', 'reducer_calls', 1)
        yield ngram, sum(counts)

if __name__ == '__main__':
    # Executar o job com diferentes configurações
    NGramCount.run()

