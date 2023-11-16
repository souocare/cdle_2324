'''
 # @ Description:

2. Modifique o exemplo MapReduce de contagem de palavras (Ex11-WordCount-02) de modo a perceber:
a) Influência dos Combinners no valor final dos contadores
b) Parametrização das funções map e reduce
c) Utilização dos mecanismos de logging;
 '''

from mrjob.job import MRJob

class WordCount(MRJob):

    def configure_args(self):
        super(WordCount, self).configure_args()
        # Configurar argumentos passados na linha de comando
        self.add_passthru_arg('--output', default='output')
        self.add_passthru_arg('--input', default='input')
        self.add_passthru_arg('--num-reducers', default=2, type=int)
        # Adicionar argumentos para configuração da função map e reduce
        self.add_passthru_arg('--mapper-param', default='param1')
        self.add_passthru_arg('--reducer-param', default='param2')

    def mapper(self, _, line):
        # Função map: contar palavras e usar mecanismos de logging
        self.increment_counter('custom_counters', 'mapper_calls', 1)
        for word in line.split():
            self.increment_counter('custom_counters', 'words_processed', 1)
            yield word, 1

    def combiner(self, word, counts):
        # Combiner: combinar contagens antes do shuffle
        self.increment_counter('custom_counters', 'combiner_calls', 1)
        yield word, sum(counts)

    def reducer(self, word, counts):
        # Função reduce: somar contagens finais e usar mecanismos de logging
        self.increment_counter('custom_counters', 'reducer_calls', 1)
        yield word, sum(counts)

if __name__ == '__main__':
    # Executar o job com diferentes configurações
    WordCount.run()
