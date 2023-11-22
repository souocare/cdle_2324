'''
 # @ Description:

3. Desenvolva um novo job MapReduce capaz de realizar o merge de diferentes jobs MapReduce do tipo Ex10-WordCount-01.
 '''


from mrjob.job import MRJob, MRStep


# Não tenho a certeza do que o professor pretende mesmo.

class MergeJob(MRJob):

    def configure_args(self):
        super(MergeJob, self).configure_args()
        self.add_passthru_arg('--input', nargs='+', default=['input1', 'input2'])
        self.add_passthru_arg('--output', default='merged_output')
        self.add_passthru_arg("-nr", "--numreducers", help="Number of reducers")
        self.add_passthru_arg("-cc", "--compressioncodec", help="Compression codec (e.g., gzip)")



    def mapper(self, key, value):
        #value = value.decode('utf-8')  # Decode bytes to utf-8 string

        #for line in value:
        line = value #line.strip()
        tokens = re.findall(r"\b\w+\b", line)
        for token in tokens:
            yield token, 1

    def reducer(self, key, value):
        yield None, (sum(value), key)

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
    MergeJob.run()
