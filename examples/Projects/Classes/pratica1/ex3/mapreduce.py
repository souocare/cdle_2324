'''
 # @ Description:

3. Desenvolva um novo job MapReduce capaz de realizar o merge de diferentes jobs MapReduce do tipo Ex10-WordCount-01.
 '''


from mrjob.job import MRJob

# Não tenho a certeza do que o professor pretende mesmo.

class MergeJob(MRJob):

    def configure_args(self):
        super(MergeJob, self).configure_args()
        self.add_passthru_arg('--output', default='merged_output')
        self.add_passthru_arg('--input', nargs='+', default=['input1', 'input2'])

    def mapper(self, _, line):
        yield _, line

    def reducer(self, key, values):
        for value in values:
            yield key, value

if __name__ == '__main__':
    MergeJob.run()
