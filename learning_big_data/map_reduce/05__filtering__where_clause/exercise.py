
from mrjob.job import MRJob


class ReputationJob(MRJob):

    def mapper(self, key, line):
        yield 'a', 'a'