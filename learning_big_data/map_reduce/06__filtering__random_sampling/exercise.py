
from mrjob.job import MRJob


class RandomSamplingJob(MRJob):

    def mapper(self, key, line):
        yield 'a', 'a'