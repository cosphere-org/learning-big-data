
from mrjob.job import MRJob


class WordCountJob(MRJob):

    def mapper(self, key, line):
        yield 'a', 'a'

    def reducer(self, key, values):
        yield 'a', 'a'