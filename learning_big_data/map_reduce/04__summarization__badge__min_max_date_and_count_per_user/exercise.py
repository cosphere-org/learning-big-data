
from mrjob.job import MRJob
import xmltodict
from delorean import parse


class BadgeMinMaxCountPerUserJob(MRJob):
    
    def mapper(self, _, line):
        yield 'a', 'a'

    def combiner(self, user_id, values):
        yield 'a', 'a'
        
    def reducer(self, user_id, values):
        yield 'a', 'a'