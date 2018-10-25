
from mrjob.job import MRJob
import xmltodict
from delorean import parse


class BadgeAveragePerDayJob(MRJob):
    
    def mapper(self, _, line):
        yield 'a', 'a'
    
    def combiner(self, dt, values):
        yield 'a', 'a'
        
    def reducer(self, dt, values):
        yield 'a', 'a'