
from mrjob.job import MRJob
import xmltodict


class BadgeCountJob(MRJob):
    
    def mapper(self, _, line):
        yield 'a', 'a'
        
    def combiner(self, badge_name, counts):
        yield 'a', 'a'
        
    def reducer(self, badge_name, counts):
        yield 'a', 'a'   
        
        
class BadgeCountPerUserJob(MRJob):        
    def mapper(self, _, line):
        yield 'a', 'a'
        
    def combiner(self, user_id, name_counts):
        yield 'a', 'a'
        
    def reducer(self, user_id, name_counts):        
        yield 'a', 'a' 