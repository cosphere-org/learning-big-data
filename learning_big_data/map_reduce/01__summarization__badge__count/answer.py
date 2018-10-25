
from mrjob.job import MRJob
import xmltodict


class BadgeCountJob(MRJob):
    
    def mapper(self, _, line):
        record = dict(xmltodict.parse(line)['row'])        
        badge = {k.replace('@', '').lower(): v for k, v in record.items()}
        
        yield (badge['name'], 1)
        
    def combiner(self, badge_name, counts):
        yield (badge_name, sum(counts)) 
        
    def reducer(self, badge_name, counts):
        yield (badge_name, sum(counts))   
        
        
class BadgeCountPerUserJob(MRJob):        
    def mapper(self, _, line):
        record = dict(xmltodict.parse(line)['row'])        
        badge = {k.replace('@', '').lower(): v for k, v in record.items()}
        
        yield (badge['userid'], (badge['name'], 1))
        
    def combiner(self, user_id, name_counts):
        counts = {}
        for name, count in name_counts:
            counts.setdefault(name, 0)
            counts[name] += count
            
        for name, count in counts.items():
            yield (user_id, (name, count))  
        
    def reducer(self, user_id, name_counts):        
        counts = {}
        for name, count in name_counts:
            counts.setdefault(name, 0)
            counts[name] += count
            
        yield (user_id, list(counts.items()))         