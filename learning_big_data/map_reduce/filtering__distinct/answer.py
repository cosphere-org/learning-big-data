
from mrjob.job import MRJob
import xmltodict


class DistictReputationJob(MRJob):
    
    def mapper(self, _, line):
        line = line.strip()
        record = dict(xmltodict.parse(line)['row'])        
        user = {k.replace('@', '').lower(): v for k, v in record.items()}
        reputation = int(user['reputation'])        
        
        yield (reputation, None)
        
    def reducer(self, reputation, _):
        yield (None, reputation)