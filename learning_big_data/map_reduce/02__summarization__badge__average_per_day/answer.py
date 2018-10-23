
from mrjob.job import MRJob
import xmltodict
from delorean import parse


class BadgeAveragePerDayJob(MRJob):
    
    def mapper(self, _, line):
        record = dict(xmltodict.parse(line)['row'])        
        badge = {k.replace('@', '').lower(): v for k, v in record.items()}

        yield (
            parse(badge['date']).date.strftime('%Y-%m-%d'), 
            {'avg': len(badge['name']), 'count': 1},
        )
    
    def combiner(self, dt, values):
        yield next(self.reducer(dt, values))  
        
    def reducer(self, dt, values):
        numerator = 0
        count = 0
        for value in values:
            numerator += value['avg'] * value['count'] 
            count += value['count']

        yield (dt, {'avg': numerator / count, 'count': count})    