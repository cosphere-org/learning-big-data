
import statistics

from mrjob.job import MRJob
from delorean import parse
import xmltodict


class BadgeMediaStdDevPerDayJob(MRJob):
    
    def mapper(self, _, line):
        record = dict(xmltodict.parse(line)['row'])        
        badge = {k.replace('@', '').lower(): v for k, v in record.items()}
        
        yield (
            parse(badge['date']).date.strftime('%Y-%m-%d'), 
            len(badge['name']),
        )

    def reducer(self, dt, values):
        values = list(values)
        
        median = statistics.median(values)

        stddev = None
        if len(values) > 1:
            stddev = statistics.stdev(values)

        yield (dt, {'median': median, 'stddev': stddev})    