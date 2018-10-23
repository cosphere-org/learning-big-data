
import statistics

from mrjob.job import MRJob
from delorean import parse
import xmltodict


class BadgeMinMaxCountPerUserJob(MRJob):
    
    def mapper(self, _, line):
        record = dict(xmltodict.parse(line)['row'])        
        badge = {k.replace('@', '').lower(): v for k, v in record.items()}

        yield (
            badge['userid'], 
            (badge['date'], badge['date'], 1),
        )

    def combiner(self, user_id, values):
        yield next(self.reducer(user_id, values))
        
    def reducer(self, user_id, values):
        g_dt_min = None
        g_dt_max = None
        g_cnt = 0

        for dt_min, dt_max, cnt in values:
            if g_dt_min is None or parse(dt_min) < parse(g_dt_min):
                g_dt_min = dt_min

            if g_dt_max is None or parse(dt_max) > parse(g_dt_max):
                g_dt_max = dt_max

            g_cnt += cnt

        yield (user_id, (g_dt_min, g_dt_max, g_cnt))        