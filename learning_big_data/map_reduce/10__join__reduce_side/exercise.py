
from mrjob.job import MRJob
import xmltodict


def row_to_dict(row):
    row = row.strip()
    record = dict(xmltodict.parse(row)['row'])        

    return {k.replace('@', '').lower(): v for k, v in record.items()}


class ReduceSideJoinJob(MRJob):

    def mapper(self, _, line):
        # -- your code goes here
        yield 'a', 'a'
            
    def reducer(self, userid, entities):
        # -- your code goes here
        yield 'a', 'a'