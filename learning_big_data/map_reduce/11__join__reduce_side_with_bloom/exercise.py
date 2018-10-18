
import os

from mrjob.job import MRJob
import xmltodict
from pybloom_live import BloomFilter


basedir = os.path.dirname(__file__)


def row_to_dict(row):
    row = row.strip()
    record = dict(xmltodict.parse(row)['row'])        

    return {k.replace('@', '').lower(): v for k, v in record.items()}


#
# TRAIN BLOOM FILTER
#
# -- training the Bloom filter
# -- you code goes here...
bf = 'replace this with something...'

with open('./resources/hot_user_ids.bf', 'wb') as f:
    bf.tofile(f)
      
        
class ReduceSideWithBloomFilterJob(MRJob):

    def mapper_init(self):
        with open(os.path.join(basedir, 'resources/hot_user_ids.bf'), 'rb') as f:
            self.filter = BloomFilter.fromfile(f)
        
    def mapper(self, _, line):
        # -- your code goes here
        yield 'a', 'a'
            
    def reducer(self, userid, entities):
        # -- your code goes here
        yield 'a', 'a'