
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
bf = BloomFilter(capacity=10 ** 5, error_rate=0.01)
with open('./resources/0.xml', 'r') as f:
    
    for line in f:
        user = row_to_dict(line)
        bf.add(user['displayname'])

with open('./resources/hot_displayname.bf', 'wb') as f:
    bf.tofile(f)
        
        
#
# MAP REDUCE JOB USING THE FILTER
# 
class NotHotFilterJob(MRJob):
    
    def mapper_init(self):
        with open(os.path.join(basedir, 'resources/hot_displayname.bf'), 'rb') as f:
            self.filter = BloomFilter.fromfile(f)
        
    def mapper(self, key, line):
        user = row_to_dict(line)
        
        if user['displayname'] not in self.filter: 
            yield (key, user) 