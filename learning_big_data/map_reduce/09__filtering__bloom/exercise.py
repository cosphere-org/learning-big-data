

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
        # -- you code goes here
        yield 'a', 'a'            