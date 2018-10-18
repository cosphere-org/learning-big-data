
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
with open('./resources/0-users.xml', 'r') as f:
    
    for line in f:
        user = row_to_dict(line)
        if int(user['reputation']) > 1500:
            bf.add(user['id'])

            
with open('./resources/hot_user_ids.bf', 'wb') as f:
    bf.tofile(f)
    
    
class ReduceSideWithBloomFilterJob(MRJob):

    def mapper_init(self):
        with open(os.path.join(basedir, 'resources/hot_user_ids.bf'), 'rb') as f:
            self.filter = BloomFilter.fromfile(f)
        
    def mapper(self, _, line):
        entity = row_to_dict(line)
         
        if 'reputation' in entity:
            if int(entity['reputation']) > 1500:
                yield entity['id'], {'type': 'user', 'entity': entity}
        
        else:
            if entity['userid'] in self.filter:
                yield entity['userid'], {'type': 'badge', 'entity': entity}
            
    def reducer(self, userid, entities):
        record = {
            'user': None,
            'badges': [],
        }
        for entity in entities:
            if entity['type'] == 'user':
                record['user'] = entity['entity']
            
            else:
                record['badges'].append(entity['entity'])      
                
        if record['user'] and record['badges']:
            yield None, record