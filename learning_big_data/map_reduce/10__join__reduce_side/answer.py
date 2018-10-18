
from mrjob.job import MRJob
import xmltodict


def row_to_dict(row):
    row = row.strip()
    record = dict(xmltodict.parse(row)['row'])        

    return {k.replace('@', '').lower(): v for k, v in record.items()}


class ReduceSideJoinJob(MRJob):

    def mapper(self, _, line):
        entity = row_to_dict(line)
         
        if 'reputation' in entity:
            yield entity['id'], {'type': 'user', 'entity': entity}
        
        else:
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