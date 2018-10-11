
import json

from mrjob.job import MRJob
import xmltodict


class ReputationJob(MRJob):

    def mapper(self, key, line):
        line = line.strip()
        record = dict(xmltodict.parse(line)['row'])        
        user = {k.replace('@', '').lower(): v for k, v in record.items()}
        
        if int(user['reputation']) > 10000: 
            yield (key, json.dumps(user))    