
import json
import random

from mrjob.job import MRJob
import xmltodict


class RandomSamplingJob(MRJob):

    def mapper(self, key, line):
        line = line.strip()
        record = dict(xmltodict.parse(line)['row'])        
        user = {k.replace('@', '').lower(): v for k, v in record.items()}
        
        if random.random() < 0.01: 
            yield (key, json.dumps(user))    