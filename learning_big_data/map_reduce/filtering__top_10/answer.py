
import json

from mrjob.job import MRJob
import xmltodict


class Top10UsersJob(MRJob):

    def mapper_init(self):
        self.local_top_10 = []
        
    def mapper(self, key, line):
        line = line.strip()
        record = dict(xmltodict.parse(line)['row'])        
        user = {k.replace('@', '').lower(): v for k, v in record.items()}
        reputation = int(user['reputation'])
        
        self.local_top_10.append((reputation, json.dumps(user)))
        if len(self.local_top_10) > 10:
            self.local_top_10 = sorted(self.local_top_10, key=lambda x: x[0], reverse=True)
            self.local_top_10 = self.local_top_10[:10]
            
    def mapper_final(self):
        yield (None, [user for _, user in self.local_top_10])
        
    def reducer(self, _, top_10s):
        global_top_10 = []
        for top_10 in top_10s:
            for user in top_10:
                user = json.loads(user)
                global_top_10.append((user['reputation'], user))
        
        global_top_10 = sorted(global_top_10, key=lambda x: x[0], reverse=True)
        global_top_10 = global_top_10[:10]                  
        
        yield None, [user for _, user in global_top_10]                                     