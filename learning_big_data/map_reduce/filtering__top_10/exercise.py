
from mrjob.job import MRJob


class Top10UsersJob(MRJob):

    def mapper_init(self):
        pass

    def mapper(self, key, line):
        yield 'a', 'a'
        
    def mapper_final(self):
        pass
    
    def reducer(self, key, values):
        pass
    