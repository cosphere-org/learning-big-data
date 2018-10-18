
import math 
import mmh3 
from bitarray import bitarray 


class BloomFilter: 
    
    def __init__(self, capacity, error_rate):         
        self.size = self.get_size(capacity, error_rate)         
        self.hash_count = self.get_hash_count(self.size, capacity) 
  
        self.bit_array = bitarray(self.size) 
        self.bit_array.setall(0) 
  
    def add(self, item): 
        pass
    
    def __contains__(self, item): 
        pass
    
    def get_size(self, n, p): 
        pass
    
    def get_hash_count(self, m, n): 
        pass 