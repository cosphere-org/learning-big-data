
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
        for i in range(self.hash_count): 
            index = mmh3.hash(item, i) % self.size 
        
            self.bit_array[index] = True
  
    def __contains__(self, item): 
        for i in range(self.hash_count): 
            index = mmh3.hash(item,i) % self.size 
            if not self.bit_array[index]:   
                return False

        return True
  
    def get_size(self, n, p): 
        
        m = -(n * math.log(p))/(math.log(2) ** 2) 

        return int(m) 
  
    def get_hash_count(self, m, n): 
        
        k = (m / n) * math.log(2) 
        
        return int(k) 