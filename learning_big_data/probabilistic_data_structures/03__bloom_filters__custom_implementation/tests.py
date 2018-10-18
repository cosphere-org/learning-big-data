
from unittest import TestCase

from answer import BloomFilter
# from exercise import BloomFilter


class BloomFilterTestCase(TestCase):
    
    def test__init__size__is_optimal(self):
        assert BloomFilter(10, 0.01).size == 95
        assert BloomFilter(100, 0.001).size == 1437
        assert BloomFilter(10000, 0.001).size == 143775
        
    def test__init__hash_count__is_optimal(self):
        assert BloomFilter(10, 0.01).hash_count == 6
        assert BloomFilter(100, 0.001).hash_count == 9
        assert BloomFilter(10000, 0.001).hash_count == 9
        
    def test_add_and_contain_work(self):
        bf = BloomFilter(10, 0.01)
        for word in ['Alice', 'Bob', 'Charles']:
            bf.add(word)
            assert word in bf 
        
        for word in ['alice', 'bob', 'charles']:
            assert word not in bf                 