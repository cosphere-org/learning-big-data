
from unittest import TestCase
from random import sample
from string import ascii_lowercase

from faker import Faker

# from exercise import train_bloom_filter
from answer import train_bloom_filter


class BloomFilterTestCase(TestCase):
    
    def setUp(self):
        self.faker = Faker()

    def get_prefix(self):
        return ''.join(sample(ascii_lowercase, 10))

    def get_names(self, count):
        return set([
            f'{self.faker.name()} {self.get_prefix()}' 
            for _ in range(count)])

    def test_train_bloom_filter__deals_with_simple_case(self):
        
        names = ['Alice', 'Bob', 'Charlers']
        
        bf = train_bloom_filter(names, 0.001)
        
        for name in names:
            assert name in bf
            
        assert 'alice' not in bf
        assert 'bobby' not in bf       
        
    def test_train_bloom_filter__deals_with_complex_case(self):
        
        names_0 = self.get_names(10 ** 4)
        names_1 = self.get_names(10 ** 4)
        
        bf = train_bloom_filter(names_0, 0.01)
        
        for name in names_0:
            assert name in bf
            
        negatives = names_1 - names_0
        assert len(negatives) > 0
        false_positives = sum([name in bf for name in negatives])         
        assert false_positives > 0
        assert abs(false_positives / 10 ** 4 - 0.01) < 0.01        