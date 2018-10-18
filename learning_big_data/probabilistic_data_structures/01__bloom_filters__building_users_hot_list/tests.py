
from unittest import TestCase
from random import sample
from string import ascii_letters

from pybloom_live import BloomFilter

# from exercise import row_to_dict, train_bloom_filter
from answer import row_to_dict, train_bloom_filter


class BloomFilterTestCase(TestCase):
    
    def setUp(self):
        names = set()
        with open('resources/0.xml', 'r') as f:
            for line in f:
                user = row_to_dict(line)
                names.add(user['displayname'])

        self.names = names
    
    def test_train_bloom_filter__directly(self):
        bf = train_bloom_filter()
        
        for name in sample(self.names, 10):
            assert name in bf
            
            prefix = ''.join(sample(ascii_letters, 10))
            fake_name = f'{prefix}{name}'
            assert fake_name not in bf

    def test_train_bloom_filter__from_file(self):
        with open('./resources/hot_names_bloom_filter', 'rb') as f:
            bf = BloomFilter.fromfile(f)

        for name in sample(self.names, 10):
            assert name in bf
            
            prefix = ''.join(sample(ascii_letters, 10))
            fake_name = f'{prefix}{name}'
            assert fake_name not in bf