
from unittest import TestCase

from pybloom_live import BloomFilter

from answer import calculate_error_rate
# from exercise import calculate_error_rate


class BloomFilterErrorRateTestCase(TestCase):
    
    def prepare_test_and_train(self, threshold_divider):

        train = []
        test = []
        with open('./resources/top_1000_english_words.txt', 'r') as f:
            for i, word in enumerate(f):
                word = word.strip()

                if i % threshold_divider == 0:
                    test.append(word)

                else:
                    train.append(word)

        return train, test
    
    def test_calculate_error_rate__small_test(self):
        
        # -- fetch test and train samples
        train, test = self.prepare_test_and_train(10)
        
        # -- train Bloom filter
        bf = BloomFilter(len(train), error_rate=0.1)
        for word in train:
            bf.add(word)
        
        assert round(calculate_error_rate(bf, test), 1) == 0.1
        
    def test_calculate_error_rate__large_test(self):
        
        # -- fetch test and train samples
        train, test = self.prepare_test_and_train(2)
        
        # -- train Bloom filter
        bf = BloomFilter(len(train), error_rate=0.1)
        for word in train:
            bf.add(word)
        
        assert round(calculate_error_rate(bf, test), 1) == 0.1        