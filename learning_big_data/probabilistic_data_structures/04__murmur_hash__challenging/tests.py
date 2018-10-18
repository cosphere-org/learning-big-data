
from unittest import TestCase

from answer import calculate_collisions_rate, is_uniform
# from exercise import calculate_collisions_rate, is_uniform


class MruMruChallengeTestCase(TestCase):
    
    def setUp(self):
        self.words = []
        with open('./resources/top_1000_english_words.txt', 'r') as f:
            for word in f:
                self.words.append(word.strip())
                
    def test_calculate_collisions_rate(self):
        assert calculate_collisions_rate(self.words[:100], seed=10) == 0
        assert calculate_collisions_rate(self.words[:500], seed=19) == 0.004        
        assert calculate_collisions_rate(self.words, seed=190) == 0.002

    def test_is_uniform(self):
        assert is_uniform(self.words[:100], seed=10) is True
        assert is_uniform(self.words[:500], seed=19) is True        
        assert is_uniform(self.words, seed=190) is True
        