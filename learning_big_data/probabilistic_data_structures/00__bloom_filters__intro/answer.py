
from pybloom_live import BloomFilter


def train_bloom_filter(elements, error_rate=0.001):
    elements = set(elements)
    bf = BloomFilter(capacity=len(elements), error_rate=error_rate)
    for element in elements:
        bf.add(element)
    
    return bf