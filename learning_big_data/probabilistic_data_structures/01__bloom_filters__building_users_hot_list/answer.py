
import xmltodict
from pybloom_live import BloomFilter


def row_to_dict(row):
    row = row.strip()
    record = dict(xmltodict.parse(row)['row'])        

    return {k.replace('@', '').lower(): v for k, v in record.items()}


def train_bloom_filter():
    # -- training the Bloom filter
    hot_display_names = set()
    with open('./resources/0.xml', 'rb') as f:
        for line in f:
            user = row_to_dict(line)
            hot_display_names.add(user['displayname'])

    bf = BloomFilter(len(hot_display_names), error_rate=0.001)

    for name in hot_display_names:
        bf.add(name)
        
    with open('./resources/hot_names_bloom_filter', 'wb') as f:
        bf.tofile(f)
        
    return bf