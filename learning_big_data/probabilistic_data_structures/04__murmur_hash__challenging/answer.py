
import statistics

import mmh3


def _find_counts_distribution(elements, seed):
    collisions = {}
    for element in elements:
        hash_value = mmh3.hash(element, seed=seed)
        collisions.setdefault(hash_value, 0)
        collisions[hash_value] += 1 
        
    return collisions

    
def calculate_collisions_rate(elements, seed):
    collisions = _find_counts_distribution(elements, seed)
    
    return sum(v for k, v in collisions.items() if v > 1) / len(elements)


def is_uniform(elements, seed):
    collisions = _find_counts_distribution(elements, seed)

    return abs(statistics.mean(collisions.values()) - 1) < 0.01