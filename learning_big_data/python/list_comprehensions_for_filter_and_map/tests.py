
from answer import filter_elements, map_elements


def test_filter_elements():
    
    assert filter_elements([1, 3, 4, 5, 6, 1], lambda x: x % 2 == 0) == [4, 6]
    
    
def test_map_elements():
    
    assert map_elements([1, 3, 4, 5], lambda x: x ** 2) == [1, 9, 16, 25]
        