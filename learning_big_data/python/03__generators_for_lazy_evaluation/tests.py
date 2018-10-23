
from answer import lazy_square, lazy_element_sum, lazy_find
# from exercise import lazy_square, lazy_element_sum, lazy_find


def test_lazy_square():
    
    # -- works for generator and returns generator
    assert next(lazy_square(range(4, 5))) == 16
    
    # -- all elements are really squared
    assert list(lazy_square((1, 4, 7, 3))) == [1, 16, 49, 9]

    
def test_lazy_element_sum():
        
    # -- works for generators of the same size
    assert list(lazy_element_sum(range(4, 8), range(6, 16, 2))) == [10, 13, 16, 19]
    
    
def test_lazy_find():
    
    # -- works for single match
    assert list(lazy_find('./resources/input_text.txt', 'Despite')) == [
        'learnt once you know about them. Despite reading several articles on bloom',
    ]  
    
    # -- works for multi match
    assert list(lazy_find('./resources/input_text.txt', 'Bloom')) == [
        'Bloom Filters is one of those data structures that you don’t generally',
        'learnt once you know about them. Despite reading several articles on bloom',
    ]