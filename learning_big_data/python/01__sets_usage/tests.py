
from exercise import find_unique, equal_no_matter_order, unique_intersection, unique_union, unique_diff
# from answer import find_unique, equal_no_matter_order, unique_intersection, unique_union, unique_diff


def test_find_unique():
    
    assert find_unique([]) == []

    # -- works with strings 
    assert find_unique(['a', 'a', 'b']) == ['a', 'b']

    # -- works with numbers 
    assert find_unique([5, 5, 5.0]) == [5]
    
    # -- works with many items
    assert find_unique([1, 3, 2, 2, 5]) == [1, 2, 3, 5]
    

def test_equal_no_matter_order():
    
    # -- works when already equal
    assert equal_no_matter_order([1], [1]) is True
    
    # -- works for integer lists
    assert equal_no_matter_order([1, 4, 5], [4, 5, 1]) is True 

    # -- works for strings
    assert equal_no_matter_order('hello world', 'world hello') is True 
    
    # -- is case sensitive
    assert equal_no_matter_order('hello', 'Hello') is False
    
    
def test_unique_intersection():
    
    # -- works when all in common
    assert unique_intersection([1], [1]) == [1]
    
    # -- works when none in common
    assert unique_intersection([1, 4, 5], [8, 9]) == []

    # -- works on strings
    assert unique_intersection('hello you', 'world hello') == [' ', 'e', 'h', 'l', 'o']   
    
    
def test_unique_union():

    # -- works when all in common
    assert unique_union([1], [1]) == [1]
    
    # -- works when none in common
    assert unique_union([1, 4, 5, 5], [8, 9]) == [1, 4, 5, 8, 9]    
    
    
def test_unique_diff():
    # -- works when all in common
    assert unique_diff([1], [1]) == []
    
    # -- works when none in common
    assert unique_diff([1, 4, 5, 5], [5, 9]) == [1, 4]            