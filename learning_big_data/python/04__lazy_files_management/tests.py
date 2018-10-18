
# from answer import find_offset, read_between
from exercise import find_offset, read_between


def test_find_offset():
    
    assert list(find_offset('./resources/simple.txt', 'świecie')) == [
        (6, 14), 
        (29, 37),
    ] 
    
    assert list(find_offset('./resources/complex.txt', 'Filters')) == [
        (7, 14), 
        (221, 228),
    ]     


def test_read_between():
    
    assert read_between('./resources/simple.txt', 6, 14) == 'świecie'
    assert read_between('./resources/simple.txt', 29, 37) == 'świecie'

    assert read_between('./resources/complex.txt', 221, 228) == 'filters'