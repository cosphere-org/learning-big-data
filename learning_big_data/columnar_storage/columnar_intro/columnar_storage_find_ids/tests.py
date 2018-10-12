
from exercise import find_ids
# from answer import find_ids


def test_find_ids_age():
    ids = find_ids('./resources/employees.vicol1', 'age', '33')
    assert ids == [6, 8]
    
def test_find_ids_department():
    ids = find_ids('./resources/employees.vicol1', 'department', 'BigData')
    assert ids == [0, 4, 6, 8]
    
    
