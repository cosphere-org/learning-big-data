
from exercise import select_columns
# from answer import find_ids


def test_select_columns_one_id():
    records = select_columns('./resources/employees.vicol1', [0], ['first_name'])
    assert records == [{ 'first_name': 'Cindy' }]
    
def test_select_columns_multiple_ids():
    records = select_columns('./resources/employees.vicol1', [2,3], ['last_name', 'department'])
    assert records == [
        { 'last_name': 'Garcia', 'department': 'Embedded' }, 
        { 'last_name': 'Johnson', 'department': 'Embedded' },
    ]

    
