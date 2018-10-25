
# from exercise import read_column
from answer import read_column


def test_read_column_big_data_deps():
    departments = read_column('./resources/employees.vicol1', 'department')
    big_data_departments = [dep for dep in departments if dep == 'BigData']
    assert len(big_data_departments) == 4
    
def test_read_column_big_data_first_name():
    first_names = read_column('./resources/employees.vicol1', 'first_name')
    assert first_names == ['Cindy', 'Manuel', 'Elizabeth', 'Betty', 'Michael', 'Grace', 'Julia', 'Donna', 'Billy', 'Todd']
    
    