# !cat code_answer.py


def find_unique(x):
    return sorted(set(x))


def equal_no_matter_order(x, y):
    return set(x) == set(y)


def unique_intersection(x, y):
    return sorted(set(x) & set(y))
    
    
def unique_union(x, y): 
    return sorted(set(x) | set(y))
    

def unique_diff(x, y):
    return sorted(set(x) - set(y))    