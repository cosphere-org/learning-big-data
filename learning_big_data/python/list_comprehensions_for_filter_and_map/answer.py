
def filter_elements(x, fn):
    
    return [elem for elem in x if fn(elem)]


def map_elements(x, fn):
    
    return [fn(elem) for elem in x]