
def lazy_square(iterable):
    for elem in iterable:
        yield elem ** 2
        

def lazy_element_sum(iterable_a, iterable_b):
    for a, b in zip(iterable_a, iterable_b):
        yield a + b
        
    
def lazy_find(filepath, term):
    for line in open(filepath, 'r'):
        if term.lower() in line.lower():
            yield line.strip()