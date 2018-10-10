
def find_offset(filepath, term):
    
    term = term.lower().encode('utf8')
    current_offset = 0 
    for line in open(filepath, 'rb'):
        line = line.lower()
        
        if term in line:
            index = -1
            while True:
                index = line.find(term, index + 1)
                
                if index >= 0:
                    yield current_offset + index, current_offset + index + len(term)
                    
                else:
                    break
        
        current_offset += len(line)
        

def read_between(filepath, start, stop):
    
    with open(filepath, 'rb') as f:
        f.seek(start)
        
        return f.read(stop - start).decode('utf8')