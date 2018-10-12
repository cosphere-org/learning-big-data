%%file exercise.py

import json

def find_ids(path, column_name, value):
    with open(path, 'r') as f:
        meta = json.loads(f.readline())
        ref_offset = f.tell() # ref offset
        
        filter_column = meta['columns'][column_name]
    
        
        
        start, end = filter_column['start'], filter_column['end']
        f.seek(ref_offset + start)
        raw_values = f.read(end - start) 
        values = raw_values and raw_values.split(',') or []
        found_indices = [i for i, r in enumerate(values) if r == value]
        return found_indices 
