import json

def read_column(path, column_name):
    with open(path, 'r') as f:
        meta = json.loads(f.readline())
        ref_offset = f.tell()
        column_meta = meta['columns'][column_name]
        start, end = column_meta['start'], column_meta['end']
        
        f.seek(ref_offset + start)
        raw_values = f.read(end - start) 
        values = raw_values and raw_values.split(',') or []
        
    return values
        