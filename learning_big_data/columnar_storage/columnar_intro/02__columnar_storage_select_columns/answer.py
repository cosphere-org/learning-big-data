import json 

def select_columns(path, indices, select_columns):
    with open(path, 'r') as f:  
        meta = json.loads(f.readline())
        ref_offset = f.tell()   
        data = []
        
        for column in select_columns:
        
            start, end = meta['columns'][column]['start'], meta['columns'][column]['end']
            
            f.seek(ref_offset + start)
            raw_values = f.read(end - start) 
            values = raw_values and raw_values.split(',') or []
            
            data.append([values[index] for index in indices])
    
    records = []
    for element in list(zip(*data)):
        record = {}
        for i, column in enumerate(select_columns):
            record[column] = element[i]
        records.append(record)
    
    return records
