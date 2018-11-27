from texttable import Texttable

class ViCol1TableReader:
    def __init__(self, filepath):
        self.filepath = filepath
        self.__conditions = {}
        self.__column_names = set()
        self.__meta = None
        self.__f = None
        self.__ref_offset = 0
    
    def where(self, column_name, condition):
        self.__conditions.setdefault(column_name,[]).append(condition)
            
        return self
    
    def select(self, column_names):
        self.__column_names |= set(column_names)
        return self
    
    def __find_ids(self, column_name, conditions):
        filter_column = self.__meta['columns'][column_name]
        start, end = filter_column['start'], filter_column['end']
        self.__f.seek(self.__ref_offset + start)
        raw_values = self.__f.read(end - start) 
        values = raw_values and raw_values.split(',') or []
        found_indices = [i for i, v in enumerate(values) if all([c(v) for c in conditions])]
        return found_indices
    
    def __find_all_ids(self):
        column_meta = next(iter(self.__meta['columns'].values()))
        start, end = column_meta['start'], column_meta['end']
        self.__f.seek(self.__ref_offset + start)
        raw_values = self.__f.read(end - start) 
        values = raw_values and raw_values.split(',') or []
        return list(range(0, len(values)))        
    
    def __select_columns(self, indices):
        data = []
        
        for column in self.__column_names:
        
            start, end = self.__meta['columns'][column]['start'], self.__meta['columns'][column]['end']
            
            self.__f.seek(self.__ref_offset + start)
            raw_values = self.__f.read(end - start) 
            values = raw_values and raw_values.split(',') or []
            
            data.append([values[index] for index in indices])
        return data
        
    def __display(self, headers, rows):
        table = Texttable()
        table.add_rows([headers] + rows)
        print(table.draw())

    
    def execute(self):
        with open(self.filepath, 'r') as f:
            self.__meta = json.loads(f.readline())
            self.__ref_offset = f.tell() # ref offset
            self.__f = f
            
            ids = set()
            
            if not self.__conditions:
                ids |= set(self.__find_all_ids())
            
            for column, conditions in self.__conditions.items():
                if ids:
                    ids &= set(self.__find_ids(column, conditions))
                else:
                    ids |= set(self.__find_ids(column, conditions))
            data = self.__select_columns(ids)
            self.__display(self.__column_names, list(zip(*data)))
            
    def execute_silent(self):
        with open(self.filepath, 'r') as f:
            self.__meta = json.loads(f.readline())
            self.__ref_offset = f.tell() # ref offset
            self.__f = f
            
            ids = set()
            
            if not self.__conditions:
                ids |= set(self.__find_all_ids())
            
            for column, conditions in self.__conditions.items():
                if ids:
                    ids &= set(self.__find_ids(column, conditions))
                else:
                    ids |= set(self.__find_ids(column, conditions))
            data = self.__select_columns(ids)
        
        
        
import json

class ViCol1TableWriter:
    def __init__(self, filepath):
        self.filepath = filepath
        self.__records_to_insert = []
    
    def __decode_file(self):
        with open(self.filepath, 'r') as f:
            header = json.loads(f.readline())
            end_of_meta = f.tell()
            columns = []
            for column_name in header['columns'].keys():
                column = header['columns'][column_name]
                f.seek(end_of_meta + column['start'])
                data_string = f.read(column['end'] - column['start']) 
                data = data_string.split(',') if data_string else []
                columnData = { 'name': column_name, 'data': data}
                columns.append(columnData)
        return (header, columns)


    def __encode_file(self, columns):
        new_header = {'columns': {}}
        current_offset = 0
        for column in columns:
            joined_data_len = len(','.join(column['data']))
            new_header['columns'][column['name']] = { 
                'start': current_offset, 
                'end': current_offset + joined_data_len
            }
            current_offset = current_offset + joined_data_len

        with open(self.filepath, 'w') as f:
                f.write(json.dumps(new_header) + '\n')
                for column in columns:
                    f.write(','.join(column['data']))
    
    def insert(self, record):
        header, columns = self.__decode_file()
        for column in columns:
            column['data'].append(f'{record[column["name"]]}')
        self.__encode_file(columns)
                                  
    def insert_many(self, records):
        header, columns = self.__decode_file()
        for column in columns:
            for record in records:
                column['data'].append(f'{record[column["name"]]}')
        self.__encode_file(columns)