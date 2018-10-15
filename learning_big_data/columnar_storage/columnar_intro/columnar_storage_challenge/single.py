from texttable import Texttable
import json 
class ViCol1Table:
    def __init__(self, filepath):
        self.filepath = filepath
        self.__condition = None
        self.__column_names = set()
        self.__meta = None
        self.__f = None
        self.__ref_offset = 0
    
    def where(self, column_name, condition):
        self.__condition = (column_name, condition)
            
        return self
    
    def select(self, column_names):
        self.__column_names = set(column_names)
        return self
    
    def __find_ids(self, column_name, condition):
        filter_column = self.__meta['columns'][column_name]
        start, end = filter_column['start'], filter_column['end']
        self.__f.seek(self.__ref_offset + start)
        raw_values = self.__f.read(end - start) 
        values = raw_values and raw_values.split(',') or []
        found_indices = [i for i, v in enumerate(values) if condition(v)]
        return found_indices
    
    def __select_columns(self, indices):
        data = []
        
        for column in self.__column_names:
        
            start, end = self.__meta['columns'][column]['start'], self.__meta['columns'][column]['end']
            print(column, start, end)
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
            
            column, condition = self.__condition
            ids = self.__find_ids(column, condition)
            print(ids)
            data = self.__select_columns(ids)
            print(data)
            self.__display(self.__column_names, list(zip(*data)))
            
            



             
    
    
    
    