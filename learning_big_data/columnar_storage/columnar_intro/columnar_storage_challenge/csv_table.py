from texttable import Texttable

def print_table(headers, rows):
    table = Texttable()
    table.add_rows([headers] + rows)
    print(table.draw())


class CsvTable:
    
    def __init__(self, filepath):
        with open(filepath, 'r') as f:
            self.columns = f.readline().strip().split(',')
            self.rows = []
            
            for line in f.readlines():
                entries = line.strip().split(',')
                
                self.rows.append({
                    column: entries[i]
                    for i, column in enumerate(self.columns)
                })
            
    def select(self, *columns):
        self.columns = columns
        
        return self
    
    def limit(self, limit):
        self.rows = self.rows[:limit]

        return self
    
    def offset(self, offset):
        self.rows = self.rows[offset:]
    
        return self
    
    def order_by(self, column, asc=True):
        self.rows = sorted(self.rows, key=lambda x: x[column], reverse=not asc)
        
        return self
    
    def where(self, condition):
        self.rows = [row for row in self.rows if condition(row)]
        
        return self
    
    def collect(self):        
        print_table(
            self.columns, 
            [[row[column] for column in self.columns] for row in self.rows])
        
    def collect_silent(self):        
        pass
