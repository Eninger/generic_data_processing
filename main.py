import csv

class DataProcessor:
    "Generic data processing library"
    
    def __init__(self):
        self.data = None

    def load_data(self,filename, sep, types: dict):
        """load the data from a dinamic csv file"""
        
        def convert_data(row):
            for key, value in types.items():
                row[key] = value(row[key])
            return row

        with open(filename, encoding='UTF8') as csvfile:
            reader = csv.DictReader(csvfile,delimiter=sep)

            data = list(map(convert_data,reader))
        
        self.data = data

        return self.data

    def write_data(self):
        """write data as a csv file called output.csv"""
        with open('output.csv','w',encoding='UTF8',newline='') as csvfile:
            writer = csv.writer(csvfile,delimiter=',')

            for row in self.data:
                writer.writerow(row.values())

    def index_search(self, start, stop=None):
        """search on data based on index given"""

        if isinstance(start,int) and stop == None:
            return self.data[start]
        
        elif isinstance(start,tuple):
            index_list = []
            for index in start:
                index_list.append(self.data[index])
            
            return index_list
        
        else: return self.data[start:stop]


    def filter(self,query:dict):
        """Filter data based on query and returns fltered_data"""
        filtered_data = []
        for row in self.data:
            for column, value in query.items():
                for n in value:  
                    if row[column] == n:
                        filtered_data.append(row)

        return filtered_data
    
    def projection(self,columns:list):
        filtered_data = []

        for row in self.data:
            filtered_row = {col:row[col] for col in columns}
            filtered_data.append(filtered_row)

        return filtered_data
    

    def update(self,column:str,newValue,condition=None,row=None):
        """update data based on parameters given and return updated data"""
        
        if row == None and condition == None:
            for row in self.data:
                row[column] = newValue
        
        elif row == None and condition != None:
            for row in self.data:
                if row[column] == condition:
                    row[column] = newValue

        elif condition == None and row != None :
            self.data[row][column] = newValue
        
        else:
            if self.data[row][column] == condition:
                
                self.data[row][column] = newValue
                

        return self.data