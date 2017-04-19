import dbBase as db

from sqlalchemy import Column, Integer
from sqlalchemy import Table

import pandas as pd

class Dates(db.DB_BASE):
    
    def __init__(self):
        db_name,table_name = 'dates','trading_days'
        super(Dates,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('date',Integer,primary_key = True,autoincrement = False),
                    )
        self.date_struct = self.quick_map(self.table_struct)
        
if __name__ == '__main__':
    
    infile = r'D:\future\dates.csv'
    df = pd.read_csv(infile,parse_dates = [0])
    df['date'] = df['date'].apply(lambda x: int(x.year * 10000 + x.month * 100 + x.day))
    print df.head()
    dates = Dates()
    df.to_sql('trading_days',dates.engine,index = False,if_exists = 'append',chunksize = 2048) 
    