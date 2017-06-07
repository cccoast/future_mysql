import dbBase as db
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table

from data_import import cffex_if
from trading_day_list import Dates

import numpy as np
import pandas as pd

class cffex_if_min(db.DB_BASE):
    
    def __init__(self,db_name,table_name):
        super(cffex_if_min,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('id',String(20),primary_key = True),
                     Column('day',Integer,primary_key = True,autoincrement = False),
                     Column('spot',Integer,primary_key = True,autoincrement = False),
                     Column('Time',String(20)),
                     Column('OpenPrice',Float),
                     Column('HighPrice',Float),
                     Column('LowPrice',Float),
                     Column('ClosePrice',Float),
                     Column('Volume',Integer),
                     Column('OpenInterest',Integer),
                    )
        
    def create_table(self):
        self.if_struct = self.quick_map(self.table_struct)
                
    def check_table_exist(self):
        return self.table_struct.exists()

class Sampler(object):
    
    def __init__(self,freq = 1):
        dates = Dates()
        self.trading_days = np.array([ int(obj.date) for obj in dates.query_obj(dates.date_struct) ])
        self.spots_gap = 120 * freq
        
    def sample_if(self,start_date = None,end_date = None):
        
        days = []
        if start_date is not None:
            days = self.trading_days[ np.where(np.all(self.trading_days >= int(start_date),self.trading_days < int(end_date)))]
        else:
            days = self.trading_days
            
        for iday in days:
            print iday
            tick_table = cffex_if('cffex_if',str(iday))
            if tick_table.check_table_exist():
                tick_table.create_table()
                min_table = cffex_if_min('cffex_if_min',str(iday))
                df = pd.read_sql_table(str(iday),tick_table.engine,index_col = ['spot'])
                for col_name,sub_df in df.groupby('id'):
                    print col_name
                    print sub_df.head()
            exit()
            
            
class Filler(object):
    pass

if __name__ == '__main__':
    
    sampler = Sampler()
    sampler.sample_if()
    
    