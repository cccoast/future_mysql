import dbBase as db
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table

from data_import import cffex_if
from trading_day_list import Dates
from time2point import DayMode

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
        
    def sample_if(self,start_date = None,end_date = None,force_reload = False):
        
        days = []
        if start_date is not None:
            days = self.trading_days[ np.where(np.all(self.trading_days >= int(start_date),self.trading_days < int(end_date)))]
        else:
            days = self.trading_days
        day_mode = DayMode()
        
        total_spots_tick = day_mode.cffex_last
        total_spots_min  = int(total_spots_tick / self.spots_gap)
        min_columns = ('id','day','spot','Time','OpenPrice','HighPrice',\
                            'ClosePrice','LowPrice','Volume','OpenInterest')
        print 'spots count of tick/min = ',total_spots_tick,total_spots_min
        for iday in days:
            print iday
            tick_table = cffex_if('cffex_if',str(iday))
            if tick_table.check_table_exist():
                tick_table.create_table()
                min_table = cffex_if_min('cffex_if_min',str(iday))
                
                if force_reload and min_table.check_table_exist():
                    print '[warning] drop table = ',iday
                    min_table.drop_table(str(iday))
                    
                min_table.create_table()   
                tick_df_all = pd.read_sql_table(str(iday),tick_table.engine,index_col = ['spot'])
#                 print tick_df.head()
                for id,tick_df in tick_df_all.groupby('id'):
                   
                    min_df = pd.DataFrame(index = range(total_spots_min),columns = min_columns)
                    min_df.ix[:,'id'] = id
                    min_df.ix[:,'day'] = iday
                    min_df.ix[:,'spot'] = min_df.index
                    
                    for tick_spot in xrange(0,total_spots_tick - self.spots_gap,self.spots_gap):
                        min_spot = tick_spot / self.spots_gap
                        min_df.ix[min_spot,'Time'] = tick_df.ix[tick_spot,'Time'].split('.')[0]
                        min_df.ix[min_spot,'OpenPrice'] = float(tick_df.ix[tick_spot,'LastPrice'])
                        min_df.ix[min_spot,'HighPrice'] = float(tick_df.ix[tick_spot:tick_spot+self.spots_gap-1,'LastPrice'].max())
                        min_df.ix[min_spot,'LowPrice'] = float(tick_df.ix[tick_spot:tick_spot+self.spots_gap-1,'LastPrice'].min())
                        min_df.ix[min_spot,'ClosePrice'] = float(tick_df.ix[tick_spot+self.spots_gap-1,'LastPrice'])
                        min_df.ix[min_spot,'Volume'] = int(tick_df.ix[tick_spot+self.spots_gap-1,'Volume'])
                        min_df.ix[min_spot,'OpenInterest'] = int(tick_df.ix[tick_spot+self.spots_gap-1,'OpenInterest'])
                    
                    min_df.to_sql(str(iday),min_table.engine,index = False,if_exists = 'append') 
                    
if __name__ == '__main__':
    
    sampler = Sampler()
    sampler.sample_if(force_reload = True)
    
    