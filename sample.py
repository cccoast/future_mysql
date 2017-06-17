import dbBase as db
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table

from table_struct import cffex_if,cffex_if_min,cffex_if_day
from trading_day_list import Dates
from time2point import DayMode

import numpy as np
import pandas as pd

class Sampler(object):
    
    def __init__(self,freq = 1):
        dates = Dates()
        self.trading_days = np.array([ int(obj.date) for obj in dates.query_obj(dates.date_struct) ])
        self.spots_gap = 120 * freq
        
    def sample_if_min(self,start_date = None,end_date = None,force_reload = False):
        
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
            tick_table = cffex_if('cffex_tick',str(iday))
            if tick_table.check_table_exist():
                tick_table.create_table()
                min_table = cffex_if_min('cffex_if_min',str(iday))               
                if force_reload and min_table.check_table_exist():
                    print '[warning] drop table = ',iday
                    min_table.drop_table(str(iday))
                elif min_table.check_table_exist():
                    continue            
                print iday
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
        
    def sample_if_day(self,force_reload = False):
        
        #date range the same as min block
        day_columns = ('id','day','OpenPrice','HighPrice',\
                            'LowPrice','ClosePrice','Volume','OpenInterest')
        days = self.trading_days
        
        day_table = cffex_if_day('cffex_day','if')
        if force_reload and day_table.check_table_exist():
            print '[warning] drop day table [if]'
            day_table.drop_table('if')
        elif day_table.check_table_exist():
            print 'table already exist!'
            return 0
        
        day_table.create_table()
        
        for iday in days:
            print iday
            min_table = cffex_if_min('cffex_if_min',str(iday))
            if min_table.check_table_exist():
                min_df_all = pd.read_sql_table(str(iday),min_table.engine,index_col = ['spot'])
                for id,min_df in min_df_all.groupby('id'):
                    open_price = float(min_df.ix[0,'OpenPrice'])
                    close_price = float(min_df.ix[len(min_df)-1,'ClosePrice'])
                    high_price = float(min_df['HighPrice'].max())
                    low_price = float(min_df['LowPrice'].min())
                    volume = int(min_df.ix[len(min_df)-1,'Volume'])
                    open_interest = int(min_df.ix[len(min_df)-1,'OpenInterest'])
                    to_be_inserted_list = (id,int(iday),open_price,high_price,low_price,\
                                                close_price,volume,open_interest)
                    to_be_inserted_dict = dict(zip(day_columns,to_be_inserted_list))
                    day_table.insert_dictlike(day_table.if_day_struct, to_be_inserted_dict, merge = True)
                    
if __name__ == '__main__':
    
    sampler = Sampler()
#     sampler.sample_if(force_reload = True)
#     sampler.sample_if_min(force_reload = False)
    sampler.sample_if_day(force_reload = True)
    
    