
import os,sys
parent_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
if parent_path not in sys.path:
    sys.path.append(parent_path)
    
import dbBase as db
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float

from future_table_struct import data_model_tick, data_model_min, data_model_day, FutureTicker
from trading_date import FutureDates
from time2point import DayMode
from future_mysql.misc import cffex_tickers, run_paralell_tasks

from stock import StockTicker 

import numpy as np
import pandas as pd

default_subprocess_numbers = 1

min_columns = ('id','day','spot','Time','OpenPrice','HighPrice','ClosePrice','LowPrice','Volume','OpenInterest')
day_columns = ('id','day','OpenPrice','HighPrice','LowPrice','ClosePrice','Volume','OpenInterest')


class Sampler(object):

    def __init__(self, freq=1):
        dates = FutureDates()
        self.trading_days = np.array([int(obj.date) for obj in dates.query_obj(dates.table_struct)])
        self.spots_gap = 120 * freq

    def sample_min(self, ticker, start_date= None, end_date= None, force_reload=False):

        ticker = ticker[:2]
        days = []
        if start_date is not None:
            days = [i for i in self.trading_days if start_date < i < end_date ]
        else:
            days = self.trading_days

        day_mode = DayMode()
        ticker_info = FutureTicker()

        total_spots_tick = day_mode.cffex_last if ticker[:2] in cffex_tickers else day_mode.other_last
        total_spots_min = int(total_spots_tick / self.spots_gap)
        db_name_min = ticker_info.get_dbname(ticker, level='min')
        db_name_tick = ticker_info.get_dbname(ticker, level='tick')

        print('spots count of tick/min = ', total_spots_tick, total_spots_min)

        def sample_sub_day_list(sample_days):
            for iday in sample_days:
                tick_table = data_model_tick(db_name_tick, str(iday))
                if tick_table.check_table_exist():
                    tick_table.create_table()
                    min_table = data_model_min(db_name_min, str(iday))
                    if force_reload and min_table.check_table_exist():
                        print('[warning] drop table = ', iday)
                        min_table.drop_table(str(iday))
                    elif min_table.check_table_exist():
                        continue
                    print(iday)
                    min_table.create_table()
                    tick_df_all = pd.read_sql_table(str(iday), tick_table.engine, index_col=['spot'])
                    #                 print tick_df.head()
                    for id, tick_df in tick_df_all.groupby('id'):

                        min_df = pd.DataFrame(
                            index=list(range(total_spots_min)), columns=min_columns)
                        min_df.loc[:, 'id'] = id
                        min_df.loc[:, 'day'] = iday
                        min_df.loc[:, 'spot'] = min_df.index

                        for tick_spot in range(0, total_spots_tick - self.spots_gap,self.spots_gap):
                            min_spot = tick_spot / self.spots_gap
                            min_df.loc[min_spot, 'Time'] = tick_df.loc[tick_spot, 'Time'].split('.')[0]
                            min_df.loc[min_spot, 'OpenPrice'] = float(tick_df.loc[tick_spot, 'LastPrice'])
                            min_df.loc[min_spot, 'HighPrice'] = float(tick_df.loc[tick_spot:tick_spot + self.spots_gap - 1, 'LastPrice'].max())
                            min_df.loc[min_spot, 'LowPrice'] = float( tick_df.loc[tick_spot:tick_spot + self.spots_gap - 1, 'LastPrice'].min())
                            min_df.loc[min_spot, 'ClosePrice'] = float(tick_df.loc[tick_spot + self.spots_gap - 1,'LastPrice'])
                            min_df.loc[min_spot, 'Volume'] = int(tick_df.loc[tick_spot + self.spots_gap - 1,'Volume'])
                            min_df.loc[min_spot, 'OpenInterest'] = int(tick_df.loc[tick_spot + self.spots_gap - 1,'OpenInterest'])

                        min_df.to_sql(str(iday),min_table.engine,index=False,if_exists='append')

        #start multiprocessing
        sub_day_list = list(np.split(days, [
                               len(days) / default_subprocess_numbers * i
                               for i in range(1, default_subprocess_numbers)
                           ]))
        run_paralell_tasks(sample_sub_day_list, sub_day_list)

#         pool = Pool(default_subprocess_numbers)
#         pool.map(sample_sub_day_list,sub_day_list)
#         pool.close()
#         pool.join()

    def sample_day(self, ticker, force_reload=False):

        ticker = ticker[:2]
        #date range the same as min block
        days = self.trading_days

        ticker_info = FutureTicker()
        db_name_min = ticker_info.get_dbname(ticker, level='min')
        db_name_day = ticker_info.get_dbname(ticker, level='day')
        table_name = ticker
        print(db_name_day, table_name)
        day_table = data_model_day(db_name_day, table_name)
        if force_reload and day_table.check_table_exist():
            print('[warning] drop day table [{}]'.format(table_name))
            day_table.drop_table(table_name)
        elif day_table.check_table_exist():
            print('table already exist!')
            return 0

        day_table.create_table()

        def sample_sub_day_list(sample_days):
            #             print sample_days
            for iday in sample_days:
                min_table = data_model_min(db_name_min, str(iday))
                if min_table.check_table_exist():
                    print(iday)
                    min_df_all = pd.read_sql_table(str(iday), min_table.engine, index_col=['spot'])
                    for ticker_id, min_df in min_df_all.groupby('id'):
                        open_price = float(min_df.loc[0, 'OpenPrice'])
                        close_price = float(min_df.loc[len(min_df) - 1, 'ClosePrice'])
                        high_price = float(min_df['HighPrice'].max())
                        low_price = float(min_df['LowPrice'].min())
                        volume = int(min_df.loc[len(min_df) - 1, 'Volume'])
                        open_interest = int(min_df.loc[len(min_df) - 1, 'OpenInterest'])
                        to_be_inserted_list = (ticker_id,int(iday),open_price,high_price,low_price,close_price,volume,open_interest)
                        to_be_inserted_dict = dict(list(zip(day_columns, to_be_inserted_list)))
                        day_table.insert_dictlike(day_table.day_struct,to_be_inserted_dict,merge=True)

        sub_day_list = list(map(list,np.split(days, [len(days) / default_subprocess_numbers * i for i in range(1, default_subprocess_numbers)])))
        run_paralell_tasks(sample_sub_day_list, sub_day_list)

class StockMinSampler(object):
    
    BEGIN_MILLI_OF_DAY = 34200000  #9:30
    LUNCH_BREAK_BEGIN  = 41400000  #11:30
    LUNCH_BREAK_END    = 43200000  #13:01
    END_MILLI_OF_DAY   = 54000000  #15:00
    
    SPOTS_OF_1MIN = 241
    SPOTS_OF_5MIN = 49
    
    #5分钟的第一个点是开盘点，实际只有一分钟的区间长度
    def one_to_N(self, N = 5):
        one_min_to_five_min = [0,] * self.SPOTS_OF_1MIN
        for i in range(1,self.SPOTS_OF_1MIN):
            one_min_to_five_min[i] = int( ( i - 1 ) / N ) + 1
        return one_min_to_five_min

def debug_single_day_min(ticker, day=20140314, freq=120):
    spots_gap = 120 * freq
    ticker_info = FutureTicker()
    day_mode = DayMode()
    total_spots_tick = day_mode.cffex_last if ticker[:
                                                     2] in cffex_tickers else day_mode.other_last
    total_spots_min = int(total_spots_tick / spots_gap)
    db_name_tick = ticker_info.get_dbname(ticker, level='tick')
    tick_table = data_model_tick(db_name_tick, str(day))
    tick_df_all = pd.read_sql_table(
        str(day), tick_table.engine, index_col=['spot'])
    for id, tick_df in tick_df_all.groupby('id'):
        print(id)
        min_df = pd.DataFrame(index=list(range(total_spots_min)), columns=min_columns)
        min_df.loc[:, 'id'] = id
        min_df.loc[:, 'day'] = day
        min_df.loc[:, 'spot'] = min_df.index
        for tick_spot in range(0, total_spots_tick - spots_gap, spots_gap):
            try:
                min_spot = tick_spot / spots_gap
                min_df.loc[min_spot, 'Time'] = tick_df.loc[tick_spot,'Time'].split('.')[0]
                min_df.loc[min_spot, 'OpenPrice'] = float(tick_df.loc[tick_spot, 'LastPrice'])
                min_df.loc[min_spot, 'HighPrice'] = float(tick_df.loc[tick_spot:tick_spot + spots_gap - 1, 'LastPrice'].max())
                min_df.loc[min_spot, 'LowPrice'] = float(tick_df.loc[tick_spot:tick_spot + spots_gap - 1, 'LastPrice'].min())
                min_df.loc[min_spot, 'ClosePrice'] = float(tick_df.loc[tick_spot + spots_gap - 1, 'LastPrice'])
                min_df.loc[min_spot, 'Volume'] = int(tick_df.loc[tick_spot + spots_gap - 1, 'Volume'])
                min_df.loc[min_spot, 'OpenInterest'] = int(tick_df.loc[tick_spot + spots_gap - 1, 'OpenInterest'])
            except:
                print(tick_spot)
                print(tick_df.loc[tick_spot, 'Time'])
                exit(-1)

        print(min_df.head())

def init():
    sampler = Sampler()
    sampler.sample_min('if', start_date = 20160605, end_date = 20160610, force_reload = False)
    sampler.sample_day('if', force_reload = True)
    
    sampler = Sampler()
    sampler.sample_min('au', start_date = 20160605, end_date = 20160610, force_reload = False)
    sampler.sample_day('au', force_reload = True)
    
if __name__ == '__main__':
    sms = StockMinSampler()
    v = sms.one_to_N(5)
    
