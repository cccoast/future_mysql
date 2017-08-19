import dbBase as db
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float

from future_table_struct import data_model_tick, data_model_min, data_model_day, Ticker
from trading_date import FutureDates
from time2point import DayMode
from misc import cffex_tickers, run_paralell_tasks

import numpy as np
import pandas as pd

default_subprocess_numbers = 4

min_columns = ('id','day','spot','Time','OpenPrice','HighPrice',\
                            'ClosePrice','LowPrice','Volume','OpenInterest')
day_columns = ('id','day','OpenPrice','HighPrice',\
                            'LowPrice','ClosePrice','Volume','OpenInterest')


class Sampler(object):

    def __init__(self, freq=1):
        dates = FutureDates()
        self.trading_days = np.array(
            [int(obj.date) for obj in dates.query_obj(dates.table_struct)])
        self.spots_gap = 120 * freq

    def sample_min(self,
                   ticker,
                   start_date=None,
                   end_date=None,
                   force_reload=False):

        ticker = ticker[:2]
        days = []
        if start_date is not None:
            days = self.trading_days[np.where(
                np.all(self.trading_days >= int(start_date), self.trading_days <
                       int(end_date)))]
        else:
            days = self.trading_days

        day_mode = DayMode()
        ticker_info = Ticker()

        total_spots_tick = day_mode.cffex_last if ticker[:
                                                         2] in cffex_tickers else day_mode.other_last
        total_spots_min = int(total_spots_tick / self.spots_gap)
        db_name_min = ticker_info.get_dbname(ticker, level='min')
        db_name_tick = ticker_info.get_dbname(ticker, level='tick')

        print 'spots count of tick/min = ', total_spots_tick, total_spots_min

        def sample_sub_day_list(sample_days):
            for iday in sample_days:
                tick_table = data_model_tick(db_name_tick, str(iday))
                if tick_table.check_table_exist():
                    tick_table.create_table()
                    min_table = data_model_min(db_name_min, str(iday))
                    if force_reload and min_table.check_table_exist():
                        print '[warning] drop table = ', iday
                        min_table.drop_table(str(iday))
                    elif min_table.check_table_exist():
                        continue
                    print iday
                    min_table.create_table()
                    tick_df_all = pd.read_sql_table(
                        str(iday), tick_table.engine, index_col=['spot'])
                    #                 print tick_df.head()
                    for id, tick_df in tick_df_all.groupby('id'):

                        min_df = pd.DataFrame(
                            index=range(total_spots_min), columns=min_columns)
                        min_df.ix[:, 'id'] = id
                        min_df.ix[:, 'day'] = iday
                        min_df.ix[:, 'spot'] = min_df.index

                        for tick_spot in xrange(
                                0, total_spots_tick - self.spots_gap,
                                self.spots_gap):
                            min_spot = tick_spot / self.spots_gap
                            min_df.ix[min_spot, 'Time'] = tick_df.ix[
                                tick_spot, 'Time'].split('.')[0]
                            min_df.ix[min_spot, 'OpenPrice'] = float(
                                tick_df.ix[tick_spot, 'LastPrice'])
                            min_df.ix[min_spot, 'HighPrice'] = float(
                                tick_df.ix[tick_spot:tick_spot + self.spots_gap
                                           - 1, 'LastPrice'].max())
                            min_df.ix[min_spot, 'LowPrice'] = float(
                                tick_df.ix[tick_spot:tick_spot + self.spots_gap
                                           - 1, 'LastPrice'].min())
                            min_df.ix[min_spot, 'ClosePrice'] = float(
                                tick_df.ix[tick_spot + self.spots_gap - 1,
                                           'LastPrice'])
                            min_df.ix[min_spot, 'Volume'] = int(
                                tick_df.ix[tick_spot + self.spots_gap - 1,
                                           'Volume'])
                            min_df.ix[min_spot, 'OpenInterest'] = int(
                                tick_df.ix[tick_spot + self.spots_gap - 1,
                                           'OpenInterest'])

                        min_df.to_sql(
                            str(iday),
                            min_table.engine,
                            index=False,
                            if_exists='append')

        #start multiprocessing
        sub_day_list = map(list,
                           np.split(days, [
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

        ticker_info = Ticker()
        db_name_min = ticker_info.get_dbname(ticker, level='min')
        db_name_day = ticker_info.get_dbname(ticker, level='day')
        table_name = ticker

        day_table = data_model_day(db_name_day, table_name)
        if force_reload and day_table.check_table_exist():
            print '[warning] drop day table [{}]'.format(table_name)
            day_table.drop_table(table_name)
        elif day_table.check_table_exist():
            print 'table already exist!'
            return 0

        day_table.create_table()

        def sample_sub_day_list(sample_days):
            #             print sample_days
            for iday in sample_days:
                min_table = data_model_min(db_name_min, str(iday))
                if min_table.check_table_exist():
                    print iday
                    min_df_all = pd.read_sql_table(
                        str(iday), min_table.engine, index_col=['spot'])
                    for ticker_id, min_df in min_df_all.groupby('id'):
                        open_price = float(min_df.ix[0, 'OpenPrice'])
                        close_price = float(
                            min_df.ix[len(min_df) - 1, 'ClosePrice'])
                        high_price = float(min_df['HighPrice'].max())
                        low_price = float(min_df['LowPrice'].min())
                        volume = int(min_df.ix[len(min_df) - 1, 'Volume'])
                        open_interest = int(
                            min_df.ix[len(min_df) - 1, 'OpenInterest'])
                        to_be_inserted_list = (ticker_id,int(iday),open_price,high_price,low_price,\
                                                    close_price,volume,open_interest)
                        to_be_inserted_dict = dict(
                            zip(day_columns, to_be_inserted_list))
                        day_table.insert_dictlike(
                            day_table.day_struct,
                            to_be_inserted_dict,
                            merge=True)

        sub_day_list = map(list,
                           np.split(days, [
                               len(days) / default_subprocess_numbers * i
                               for i in range(1, default_subprocess_numbers)
                           ]))
        run_paralell_tasks(sample_sub_day_list, sub_day_list)


def debug_single_day_min(ticker, day=20140314, freq=120):
    spots_gap = 120 * freq
    ticker_info = Ticker()
    day_mode = DayMode()
    total_spots_tick = day_mode.cffex_last if ticker[:
                                                     2] in cffex_tickers else day_mode.other_last
    total_spots_min = int(total_spots_tick / spots_gap)
    db_name_tick = ticker_info.get_dbname(ticker, level='tick')
    tick_table = data_model_tick(db_name_tick, str(day))
    tick_df_all = pd.read_sql_table(
        str(day), tick_table.engine, index_col=['spot'])
    for id, tick_df in tick_df_all.groupby('id'):
        print id
        min_df = pd.DataFrame(index=range(total_spots_min), columns=min_columns)
        min_df.ix[:, 'id'] = id
        min_df.ix[:, 'day'] = day
        min_df.ix[:, 'spot'] = min_df.index
        for tick_spot in xrange(0, total_spots_tick - spots_gap, spots_gap):
            try:
                min_spot = tick_spot / spots_gap
                min_df.ix[min_spot, 'Time'] = tick_df.ix[tick_spot,
                                                         'Time'].split('.')[0]
                min_df.ix[min_spot, 'OpenPrice'] = float(
                    tick_df.ix[tick_spot, 'LastPrice'])
                min_df.ix[min_spot, 'HighPrice'] = float(
                    tick_df.ix[tick_spot:tick_spot + spots_gap - 1, 'LastPrice']
                    .max())
                min_df.ix[min_spot, 'LowPrice'] = float(tick_df.ix[
                    tick_spot:tick_spot + spots_gap - 1, 'LastPrice'].min())
                min_df.ix[min_spot, 'ClosePrice'] = float(
                    tick_df.ix[tick_spot + spots_gap - 1, 'LastPrice'])
                min_df.ix[min_spot, 'Volume'] = int(
                    tick_df.ix[tick_spot + spots_gap - 1, 'Volume'])
                min_df.ix[min_spot, 'OpenInterest'] = int(
                    tick_df.ix[tick_spot + spots_gap - 1, 'OpenInterest'])
            except:
                print tick_spot
                print tick_df.ix[tick_spot, 'Time']
                exit(-1)

        print min_df.head()


if __name__ == '__main__':

    sampler = Sampler()
    #     sampler.sample_min('au',force_reload = False)
    sampler.sample_day('au', force_reload=True)

#     debug_single_day_min('au',day = 20140314)
