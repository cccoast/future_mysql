import os
import pandas as pd
import numpy as np

import time2point
from functools import partial
from future_table_struct import data_model_tick
from trading_date import AllTradingDays
from misc import cffex_tickers, shfex_tickers


def timeSplit(time_stamp):
    [hh, mm], [ss, mili] = time_stamp.split(':')[:2], time_stamp.split(':')[-1].split('.')
    return int(hh), int(mm), int(ss), int(mili)


def SpecialTimeSplit(cookie, time_stamp):
    hh, mm, ss = time_stamp.split(':')[:]
    bucket = (int(hh) - 9) * 3600 + (int(mm)) * 60 + int(ss)
    if cookie[bucket] == False:
        cookie[bucket] = True
        return int(hh), (int(mm)), int(ss), 0
    else:
        return int(hh), (int(mm)), int(ss), 500


def import_tick_per_month(ticker,month,root_path,start_date,end_date,
                          force_reload=False):

    fspot = time2point.DayMode()

    if ticker[:2] in cffex_tickers:
        db_name = 'cffex_' + ticker[:2]
        ftime2spot = fspot.fcffex_time2spot
        spots_count_perday = fspot.cffex_last
    elif ticker[:2] in shfex_tickers:
        db_name = 'shfex_' + ticker[:2]
        ftime2spot = fspot.fother_time2spot
        spots_count_perday = fspot.other_last

    db_model = data_model_tick
    valid_dates = set(
        map(lambda x: int(x), AllTradingDays().get_trading_day_list()))

    dirs = filter(lambda x: os.path.isdir(os.path.join(x)),
                  map(lambda y: os.path.join(root_path, y),
                      os.listdir(root_path)))

    for idir in dirs:
        infiles = filter(lambda x: str.isdigit(x.split('.')[0][2:]),
                         os.listdir(idir))
        date = int(idir.split(os.path.sep)[-1])
        inss = map(lambda x: x.split('.')[0], infiles)
        day = int(month * 100 + date % 100)
        if start_date is not None and day < start_date:
            continue
        if end_date is not None and day > end_date:
            break

        new_records = db_model(db_name, day)
        if (day not in valid_dates):
            continue
        if not force_reload:
            if new_records.check_table_exist():
                continue
        else:
            if new_records.check_table_exist():
                print 'warning drop table = ', day
                new_records.drop_table(str(day))
        print day

        for ins, infile in zip(inss, infiles):
            print ins
            try:
                df = pd.read_csv(os.path.join(idir, infile),index_col=None,usecols=[0, 1, 3, 4, 5, 6, 7, 8],parse_dates=False)
            except:
                print 'error, cannot read csv file'
                continue
            #some data source file may be in wrong format
            if len(df) < 1:
                continue
#             print df.head()
            split_time_func = timeSplit
            ##for speical time stamp
            if '.' not in df['Time'].iloc[0]:
                print 'warning, time stamp format hh:mm:ss'
                cookie = [False] * ((15 - 9) * 3600 + 30 * 60)
                split_time_func = partial(SpecialTimeSplit, cookie)

            df['spot'] = df['Time'].apply(split_time_func).apply(ftime2spot)
            df['day'] = pd.Series([day] * len(df), index=df.index)
            df['id'] = pd.Series([infile.split('.')[0]] * len(df), index=df.index)

            #print df.head()
            df.sort_values(by='spot', axis=0, inplace=True)
            df.drop_duplicates('spot', keep='first', inplace=True)
            df.index = df['spot']
            df = df[df.index != -1]

            df.drop('spot', axis=1, inplace=True)

            #fill data on where bid/ask volume == 0
            #if bid/ask price is other text type,
            df.ix[df['BidVolume'] < 0.01, 'BidPrice'] = np.nan
            df.ix[df['AskVolume'] < 0.01, 'AskPrice'] = np.nan

            #take care here, some inactive contrace may not have ask/bid volume at first
            first_one = np.max(df.apply(lambda x: x.first_valid_index(), axis=0))
            df = df.reindex(xrange(spots_count_perday), method='pad')

            df.fillna(method='pad', inplace=True)
            if first_one > 0:
                df.ix[:first_one, :].fillna(method='backfill', inplace=True)

            #bid/ask price may not be float
            if (not isinstance(df.iloc[0]['BidPrice'], np.float)) and (
                    not isinstance(df.iloc[0]['BidPrice'], np.float64)):
                df[['BidPrice']] = df[['BidPrice']].astype(np.float)
            if (not isinstance(df.iloc[0]['AskPrice'], np.float)) and (
                    not isinstance(df.iloc[0]['AskPrice'], np.float64)):
                df[['AskPrice']] = df[['AskPrice']].astype(np.float)

            df['spot'] = df.index
            df['day'] = df['day'].apply(np.int)
            df['TradeVolume'] = df['TradeVolume'].astype(np.int)
            replaced = []
            for i in df.columns:
                if i != "TradeVolume":
                    replaced.append(i)
                else:
                    replaced.append('Volume')
            df.columns = replaced

            df.to_sql(str(day),new_records.engine,index=False,if_exists='append',chunksize=8192)

#             end = time.time()
#             print 'elapsed = ', end - start

def get_import_path(ticker,year,month):
    import_path = None
    if ticker == 'if':
        if year == 2013:
            import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX201301-201312/{}{:0>2}/IF'.format(year,month)
        elif year == 2014:
            import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX_{}{:0>2}/{}{:0>2}/IF'.format(year,month,year,month)
        elif year == 2015:
            if month <= 4:
                import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX201501-201504/{}{:0>2}/IF'.format(year,month)
            else:
                import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX201505-201512/CFFEX/{}{:0>2}/IF'.format(year,month)
        elif year == 2016:
            if month <= 2:
                import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX201601-201602/{}{:0>2}/IF'.format(year,month)
            else:
                import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX_201603/CFFEX/{}{:0>2}/IF'.format(year,month)
    elif ticker == 'au':
        if year == 2013:
            import_path = r'/media/xudi/software/future/data/SHFE/SHFE201301-201312/{}{:0>2}/AU'.format(year,month)
        elif year <= 2016:
            import_path = r'/media/xudi/software/future/data/SHFE/{}{:0>2}/AU'.format(year,month)
    return import_path
            
def import_cffex_if(year, month, start_date = None, end_date = None, force_reload=False):
    import_path = get_import_path('if', year, month)
    print import_path
    import_tick_per_month('if', year * 100 + month, import_path, start_date, end_date, force_reload)

def import_shfex_au(year, month, start_date = None, end_date = None, force_reload=False):
    import_path = get_import_path('au', year, month)
    print import_path
    import_tick_per_month('au',year * 100 + month,import_path,start_date,end_date,force_reload=force_reload)


if __name__ == '__main__':

    #import_shfex_au(2014, 2, force_reload=True)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-type',dest='type', nargs='?', type=str)
    parser.add_argument('-year', dest='year', nargs='?', type=int)
    parser.add_argument('-month', dest='month', nargs='?', type=int)
    parser.add_argument('-start_date', dest='start_date', nargs='?', default = None, type=int)
    parser.add_argument('-end_date', dest='end_date', nargs='?', default = None, type=int)
    args = parser.parse_args()
    arg_dict = vars(args)
    if (arg_dict['year'] is not None) and (arg_dict['month'] is not None):
        if arg_dict['type'] == 'if':
            import_cffex_if(arg_dict['year'], arg_dict['month'])
        if arg_dict['type'] == 'au':
            import_shfex_au(arg_dict['year'], arg_dict['month'])

