#coding:utf-8
import os
import pandas as pd
import numpy as np

import time2point
from functools import partial
from future_table_struct import data_model_tick
from trading_date import AllTradingDays
from misc import cffex_tickers, shfex_tickers


valid_dates = set([int(x) for x in AllTradingDays().get_trading_day_list()])

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


def import_tick_per_month(ticker,year_month,root_path,start_date,end_date,day_mode,force_reload):

    fspot = time2point.DayMode(day_mode)

    if ticker[:2] in cffex_tickers:
        db_name = 'cffex_' + ticker[:2]
        ftime2spot = fspot.fcffex_time2spot
        fspot2time = fspot.fcffex_spot2time
        fspot2time_str = lambda x: '{:02d}:{}:{}.{}'.format(*fspot.fcffex_spot2time_hhmmssms(x))
        spots_count_perday = fspot.cffex_last
    elif ticker[:2] in shfex_tickers:
        db_name = 'shfex_' + ticker[:2]
        ftime2spot = fspot.fother_time2spot
        spots_count_perday = fspot.other_last
        fspot2time_str = lambda x: '{:02d}:{}:{}.{}'.format(*fspot.fother_spot2time_hhmmssms(x))
    
    spots_count_perday = fspot.cffex_last    
        
    db_model = data_model_tick

    dirs = [x for x in [os.path.join(root_path, y) for y in os.listdir(root_path)] if os.path.isdir(os.path.join(x))]
    
    for idir in dirs:
        infiles = [x for x in os.listdir(idir) if str.isdigit(x.split('.')[0][2:])]
        date = int(idir.split(os.path.sep)[-1])
        inss = [x.split('.')[0] for x in infiles]
        day = int(year_month * 100 + date % 100)

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
                print('warning drop table = ', day)
                new_records.drop_table(str(day))
        new_records.create_table()       

        for ins, infile in zip(inss, infiles):
            print(day,ins)
            try:
                df = pd.read_csv(os.path.join(idir, infile),index_col=None,usecols=[0, 1, 3, 4, 5, 6, 7, 8],parse_dates=False)
            except:
                print('error, cannot read csv file')
                continue
            #some data source file may be in wrong format
            if len(df) < 1:
                continue
#             print df.head()
            split_time_func = timeSplit
            ##for speical time stamp
            if '.' not in df['Time'].iloc[0]:
                print('warning, time stamp format hh:mm:ss')
                cookie = [False] * ((15 - 9) * 3600 + 30 * 60)
                split_time_func = partial(SpecialTimeSplit, cookie)

            df['spot'] = df['Time'].apply(split_time_func).apply(ftime2spot)
            df['day'] = pd.Series([day] * len(df), index=df.index)
            df['id'] = pd.Series([infile.split('.')[0]] * len(df), index=df.index)

            df.sort_values(by='spot', axis=0, inplace=True)
            df.drop_duplicates('spot', keep='first', inplace=True)
            df.index = df['spot']
            df = df[df.index != -1]

            df.drop('spot', axis=1, inplace=True)

            #fill data on where bid/ask volume == 0
            #if bid/ask price is other text type,
            df.loc[df['BidVolume'] < 0.01, 'BidPrice'] = np.nan
            df.loc[df['AskVolume'] < 0.01, 'AskPrice'] = np.nan
            df.loc[df['LastPrice'] < 0.01, 'LastPrice'] = np.nan

            #take care here, some inactive contrace may not have ask/bid volume at first
            first_one = np.max(df.apply(lambda x: x.first_valid_index(), axis=0))
            df = df.reindex(range(spots_count_perday))
            
            df['spot'] = df.index
            df['Time'] = df.apply(lambda row:row['Time'] if not pd.isna(row['Time']) else fspot2time_str(row['spot']), axis=1)

            df.fillna(method='pad', inplace=True)
            if first_one > 0:
                df.fillna(method='backfill', inplace=True)

            #bid/ask price may not be float
            if (not isinstance(df.iloc[0]['BidPrice'], float)) and (not isinstance(df.iloc[0]['BidPrice'], np.float64)):
                df[['BidPrice']] = df[['BidPrice']].astype(float)
            if (not isinstance(df.iloc[0]['AskPrice'], float)) and (not isinstance(df.iloc[0]['AskPrice'], np.float64)):
                df[['AskPrice']] = df[['AskPrice']].astype(np.float)

            df['day'] = df['day'].apply(int)
            df['TradeVolume'] = df['TradeVolume'].astype(int)
            replaced = []
            for i in df.columns:
                if i != "TradeVolume":
                    replaced.append(i)
                else:
                    replaced.append('Volume')
            df.columns = replaced
            
            df.to_sql(str(day),new_records.engine,index=False,if_exists='append',chunksize=8192)
            
def import_tick_per_month_taobao(ticker,root_path,infiles,year,month,day_mode):
    
    db_name = 'cffex_if'
    
    fspot = time2point.DayMode(day_mode)
    this_ftime2spot = lambda x:fspot.fcffex_time2spot([ int(x) for x in  (*x.split('.')[0].split(':'),x.split('.')[1])] )
    this_fspot2time = lambda x: '{:02d}:{}:{}.{}'.format(*fspot.fcffex_spot2time_hhmmssms(x))
    spots_count_perday = fspot.cffex_last
#     print(spots_count_perday)
    
    db_model = data_model_tick
#     print(len(valid_dates))
    
    for ifile in infiles:    
        ifile_path = os.path.join(root_path,ifile)
        date = int(ifile.split('.')[0].split('_')[-1])
        
#         if date != 20100419 or ifile.split('.')[0].split('_')[0] != 'IF1006':
#             continue
        
        if (date not in valid_dates) or not ( year * 10000 + month * 100  < date <= year * 10000 + month * 100 + 31 ):
            continue
        
        print(date,ifile)
        
        #lastprice, askprice, askvolume, bidprice, bidvolume, openinterst, volume,day,id, timestamp(spot)
        #desired_cols = 'id','day','spot','Time','LastPrice','Volume','BidPrice','BidVolume', 'AskPrice','AskVolume','OpenInterest',
        df = pd.read_csv(ifile_path,usecols = [4,22,23,24,25,13,11,0,1,20,21],index_col = None,dtype = {0:int,1:str})        
        df_cols = 'day','id','LastPrice','Volume','OpenInterest','hhmmss','ms','BidPrice','BidVolume', 'AskPrice','AskVolume',
        df.columns = df_cols
        
        df['Time'] = df.apply(lambda row: '.'.join((row['hhmmss'],str(row['ms']))), axis=1)  
        df['spot'] = df['Time'].apply(this_ftime2spot)
        
        df.sort_values(by='spot', axis=0, inplace=True)
        df.drop_duplicates('spot', keep='first', inplace=True)
        df.index = df['spot']
        df = df[ (df.index >= 0) & (df.index <= spots_count_perday) ]
        
        df.drop(['spot','hhmmss','ms'], axis=1, inplace=True)  
        #fill missing
        
        #fill data on where bid/ask volume == 0
        #if bid/ask price is other text type,
        df.loc[df['BidVolume'] < 0.01, 'BidPrice'] = np.nan
        df.loc[df['AskVolume'] < 0.01, 'AskPrice'] = np.nan
        df.loc[df['LastPrice'] < 0.01, 'LastPrice'] = np.nan
        
        #fill missing starts
        first_one = np.max(df.apply(lambda x: x.first_valid_index(), axis=0))
        df = df.reindex(range(spots_count_perday))
        df['spot'] = df.index
        df['Time'] = df.apply(lambda row:row['Time'] if not pd.isna(row['Time']) else this_fspot2time(row['spot']), axis=1)
        
        df.fillna(method='pad', inplace=True)
        if first_one > 0:
            df.fillna(method='backfill', inplace=True)
        
        df['day'] = df['day'].astype(int)
        df['Volume'] = df['Volume'].astype(int)

        new_records = db_model(db_name, date)    
        new_records.create_table()    
        df.to_sql(str(date),new_records.engine,index=False,if_exists='append',chunksize=8192)
    
def get_import_path(ticker,year,month):
    root_path = r'J:\future\future\data'
    import_path = None
    if ticker == 'if':
        if year == 2013:
            import_path = r'CFFEX/CFFEX201301-201312/{}{:0>2}/IF'.format(year,month)
        elif year == 2014:
            import_path = r'CFFEX/CFFEX_{}{:0>2}/{}{:0>2}/IF'.format(year,month,year,month)
        elif year == 2015:
            if month <= 4:
                import_path = r'CFFEX/CFFEX201501-201504/{}{:0>2}/IF'.format(year,month)
            else:
                import_path = r'CFFEX/CFFEX201505-201512/CFFEX/{}{:0>2}/IF'.format(year,month)
        elif year == 2016:
            if month <= 2:
                import_path = r'CFFEX/CFFEX201601-201602/{}{:0>2}/IF'.format(year,month)
            else:
                import_path = r'CFFEX/CFFEX_{}{:0>2}/CFFEX/{}{:0>2}/IF'.format(year,month,year,month)
    elif ticker == 'au':
        if year == 2013:
            import_path = r'SHFE/SHFE201301-201312/{}{:0>2}/AU'.format(year,month)
        elif year <= 2016:
            import_path = r'SHFE/{}{:0>2}/AU'.format(year,month)
    return os.path.join(root_path,import_path)
            
def import_cffex_if(year, month, start_date = None, end_date = None, day_mode = 'after_2016',force_reload=False):
    import_path = get_import_path('if', year, month)
    print(import_path)
    import_tick_per_month('if', year * 100 + month, import_path, start_date, end_date, day_mode, force_reload)

def import_shfex_au(year, month, start_date = None, end_date = None, force_reload=False):
    import_path = get_import_path('au', year, month)
    print(import_path)
    import_tick_per_month('au',year * 100 + month,import_path,start_date,end_date,force_reload=force_reload)

def init():
    init_str = r'python data_import -type=if -year=2013 -month=1'

def test():
    fspot = time2point.DayMode('before_2016')
    this_ftime2spot = lambda x:fspot.fcffex_time2spot([ int(x) for x in (*(x.split('.')[0].split(':')),x.split('.')[1])] )
    print(this_ftime2spot('9:15:00.500'))

def from_cmd():
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

def import_if_before_2013():
    import regex as re
    pattern = re.compile(r'^IF([\d]{4})_([\d]{8})(\.csv)$')
    root_path = r'J:\future\future_taobao\taobao if 2010-2013\taobao if 2010-2013\IF_201212'
    infiles = sorted(list(set([ ifile for ifile in os.listdir(root_path) if pattern.search(ifile) ])))
    for year in range(2010,2013):
        for month in range(1,13):
            if year == 2010 and month < 4:
                continue
            print(year,month)
            import_tick_per_month_taobao('if',root_path,year,month,'before_2016')
            
def import_if_after_201610():
    import regex as re
    pattern = re.compile(r'^IF([\d]{4})_([\d]{8})(\.csv)$')
    root_path = r'J:\future\future_taobao\taobao if tick\IF2016-2022'
    infiles = sorted(list(set([ ifile for ifile in os.listdir(root_path) if pattern.search(ifile) ])))
    for year in range(2016,2023):
        for month in range(1,13):
            if year == 2016 and month < 10:
                continue
            print(year,month)
            import_tick_per_month_taobao('if',root_path,infiles,year,month,'after_2016')


def clear_db(start_date,end_date):
    sql = r'DELETE FROM cffex_if.{} WHERE 1=1'
    valid_dates = [int(x) for x in AllTradingDays().get_trading_day_list() if start_date <= x < end_date]
    for date in valid_dates:
        db = data_model_tick('cffex_if',date)
        if db.check_table_exist():
            print(date)
            db.execute_sql(sql.format(db.table_struct.name))
            
def import_2016_new():
    for month in range(1,10):
        import_cffex_if(2016,month,day_mode = 'after_2016',force_reload=True)

if __name__ == '__main__':
#     import_if_after_201610()
#     clear_db(20160101,20161001)
#     import_2016_new()
    pass

