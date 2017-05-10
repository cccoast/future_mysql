import dbBase as db
import os
import pandas as pd
import numpy as np
import time

from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table

import time2point
from functools import partial

class cffex_if(db.DB_BASE):
    
    def __init__(self,db_name,table_name):
        super(cffex_if,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('id',String(20),primary_key = True),
                     Column('day',Integer,primary_key = True,autoincrement = False),
                     Column('spot',Integer,primary_key = True,autoincrement = False),
                     Column('Time',String(20)),
                     Column('LastPrice',Float),
                     Column('Volume',Integer),
                     Column('BidPrice',Float),
                     Column('BidVolume',Integer),
                     Column('AskPrice',Float),
                     Column('AskVolume',Integer),
                     Column('OpenInterest',Integer),
                    )
        
    def create_table(self):
        self.if_struct = self.quick_map(self.table_struct)
                
    def check_table_exist(self):
        return self.table_struct.exists()
        
def import_one_month(month,root_path,start_date):
    
    db_name = 'cffex_if'
    
    dirs = filter(lambda x: os.path.isdir(os.path.join(x)), map(lambda y: os.path.join(root_path,y), os.listdir(root_path)))
#     print dirs
    
    fspot = time2point.DayMode()
    
    def timeSplit(time_stamp):
#         try:
#             [hh,mm],[ss,mili] = time_stamp.split(':')[:2],time_stamp.split(':')[-1].split('.')
#         except:
#             print time_stamp
#             exit(-1)
        [hh,mm],[ss,mili] = time_stamp.split(':')[:2],time_stamp.split(':')[-1].split('.')
        return int(hh),int(mm),int(ss),int(mili)
    
    def SpecialTimeSplit(cookie,time_stamp):
        hh,mm,ss = time_stamp.split(':')[:]
        bucket = ( int(hh) - 9 ) * 3600 + ( int(mm) ) * 60 + int(ss)
        if cookie[bucket] == False:
            cookie[bucket] = True
            return int(hh),( int(mm) ),int(ss),0
        else:
            return int(hh),( int(mm) ),int(ss),500
            
    for idir in dirs:
        infiles = os.listdir(idir)
        date = int(idir.split(os.path.sep)[-1])
        inss = map(lambda x: x.split('.')[0],infiles)
        day = int( month * 100 + date % 100 )
        if day < start_date:
            continue
        print day
        new_records = cffex_if(db_name,day)
        for ins,infile in zip(inss,infiles):
            print ins
            df = pd.read_csv(os.path.join(idir,infile),index_col = None,usecols = [0,1,3,4,5,6,7,8],parse_dates = False)
            
            split_time_func = timeSplit
            ##for speical time stamp
            if '.' not in df['Time'].iloc[0]:
                print 'warning, time stamp format hh:mm:ss'
                cookie = [False] * ( (15 - 9) * 3600 + 30 * 60 )
                split_time_func = partial(SpecialTimeSplit,cookie)
            
            df['spot'] = df['Time'].apply(split_time_func).apply(fspot.fcffex_time2spot)
            df['day'] = pd.Series([day] * len(df),index = df.index)
            df['id'] = pd.Series([infile.split('.')[0]] * len(df),index = df.index)
            
#             print df.head()
            df.sort_values(by = 'spot', axis = 0, inplace = True)
            df.drop_duplicates('spot', keep = 'first', inplace = True)
            df.index = df['spot']
            df = df[ df.index != -1 ]
            
            df.drop('spot',axis = 1,inplace = True )
            first_one = df.index[0]
            df = df.reindex(xrange(fspot.cffex_last),method = 'pad')
            
            df.fillna(method = 'pad',inplace = True)
            if first_one > 0:
                df.ix[:first_one,:].fillna(method = 'backfill',inplace = True)
#             print df.head()
            
            df['spot'] = df.index
            df['day'] = df['day'].apply(np.int)
            df['TradeVolume'] = df['TradeVolume'].apply(np.int)
            replaced = []
            for i in df.columns:
                if i != "TradeVolume":
                    replaced.append(i)
                else:
                    replaced.append('Volume')    
            df.columns = replaced
            print len(df)
#             start = time.time()
#             new_records.insert_data_frame(new_records.if_struct, df, merge = False)
            df.to_sql(str(day),new_records.engine,index = False,if_exists = 'append',chunksize = 4096) 
#             end = time.time()
#             print 'elapsed = ', end - start

if __name__ == '__main__':
    
    #for if 2014
    #month = 201412
    #start_date = 0
    #import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX_{0}/{1}/IF'.format(month,month)
    #import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX_{0}/CFFEX/{0}/IF'.format(month,month)
    
    #for if 2015
    month = 201504
    start_date = 10
    import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX201501-201504/{0}/IF'.format(month)

#     month = 201505
#     start_date = 0
#     import_path = r'/media/xudi/software/future/data/CFFEX/CFFEX201505-201512/CFFEX/{0}/IF'.format(month)
    
    import_one_month(month,import_path,month * 100 + start_date) 
    
