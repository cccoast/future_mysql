import dbBase as db
import os
import pandas as pd
import numpy as np
import time

from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table

import time2point

class cffex_if(db.DB_BASE):
    
    def __init__(self,db_name,table_name):
        super(cffex_if,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('id',String(20),primary_key = True),
                     Column('day',Integer,primary_key = True,autoincrement = False),
                     Column('spot',Integer,primary_key = True,autoincrement = False),
                     Column('Time',String(20)),
                     Column('LastPrice',Float),
                     Column('TradeVolume',Integer),
                     Column('BidPrice',Float),
                     Column('BidVolume',Integer),
                     Column('AskPrice',Float),
                     Column('AskVolume',Integer),
                     Column('OpenInterest',Integer),
                    )
        
    def create_table(self):
        self.if_struct = self.quick_map(self.table_struct)
                
        
def import_one_month(month,root_path,start_date):
    
    db_name = 'cffex_if'
    
    dirs = filter(lambda x: os.path.isdir(os.path.join(x)), map(lambda y: os.path.join(root_path,y), os.listdir(root_path)))
#     print dirs
    
    fspot = time2point.DayMode()
    
    def timeSplit(time_stamp):
        [hh,mm],[ss,mili] = time_stamp.split(':')[:2],time_stamp.split(':')[-1].split('.')
        return int(hh),int(mm),int(ss),int(mili)
    
    def fill_open(df):
        
        #find first -1
        first_valid = -1
        for i in xrange(len(df)):
            if df.iloc[i]['spot'] != -1:
                break
            else:
                first_valid = i
                break
        #found!    
        if first_valid != -1:
            has_open = True
            for i in xrange(len(df)):
                if df.iloc[i]['spot'] > 0:
                    has_open = False
                    break
                elif df.iloc[i]['spot'] == 0:
                    has_open = True
                    break
            #no exist open , so fill open directly
            if not has_open:
                df.ix[first_valid,'spot'] = 0
            else:
                df.drop(df.index[first_valid],axis = 0)
                
        return first_valid
                
    for idir in dirs:
        infiles = os.listdir(idir)
        date = int(idir.split('\\')[-1])
        inss = map(lambda x: x.split('.')[0],infiles)
        day = int( month * 100 + date % 100 )
        if day < start_date:
            continue
        print day
        new_records = cffex_if(db_name,day)
        new_records.create_table()
        for ins,infile in zip(inss,infiles):
            print ins
            df = pd.read_csv(os.path.join(idir,infile),index_col = None,usecols = [0,1,3,4,5,6,7,8],parse_dates = False)
            df['spot'] = df['Time'].apply(timeSplit).apply(fspot.fcffex_time2spot)
            df['day'] = pd.Series([day] * len(df),index = df.index)
            df['id'] = pd.Series([infile.split('.')[0]] * len(df),index = df.index)
#             df.sort_values(by = 'spot', axis = 1, inplace = True)
            first_valid = fill_open(df)
            df.index = df['spot']
            df.drop('spot',axis = 1,inplace = True )
            df = df[ df.index != -1 ]
            print df.head()
            df = df.reindex(xrange(fspot.cffex_last),method = 'pad')
            df.fillna(method = 'pad',inplace = True)
            if first_valid >= 0:
                df.iloc[:first_valid+1].fillna(method = 'backfill',inplace = True)
            df['spot'] = df.index
            df['day'] = df['day'].apply(np.int)
            df['TradeVolume'] = df['TradeVolume'].apply(np.int)
            print df.head()
#             print df.tail()
#             start = time.time()
#             new_records.insert_data_frame(new_records.if_struct, df, merge = False)
            df.to_sql(str(day),new_records.engine,index = False,if_exists = 'append',chunksize = 2048) 
#             end = time.time()
#             print 'elapsed = ', end - start
    
if __name__ == '__main__':
    month = 201403
    start_date = 20140305
    import_path = r'D:\future\data\CFFEX\CFFEX_201403\CFFEX\201403\IF'
    import_one_month(month,import_path,start_date) 