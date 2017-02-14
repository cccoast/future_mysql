import dbBase as db
import os
import pandas as pd

from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index
from sqlalchemy import Table

import time2point

class cffex_if(db.DB_BASE):
    
    def __init__(self,db_name,table_name):
        super(cffex_if,self).__init__(db_name)
        
        table_struct = Table(table_name,self.meta,
                     Column('id',String,primary_key = True),
                     Column('day',Integer,primary_key = True,autoincrement = False),
                     Column('spot',Integer,primary_key = True,autoincrement = False),
                     Column('Time',String),
                     Column('LastPrice',Numeric),
                     Column('TradeVolume',Numeric),
                     Column('BidPrice',Numeric),
                     Column('BidVolume',Numeric),
                     Column('AskPrice',Numeric),
                     Column('AskVolume',Numeric),
                     Column('OpenInterest',Integer),
                    )
        
        if_struct = self.quick_map(table_struct)
                
        
if __name__ == '__main__':
    
    db_name = 'cffex_if'
    month = 201401
    root_path = r'D:\future\data\CFFEX\CFFEX_201401\201401\IF'
    
    dirs = filter(lambda x: os.path.isdir(os.path.join(x)), map(lambda y: os.path.join(root_path,y), os.listdir(root_path)))
#     print dirs
    
    fspot = time2point.DayMode()
    
    def timeSplit(time_stamp):
        [hh,mm],[ss,mili] = time_stamp.split(':')[:2],time_stamp.split(':')[-1].split('.')
        return int(hh),int(mm),int(ss),int(mili)
    
    
    for idir in dirs:
        infiles = os.listdir(idir)
        date = int(idir.split('\\')[-1])
        inss = map(lambda x: x.split('.')[0],infiles)
        day = month * 100 + date % 100
        print day
        for ins,infile in zip(inss,infiles):
            df = pd.read_csv(os.path.join(idir,infile),index_col = None,usecols = [0,1,3,4,5,6,7,8],parse_dates = False)
            df['spot'] = df['Time'].apply(timeSplit).apply(fspot.fcffex_time2spot)
            df['day'] = pd.Series([day] * len(df),index = df.index)
            df['id'] = pd.Series([infile.split('.')[0]] * len(df),index = df.index)
            print df.head()
            exit()
        