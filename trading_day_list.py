import dbBase as db

from sqlalchemy import Column, Integer, String
from sqlalchemy import Table

import pandas as pd
from data_import import cffex_if

class Dates(db.DB_BASE):
    
    def __init__(self):
        db_name,table_name = 'dates','trading_days'
        super(Dates,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('date',Integer,primary_key = True,autoincrement = False),
                    )
        self.date_struct = self.quick_map(self.table_struct)

class futureOrder(db.DB_BASE):
    
    def __init__(self,ticker):
        db_name,table_name = 'dates','future_order' + '_' + str.lower(ticker[:2])
        super(futureOrder,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('date',Integer,primary_key = True,autoincrement = False),
                     Column('if0001',String(20)),
                     Column('if0002',String(20)),
                     Column('if0003',String(20)),
                     Column('if0004',String(20)),
                    )
        
        self.future_order_struct = self.quick_map(self.table_struct)
        
    def set_order_cffex_if(self):
        
        dbname = 'cffex_if'
        
        dates = Dates()
        ss = dates.session()
        records = ss.query(dates.date_struct).all()
        for irec in records:
            date = irec.date
            all_future = cffex_if(db_name=dbname,table_name=str(date))
            if not all_future.check_table_exist():
                continue 
            print date
            sql = 'select distinct id from cffex_if.{0} order by id;'.format(str(date))
            tickers = all_future.execute_sql(sql)
            orders = [ irec[0] for irec in tickers ]
            to_be_inserted = [date,]
            to_be_inserted.extend(orders)
            print to_be_inserted
            self.insert_listlike(self.future_order_struct, to_be_inserted, True)
        ss.close()
        
def import_trading_days():
    
    infile = r'D:\future\dates.csv'
    df = pd.read_csv(infile,parse_dates = [0])
    df['date'] = df['date'].apply(lambda x: int(x.year * 10000 + x.month * 100 + x.day))
    print df.head()
    dates = Dates()
    df.to_sql('trading_days',dates.engine,index = False,if_exists = 'append',chunksize = 2048) 
    
def set_future_order():
        
    fo = futureOrder('if') 
    fo.set_order_cffex_if()   

def erase_invalid_days():
    dbname = 'cffex_if'
    dates = Dates()
    invalids = []
    ss = dates.session()
    records = ss.query(dates.date_struct).all()
    for irec in records:
        date = irec.date
        all_future = cffex_if(db_name=dbname,table_name=str(date))
        if not all_future.check_table_exist():
            invalids.append(date)
    print invalids
    dates.delete_lists(dates.date_struct, invalids)
          
if __name__ == '__main__':
    erase_invalid_days()
    
    