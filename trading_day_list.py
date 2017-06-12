import dbBase as db

from sqlalchemy import Column, Integer, String
from sqlalchemy import Table

import pandas as pd
from table_struct import cffex_if,cffex_if_min

from misc import get_nth_specical_weekday_in_daterange,timestamp2int

class Dates(db.DB_BASE):
    
    def __init__(self):
        db_name,table_name = 'dates','trading_days'
        super(Dates,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('date',Integer,primary_key = True,autoincrement = False),
                    )
        self.date_struct = self.quick_map(self.table_struct)
        self.trading_day_obj = self.date_struct
        
    def get_first_bigger_than(self,idate):     
        ss = self.get_session()
        ret = ss.query(self.trading_day_obj).filter(self.trading_day_obj.date >= int(idate)).first()
        if ret:
            ss.close()
            return ret.date
        else:
            ss.close()
            return None
        
    def get_first_less_than(self,idate):
        ss = self.get_session()
        ret = ss.query(self.trading_day_obj).filter(self.trading_day_obj.date <= int(idate)).order_by(self.trading_day_obj.date.desc()).first()
        if ret:
            ss.close()
            return ret.date
        else:
            ss.close()
            return None
        
    def get_trading_day_list(self):
        ss = self.session()
        records = ss.query(self.date_struct).all()
        ss.close()
        return map(lambda x: x.date, records)

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
    
    def get_exchange_rollling_day_if(self):
        weekday,nth = 5,2
        dates = Dates()
        trading_days = dates.get_trading_day_list()
        last_trading_day = timestamp2int( pd.to_datetime(str(trading_days[-1])) + pd.to_timedelta(1,unit = 'D'))
        third_fridays = get_nth_specical_weekday_in_daterange(trading_days[0], last_trading_day, weekday, nth)
        exchange_rolling_days = map(lambda x: dates.get_first_less_than(x),third_fridays)
        return exchange_rolling_days
        
    def set_order_cffex_if(self):
        
        #use min tables instead of tick tables to accelerate!
        dbname = 'cffex_if_min'
        
        dates = Dates()
        ss = dates.session()
        records = ss.query(dates.date_struct).all()
        for irec in records:
            date = irec.date
            if_table_obj = cffex_if_min(db_name=dbname,table_name=str(date))
            if not if_table_obj.check_table_exist():
                continue 
            print date
                
            min_table = pd.read_sql_table(str(date), if_table_obj.engine)
            tickers = min_table['id'].unique()
            
            
        
            sql = 'select distinct id from cffex_if_min.{0} order by id;'.format(str(date))
            tickers = if_table_obj.execute_sql(sql)
            orders = [ irec[0] for irec in tickers ]
            to_be_inserted = [date,]
            to_be_inserted.extend(orders)
            print to_be_inserted
            self.insert_listlike(self.future_order_struct, to_be_inserted, True)
        ss.close()
        
def import_trading_days():
    
    infile = r'/media/xudi/software/future/dates.csv'
    df = pd.read_csv(infile,parse_dates = [0])
    df['date'] = df['date'].apply(lambda x: int(x.year * 10000 + x.month * 100 + x.day))
    print df.head()
    dates = Dates()
    df.to_sql('trading_days',dates.engine,index = False,if_exists = 'append',chunksize = 2048) 
    
def set_future_order_if():
        
    fo = futureOrder('if') 
    fo.set_order_cffex_if()   

def erase_invalid_days_if():
    dbname = 'cffex_if'
    dates = Dates()
    invalids = []
    ss = dates.session()
    records = ss.query(dates.date_struct).all()
    for irec in records:
        date = irec.date
        if_table_obj = cffex_if(db_name=dbname,table_name=str(date))
        if not if_table_obj.check_table_exist():
            invalids.append(date)
    print 'total = ',len(records),'reserved = ',len(records) - len(invalids)
    dates.delete_lists(dates.date_struct, invalids)
   
def adjust_days():
    import_trading_days()
    erase_invalid_days_if()
    set_future_order_if()
    
if __name__ == '__main__':
    
    dates = Dates()
    print dates.get_trading_day_list()
    
    order = futureOrder('if')
    print order.get_exchange_rollling_day_if()
    
    