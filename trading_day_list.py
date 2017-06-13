import dbBase as db

from sqlalchemy import Column, Integer, String, FLOAT
from sqlalchemy import Table

import pandas as pd
import numpy as np
from table_struct import cffex_if,cffex_if_min
from itertools import chain

from misc import get_nth_specical_weekday_in_daterange,timestamp2int,get_year_month_day

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
        return map(lambda x: int(x.date), records)

class futureOrder(db.DB_BASE):
    
    def __init__(self,market_id):
        db_name,table_name = 'dates','future_order' + '_' + str.lower(market_id[:2])
        super(futureOrder,self).__init__(db_name)
        self.table_name = table_name
        self.table_struct = Table(table_name,self.meta,
                     Column('date',Integer,primary_key = True,autoincrement = False),
                     Column('if0001',String(20)),
                     Column('if0002',String(20)),
                     Column('if0003',String(20)),
                     Column('if0004',String(20)),
                    )
        
        self.future_order_struct = self.quick_map(self.table_struct)
    
    def get_exchange_rollling_day_if(self,offset = 0):
        weekday,nth = 5,2
        dates = Dates()
        trading_days = dates.get_trading_day_list()
#         #this is a [] set, need not to do the offset
#         last_trading_day = timestamp2int( pd.to_datetime(str(trading_days[-1])) + pd.to_timedelta(1,unit = 'D'))
        last_trading_day = trading_days[-1]
        print 'first day = ',trading_days[0],' last_trading_day = ',last_trading_day
        weekdays = [ i for i in chain(*get_nth_specical_weekday_in_daterange(trading_days[0], last_trading_day, weekday, nth).values())]
        exchange_rolling_days = map(lambda x: dates.get_first_less_than(x),weekdays)
        trading_days_series = pd.Series(index = trading_days,data = range(len(trading_days)))
        roll_days_dict =  { year:{} for year in  np.unique( map(lambda x: int(x/10000), exchange_rolling_days) ) }
        for day in exchange_rolling_days:
            year = int(day/10000)
            roll_days_dict[year][ int((day%10000)/100) ] = trading_days[ trading_days_series[ day ] - offset ] \
                                                            if trading_days_series[ day ] - offset >= 0 else day
        return roll_days_dict
        
    def set_order_cffex_if(self,method = 'fixed_days',fixed_days = 1):
        #use min tables instead of tick tables to accelerate!
        dbname = 'cffex_if_min'
        dates = Dates()
        rolling_day = self.get_exchange_rollling_day_if(fixed_days)
        exchang_rolling_day = self.get_exchange_rollling_day_if(0)
        trading_day_list = dates.get_trading_day_list()
        for date in trading_day_list:
            if_table_obj = cffex_if_min(db_name=dbname,table_name=str(date))
            if not if_table_obj.check_table_exist():
                continue 
            year,month,day = get_year_month_day(date)
            print date,rolling_day[year][month],exchang_rolling_day[year][month]
            if method == 'fixed_days':
                sql = 'select distinct id from cffex_if_min.{0} order by id;'.format(str(date))
                tickers = if_table_obj.execute_sql(sql)
                orders = [ irec[0] for irec in tickers ]
                if date > int(rolling_day[year][month]) and date <= int(exchang_rolling_day[year][month]):
                    orders[0],orders[1] = orders[1],orders[0]
                to_be_inserted = [date,]
                to_be_inserted.extend(orders)
                print to_be_inserted
                self.insert_listlike(self.future_order_struct, to_be_inserted, True)
        
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
    order = futureOrder('if')
    print order.get_exchange_rollling_day_if(offset = 1)
    order.set_order_cffex_if()
    
    