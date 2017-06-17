import dbBase as db

from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import create_engine,Table
from sqlalchemy.orm import sessionmaker

import pandas as pd
import numpy as np
from table_struct import data_model_tick,data_model_min
from itertools import chain

from misc import get_nth_specical_weekday_in_daterange,timestamp2int,get_year_month_day

def get_all_table_names(dbname):
    sql = r"select table_name from information_schema.tables where table_schema='{0}' and table_type='base table';".format(dbname)
    connect_str = "mysql+pymysql://xudi:123456@localhost:3306/{0}".format(dbname)    
    engine = create_engine(connect_str,echo = False)
    session = sessionmaker(bind=engine)
    ss = session()
    records = ss.execute(sql)
    ss.close()
    return [j for j in chain(*[ i for i in records])]
    
class Dates(db.DB_BASE):
    
    def __init__(self,db_name = 'dates',table_name = 'trading_days'):
        
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

class AllTradingDays(Dates):
    
    def __init__(self,):
        db_name = 'dates'
        table_name = 'all_trading_days'
        super(AllTradingDays,self).__init__(db_name,table_name)
        
class futureOrder(db.DB_BASE):
    
    def __init__(self,market_id):
        db_name,table_name = 'dates','future_order' + '_' + str.lower(market_id[:2])
        self.table_name = table_name
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
        
    def set_order_cffex_if(self,method = 'fixed_days',fixed_days = 1,force_reload = False):
        #use min tables instead of tick tables to accelerate!
        dbname = 'cffex_if_min'
        dates = Dates()
        rolling_day = self.get_exchange_rollling_day_if(fixed_days)
        exchang_rolling_day = self.get_exchange_rollling_day_if(0)
        trading_day_list = dates.get_trading_day_list()
        
        #if date order already exists, then skip
        print 'force_reload = ',force_reload
        if force_reload:
            all_records = self.query_obj(self.future_order_struct)
            self.delete_lists_obj(all_records)
        
        if not force_reload:
            exist_order_dates = set(map(lambda x:int(x.date),self.query_obj(self.future_order_struct))) 
            
        for date in trading_day_list:
            if_table_obj = data_model_min(db_name=dbname,table_name=str(date))
            if not if_table_obj.check_table_exist():
                continue 
            year,month,day = get_year_month_day(date)
            
            if not force_reload:
                if date in exist_order_dates:
                    continue
            
            print date,rolling_day[year][month],exchang_rolling_day[year][month]
            if method == 'fixed_days':
                sql = 'select distinct id from data_model_min.{0} order by id;'.format(str(date))
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
    dates = AllTradingDays()
    df.to_sql('all_trading_days',dates.engine,index = False,if_exists = 'append',chunksize = 2048) 
    
def set_future_order_if(force_reload = False):
        
    fo = futureOrder('if') 
    fo.set_order_cffex_if(force_reload = force_reload)   

def set_valid_days(dbname):
    all_dates = AllTradingDays()
    trading_day_list = all_dates.get_trading_day_list()
    valids = []
    for date in trading_day_list:
        if_table_obj = data_model_tick(db_name=dbname,table_name=str(date))
        if if_table_obj.check_table_exist():
            valids.append(date)
    print 'valids = ',len(valids)
    
    #remove all
    if_dates = Dates()
    all_records = if_dates.query_obj(if_dates.date_struct)
    if_dates.delete_lists_obj(all_records)
    
    #reinsert 
    if_dates.insert_lists(if_dates.date_struct, valids, True)

def erase_invalid_table(dbname,level = 'tick'):
    tables = get_all_table_names(dbname)
    if level == 'tick':
        model = data_model_tick
    elif level == '1min':
        model = data_model_min
    else:
        return 0
    
    print 'start to erase'
    valid_trading_days = set(map(lambda x:str(x),AllTradingDays().get_trading_day_list()))
    for i in tables:
        if i not in valid_trading_days:
            to_be_erased = model(dbname,i)
            to_be_erased.drop_table(i)
            print 'drop table = ',i
            
def adjust_if_days(dbname):
    set_valid_days(dbname)
    erase_invalid_table(dbname)
    set_future_order_if(force_reload = True)

def check_cffex_shfex_align():
    trading_day_list = (AllTradingDays().get_trading_day_list())
    a = get_all_table_names('cffex_if')
    b = get_all_table_names('shfex_au')
    print a
    print b
    a = set(a)
    b = set(b)
    diff =  (a | b) - (a & b)
    odd = sorted(list(diff))
    print odd
    #what a has but b does not
    print 'what a has but b does not = ', sorted(list(a & diff))
    #what b has but a does not
    print 'what b has but a does not = ', sorted(list(b & diff))

if __name__ == '__main__':
    check_cffex_shfex_align()
    
    