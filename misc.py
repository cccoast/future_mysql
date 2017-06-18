import pandas as pd
from multiprocessing import Process

timestamp2int = lambda x: x.year * 10000 + x.month * 100 + x.day
get_year_month_day = lambda x: (int(x/10000),int((x%10000)/100),int(x%100))

cffex_tickers = ['if','tf','ic','ih']
shfex_tickers = ['au','ag','cu','al','zn','rb','ru']

def run_parelell_tasks(func,iter_args):
    tasks = []
    for iarg in iter_args:
        task = Process(target=func, args=(iarg,))
        tasks.append(task)
    for itask in tasks:
        itask.start()
    for itask in tasks:
        itask.join()
        
class Ticker(object):
    
    def __init__(self):
        self.tid_dict = {}
        self.cffex = ['if','tf','ic','ih']
        j = 1
        for i in self.cffex:
            self.tid_dict[i] = 11000 + j
            j += 1
        self.shfex = ['au','ag','cu','al','zn','rb','ru']
        j = 1
        for i in self.shfex:
            self.tid_dict[i] = 12000 + j
            j += 1
            
    def get_market_id(self,ticker):
        ticker = str.lower(ticker)[:2]
        if ticker in self.cffex:
            return int(self.tid_dict[ticker] / 1000)
        if ticker in self.shfex:
            return int(self.tid_dict[ticker] / 1000)
        return None
    
    def get_market_no(self,ticker):
        ticker = str.lower(ticker)[:2]
        if ticker in self.cffex:
            return int(self.tid_dict[ticker] % 1000)
        if ticker in self.shfex:
            return int(self.tid_dict[ticker] % 1000)
        return None
    
    def get_id(self,ticker):
        market_id,market_no = self.get_market_id(ticker),self.get_market_no(ticker)
        last = int(filter(lambda x: str.isdigit(x),ticker))
        if market_id:
            return ( market_id * 1000 + market_no ) * 10000 + last  
        else:
            return None 
        
    def get_dbname(self,ticker,level = 'tick'):   
        ticker = str.lower(ticker)[:2]
        if ticker in self.cffex:  
            ret = '_'.join(('cffex',ticker))
        elif ticker in self.shfex:
            ret =  '_'.join(('shfex',ticker))
        else:
            return None
        if level == 'tick':
            return ret
        elif level.endswith('min'):
            return '_'.join((ret,'min'))
        else:
            return '_'.join((ret,'day'))
        
def get_special_weekday(indate):
    if not isinstance(indate,pd.tslib.Timestamp):
        return pd.to_datetime(str(indate)).isoweekday()
    else:
        return indate.isoweekday()

def get_dates_range_timestamp(start_date,end_date):
    return pd.date_range(str(start_date),str(end_date),freq = 'D')

def get_dates_range_int(start_date,end_date):
    return map(timestamp2int,pd.date_range(str(start_date),str(end_date),freq = 'D') )

def get_specical_weekday_in_daterange(start_day,end_day,weekday):
    dates = pd.Series( index = filter( lambda x:get_special_weekday(x) == weekday, \
                                         get_dates_range_timestamp(start_day,end_day) ) ) 
    return dates

def get_nth_specical_weekday_in_daterange(start_day,end_day,weekday,nth):
    dates = get_specical_weekday_in_daterange(start_day,end_day,weekday)
    ret = {}
    for year,year_dates in dates.groupby(lambda x:x.year):
#         print year,year_dates
        ret[year] = [ timestamp2int(month_nth.index[nth]) for month,month_nth in year_dates.groupby(by = lambda x: x.month) \
                                                            if len(month_nth) > nth ]
    return ret

if __name__ == '__main__':
    print get_specical_weekday_in_daterange(20140101,20151231,5)
    print get_nth_specical_weekday_in_daterange(20140101,20151231,5,2)
    