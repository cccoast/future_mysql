import pandas as pd

timestamp2int = lambda x: x.year * 10000 + x.month * 100 + x.day

def get_special_weekday(indate):
    if not isinstance(indate,pd.tslib.Timestamp):
        return pd.to_datetime(str(indate)).weekday()
    else:
        return indate.weekday()

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
    return [ timestamp2int(month_nth.index[nth]) for month,month_nth in dates.groupby(by = lambda x: x.month) ]
    
if __name__ == '__main__':
    print get_nth_specical_weekday_in_daterange(20140101,20141231,5,0)
    