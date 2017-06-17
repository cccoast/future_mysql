import pandas as pd

timestamp2int = lambda x: x.year * 10000 + x.month * 100 + x.day
get_year_month_day = lambda x: (int(x/10000),int((x%10000)/100),int(x%100))

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
    