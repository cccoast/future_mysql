import pandas as pd
from multiprocessing import Process

timestamp2int = lambda x: x.year * 10000 + x.month * 100 + x.day
get_year_month_day = lambda x: (int(x/10000),int((x%10000)/100),int(x%100))

cffex_tickers = ['if', 'tf', 'ic', 'ih']
shfex_tickers = ['au', 'ag', 'cu', 'al', 'zn', 'rb', 'ru']


def sorted_dict(indict, sort_value_index=1, reverse=True):
    return sorted(
        indict.items(), key=lambda x: x[sort_value_index], reverse=reverse)


def run_paralell_tasks(func, iter_args):
    tasks = []
    for iarg in iter_args:
        task = Process(target=func, args=(iarg,))
        tasks.append(task)
    for itask in tasks:
        itask.start()
    for itask in tasks:
        itask.join()


def get_special_month_day(indate):
    if not isinstance(indate, pd.tslib.Timestamp):
        t = pd.to_datetime(str(indate))
        return t.day
    else:
        return indate.day


def get_special_weekday(indate):
    if not isinstance(indate, pd.tslib.Timestamp):
        return pd.to_datetime(str(indate)).isoweekday()
    else:
        return indate.isoweekday()


def get_dates_range_timestamp(start_date, end_date):
    return pd.date_range(str(start_date), str(end_date), freq='D')


def get_dates_range_int(start_date, end_date):
    return map(timestamp2int,
               pd.date_range(str(start_date), str(end_date), freq='D'))


def get_specical_weekday_in_daterange(start_day, end_day, weekday):
    dates = pd.Series( index = filter( lambda x:get_special_weekday(x) == weekday, \
                                         get_dates_range_timestamp(start_day,end_day) ) )
    return dates


def get_specical_monthday_in_date_range(start_day, end_day, month_day):
    dates = pd.Series( index = filter( lambda x:get_special_month_day(x) == month_day, \
                                         get_dates_range_timestamp(start_day,end_day) ) )
    ret = {}
    for year, year_dates in dates.groupby(lambda x: x.year):
        ret[year] = map(timestamp2int, year_dates.index)
    return ret


def get_nth_specical_weekday_in_daterange(start_day, end_day, weekday, nth):
    dates = get_specical_weekday_in_daterange(start_day, end_day, weekday)
    ret = {}
    for year, year_dates in dates.groupby(lambda x: x.year):
        #         print year,year_dates
        ret[year] = [ timestamp2int(month_nth.index[nth]) for month,month_nth in year_dates.groupby(by = lambda x: x.month) \
                                                            if len(month_nth) > nth ]
    return ret


def get_first_bigger_day_than_special_monthday(dates, monthday):
    date_series = pd.Series(index=map(lambda x: pd.to_datetime(str(x)), dates))
    ret = []
    for year, year_dates in date_series.groupby(lambda x: x.year):
        for month, month_dates in year_dates.groupby(lambda x: x.month):
            first_bigger_day = filter(
                lambda x: timestamp2int(x) % 100 >= monthday, month_dates.index)
            if len(first_bigger_day) > 0:
                ret.append(timestamp2int(first_bigger_day[0]))
    return ret


if __name__ == '__main__':
    print get_nth_specical_weekday_in_daterange(20140101, 20151231, 5, 2)
    print get_specical_monthday_in_date_range(20140101, 20150101, 15)
    print get_first_bigger_day_than_special_monthday(
        get_dates_range_timestamp(20140101, 20150101), 15)
