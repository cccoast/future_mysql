import pandas as pd
import numpy as np
from future_table_struct import data_model_tick
from stock_table_struct import stock_data_model_stock_price
import matplotlib.pyplot as plt
from itertools import chain
from stock import Ticker as StockTicker
from misc import run_paralell_tasks
import ShmPython


def get_suspension_days(ipckey, ins_index = 25):
    #from shm api
    shm_api = ShmPython.Shm(ipckey)
    shm_header = shm_api.getHeader()
    trading_day_list = pd.Series(shm_api.getTradingDayList())[:-1]
    tradingDay2NDay = pd.Series(index = trading_day_list,data = range(len(trading_day_list)))
    spots_count_perday = shm_header.getSpotsCountPerDay()
    spots_count = shm_header.getSpotsCount()
    volume_index = shm_api.id2index_ind(113)
    datas = shm_api.fetchIntDataList(volume_index,ins_index,0,spots_count - spots_count_perday)
    suspension_days = [trading_day_list[i] for i in range(len(trading_day_list)) if datas[i] < 0.01]
    #from database
    ins_list = shm_header.getInstrumentsList()
    stock_ticker = StockTicker()
    ins_name = stock_ticker.id2name(ins_list[ins_index])
    sql = "select effective_date,price_type_code from b_stock_reprice where effective_date >= 20100104 and effective_date <= 20111230 and stock_code = '{0}'"\
                .format(ins_name)
    table = stock_data_model_stock_price()
    df = pd.read_sql(sql,table.engine,index_col = ['effective_date'])
    if len(df) == 0:
        df = df.reindex(trading_day_list,method='pad')
        df.iloc[:,:] = 0
    elif ( len(df.index) < len(trading_day_list) ):
        df = df.reindex(trading_day_list,method = 'pad')
        df['price_type_code'].fillna(0,inplace=True)
    df['price_type_code'] = df['price_type_code'].astype(np.int)
    a = set(suspension_days)
    sub_df = df[df['price_type_code'] == 0]
    b = set(sub_df.index)
    if a != b:
        print ins_index,ins_name,False
        print suspension_days
        print sorted(set(sub_df.index))
    else:
        print ins_index,True
     
def get_table_from_sql_db(db_name, table_name):
    #     dtype = {1:np.float,2:np.float,4:np.float,3:np.int,5:np.int,6:np.int,7:np.int}
    table = data_model_tick(db_name, table_name)
    df = pd.read_sql_table(table_name, table.engine)
    print [type(df.iloc[0][i]) for i in df.columns]
    print isinstance(df.iloc[0]['BidPrice'], np.float64)
    print df.head()


def check_trading_day_list(ipckey):
    shm_api = ShmPython.Shm(ipckey)
    trading_day_list = pd.Series(shm_api.getTradingDayList())
    print trading_day_list


def get_data_from_memory(ind_id, ipckey, ins_id, day=None):

    shm_api = ShmPython.Shm(ipckey)
    shm_header = shm_api.getHeader()
    ins_index = shm_api.id2index_ins(ins_id)
    trading_day_list = pd.Series(shm_api.getTradingDayList())
    spots_count_perday = shm_header.getSpotsCountPerDay()
    spots_count = shm_header.getSpotsCount()
    if day is not None:
        nth_day = trading_day_list[trading_day_list == day].index[0]
        start_spot = nth_day * spots_count_perday
        end_spot = start_spot + spots_count_perday
    else:
        nth_day = 0
        start_spot = 0
        #         end_spot = shm_api.getLastSpot(0)
        end_spot = spots_count - spots_count_perday

    print ind_id, nth_day, spots_count_perday, spots_count, start_spot, end_spot
    ind_index = shm_api.id2index_ind(ind_id)
    unit_size = shm_header.getIndicatorsUnitSizeList()
    if unit_size[ind_index] == 8:
        datas = shm_api.fetchDoubleDataList(ind_index, ins_index, start_spot,end_spot)
    else:
        datas = shm_api.fetchIntDataList(ind_index, ins_index, start_spot,end_spot)
    
    if len(datas) > 100000:
        datas = datas[::120]
        print 'datas too long, sample by 120 spots'
    print datas
    plt.plot(datas)
    plt.show()



def get_nostruct_data_from_memory(ipckey, nostruct_ipckey):
    shm_api = ShmPython.Shm(ipckey)
    shm_header = shm_api.getHeader()
    nostruct_shm_api = ShmPython.NoStructShm(nostruct_ipckey, 0x1503)

    ins_list = shm_header.getInstrumentsList()
    ind_list = shm_header.getIndicatorsList()
    trading_days_list = nostruct_shm_api.getTradingDayList()
    print trading_days_list

    ins_name_list = [nostruct_shm_api.id2name_ind(i) for i in ind_list]
    ind_name_list = [nostruct_shm_api.id2name_ins(i) for i in ins_list]
    print ins_name_list
    print ind_name_list

    milli_sec_list = [
        nostruct_shm_api.getSpot2MilliSec(i)
        for i in xrange(shm_header.getSpotsCountPerDay())
    ]
    spots_list = [nostruct_shm_api.getMilliSec2Spot(i) for i in milli_sec_list]

    print milli_sec_list
    print spots_list


def check_adjust(ipckey1, ipckey2):

    no_adjust_api = ShmPython.Shm(ipckey1)
    adjusted_api = ShmPython.Shm(ipckey2)
    header = no_adjust_api.getHeader()
    trading_day_list = no_adjust_api.getTradingDayList()
    trading_day_map = pd.Series(
        index=trading_day_list, data=range(len(trading_day_list)))
    spots_count_perday = header.getSpotsCountPerDay()
    from misc import get_nth_specical_weekday_in_daterange
    from itertools import chain
    weekdays = [i for i in chain(*(get_nth_specical_weekday_in_daterange(20140101, 20151231, 5, 1).values()))]
    first_roll_day = weekdays[-1]
    nth_first_roll_day = trading_day_map[first_roll_day]
    ind_index, ins_index = 0, 0
    datas1 = no_adjust_api.fetchDoubleDataList(
        ind_index, ins_index, 0, spots_count_perday * nth_first_roll_day)
    datas2 = adjusted_api.fetchDoubleDataList(
        ind_index, ins_index, 0, spots_count_perday * nth_first_roll_day)
    print 'cum adjust value = ', (datas2[0] - datas1[0])
    plt.plot(datas1, label='no_adjust')
    plt.plot(datas2, label='adjusted')
    plt.legend()
    plt.show()
    #pdiff.txt comes from what adjustor output, line 74
    df = pd.read_csv(
        '~/tmp/pdiff{}.txt'.format(ins_index + 1),
        index_col=0,
        header=None,
        sep=' ')
    print df[5].sum()

def test_stock_basic_indicators(ins_id,ipckey):
    ind_ids = [100,101,102,103,110,113,115]
    for ind_id in ind_ids:
        get_data_from_memory(ind_id,ipckey,ins_id)

def test_stock_ta_indicators(ins_id,ipckey):
    ind_ids = [120,121,122,123,124,137,138,139]
    for ind_id in ind_ids:
        get_data_from_memory(ind_id,ipckey,ins_id)
        
def test_stock_fundmental_indicators(ins_id,ipckey):
    ind_ids = [200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 210, 211, 212, 213]
    for ind_id in ind_ids:
        get_data_from_memory(ind_id,ipckey,ins_id)
 
def test_stock_unadjusted_price(ins_id,ipckey): 
    ind_ids = [250, 251, 252, 253]
    for ind_id in ind_ids:
        get_data_from_memory(ind_id,ipckey,ins_id)

def test_stock_all():
    ins_id = 600030
    ipckey = '0x0f0f0021'
    test_stock_basic_indicators(ins_id,ipckey)
    test_stock_ta_indicators(ins_id,ipckey)
    test_stock_fundmental_indicators(ins_id,ipckey)
    test_stock_unadjusted_price(ins_id,ipckey)
       
if __name__ == '__main__':
    #     get_table_from_sql_db('cffex_if','20150119')
    #     get_data_from_memory('0x0f0f0005')
    #     get_nostruct_data_from_memory('0x0f0f0002','0x0e0e0002')
    #     check_trading_day_list('0x0f0f0002')
    #     check_adjust('0x0f0f0002','0x0f0f0004')
    #     get_table_from_sql_db('shfex_au','20140228')
    
#     for ins_index in range(800):
#         get_suspension_days('0x0f0f0017',ins_index)
    
    get_data_from_memory(0,'0x0f0f0001',110010001)
#     get_nostruct_data_from_memory('0x0f0f0020','0x0e0e0020')
    