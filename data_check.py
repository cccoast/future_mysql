
import os,sys
from dbBase import DB_BASE
parent_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
if parent_path not in sys.path:
    sys.path.append(parent_path)

import pandas as pd
import numpy as np
from future_table_struct import data_model_tick,data_model_min,data_model_day
from tushare_feed.models import stock_reprice as stock_data_model_stock_reprice  
from tushare_feed.models import sw_industry,stock 

import matplotlib.pyplot as plt
from itertools import chain
from stock import StockTicker,StockIndex,StockIndustry
from misc import run_paralell_tasks
# import ShmPython
import ShmPybind
import matplotlib.pyplot as plt

from matplotlib import rcParams
rcParams['font.family'] = 'SimHei'

st = StockTicker()

#test suspension days
def test_suspension_days(ipckey,default_set = '000016.SH'):
    #from shm api
    shm_api = ShmPybind.Shm(ipckey)
    shm_header = shm_api.getHeader()
    trading_day_list = pd.Series(shm_api.getTradingDayList())[:-1]
    trading_day_map  = pd.Series(np.arange(len( shm_api.getTradingDayList() )),index = shm_api.getTradingDayList())
    trading_day_sets = set(trading_day_list.values)
    spots_count_perday = shm_header.getSpotsCountPerDay()
    spots_count = shm_header.getSpotsCount()
    volume_index = shm_api.id2index_ind(113)
    si = StockIndex() 
    
    for id in si.indexName2insIDs('000016.SH'):
        ins_index = shm_api.id2index_ins(id)
        if ins_index < 0 :
            continue        
        datas = shm_api.fetchIntDataList(volume_index,ins_index,0,spots_count - spots_count_perday)
        suspension_days = [trading_day_list[i] for i in range(len(trading_day_list)) if datas[i] < 1.0]
        print(len(datas))
        #from database
        ins_list = shm_header.getInstrumentsList()
        stock_ticker = StockTicker()
        ins_name = stock_ticker.id2name(ins_list[ins_index])
        sql = '''select effective_date,volume from t_stock_reprice 
                 where effective_date >= 20100104 and effective_date <= 20230120 
                 and stock_code = '{0}' 
              '''.format(ins_name)
        table = stock_data_model_stock_reprice()
        df = pd.read_sql(sql,table.engine,index_col = ['effective_date'])
        
        desired_suspension_days =  trading_day_sets - set(df.index.values) 
        desired_suspension_days_volume = [ shm_api.fetchIntData(volume_index,ins_index,trading_day_map[i]) for i in desired_suspension_days ]
        
        print(len(desired_suspension_days), len([ x for x in desired_suspension_days_volume if x > 0.9]))
    
def check_trading_day_list(ipckey):
    shm_api = ShmPybind.Shm(ipckey)
    trading_day_list = pd.Series(shm_api.getTradingDayList())
    print(trading_day_list)

def get_data_from_memory(ind_id, ipckey, ins_id, start_date = 20100131, end_date = 20230101, day = None):
    shm_api = ShmPybind.Shm(ipckey)
    shm_header = shm_api.getHeader()
    ins_index = shm_api.id2index_ins(ins_id)
    trading_day_list = [ i for i in pd.Series(shm_api.getTradingDayList()) if start_date < i < end_date ]
    spots_count_perday = shm_header.getSpotsCountPerDay()
    spots_count = shm_header.getSpotsCount()
    if day is not None:
        nth_day = trading_day_list[trading_day_list == day].index[0]
        start_spot = nth_day * spots_count_perday
        end_spot = start_spot + spots_count_perday
    else:
        nth_day = 0
        start_spot = 0
        end_spot = spots_count - spots_count_perday

    ind_index = shm_api.id2index_ind(ind_id)
    if ind_index < 0:
        return 
    unit_size = shm_header.getIndicatorsUnitSizeList()
    print(ind_id, nth_day, unit_size[ind_index], ind_index, ins_index, start_spot,end_spot)
    if unit_size[ind_index] == 8:
        datas = shm_api.fetchDoubleDataList(ind_index, ins_index, start_spot,end_spot)
    else:
        if ind_id == 113:
            datas = shm_api.fetchIntDataList(ind_index, ins_index, start_spot,end_spot)
        else:
            datas = shm_api.fetchFloatDataList(ind_index, ins_index, start_spot,end_spot)
    
    print(datas[::-1])
    if len(datas) > 100000:
        datas = datas[::120]
        print('datas too long, sample by 120 spots')
    plt.plot(datas)
    plt.show()


def get_nostruct_data_from_memory(ipckey, nostruct_ipckey):
    shm_api = ShmPybind.Shm(ipckey)
    shm_header = shm_api.getHeader()
    nostruct_shm_api = ShmPybind.NoStructShm(nostruct_ipckey, 0x1503)

    ins_list = shm_header.getInstrumentsList()
    ind_list = shm_header.getIndicatorsList()
    trading_days_list = nostruct_shm_api.getTradingDayList()
    print(trading_days_list)
    
    ind_name_list = [nostruct_shm_api.id2name_ind(i) for i in ind_list]
    ins_name_list = [nostruct_shm_api.id2name_ins(i) for i in ins_list]
    print(ins_name_list)
    print(ind_name_list)

    milli_sec_list = [
        nostruct_shm_api.getSpot2MilliSec(i)
        for i in range(shm_header.getSpotsCountPerDay())
    ]
    spots_list = [nostruct_shm_api.getMilliSec2Spot(i) for i in milli_sec_list]

    print(milli_sec_list)
    print(spots_list)

def check_adjust(ipckey1, ipckey2):
    no_adjust_api = ShmPybind.Shm(ipckey1)
    adjusted_api = ShmPybind.Shm(ipckey2)
    header = no_adjust_api.getHeader()
    trading_day_list = no_adjust_api.getTradingDayList()
    trading_day_map = pd.Series(
        index=trading_day_list, data=list(range(len(trading_day_list))))
    spots_count_perday = header.getSpotsCountPerDay()
    from misc import get_nth_specical_weekday_in_daterange
    from itertools import chain
    weekdays = [i for i in chain(*(list(get_nth_specical_weekday_in_daterange(20140101, 20151231, 5, 1).values())))]
    first_roll_day = weekdays[-1]
    nth_first_roll_day = trading_day_map[first_roll_day]
    ind_index, ins_index = 0, 0
    datas1 = no_adjust_api.fetchDoubleDataList(
        ind_index, ins_index, 0, spots_count_perday * nth_first_roll_day)
    datas2 = adjusted_api.fetchDoubleDataList(
        ind_index, ins_index, 0, spots_count_perday * nth_first_roll_day)
    print('cum adjust value = ', (datas2[0] - datas1[0]))
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
    print(df[5].sum())
    
#######################################unit test#############################################

##test price
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
##############################################################
 
def test_stock_unadjusted_price(ins_id,ipckey): 
    ind_ids = [250, 251, 252, 253]
    for ind_id in ind_ids:
        get_data_from_memory(ind_id,ipckey,ins_id)

def test_stock_all():
    ins_id = 600030
    ipckey = '0x0f0f0001'
    test_stock_basic_indicators(ins_id,ipckey)
    test_stock_ta_indicators(ins_id,ipckey)
    test_stock_fundmental_indicators(ins_id,ipckey)
#     ipckey = '0x0f0f0003'
#     test_stock_unadjusted_price(ins_id,ipckey)
 
def get_ind_from_db(ins_name,ind_name,start_date,end_date):
    db = DB_BASE('tushare')
    sql = '''select effective_date,{} from t_stock_price 
             where effective_date >= {} and effective_date <= {} 
             and stock_code = '{}'
             '''.format(ind_name,start_date,end_date,ins_name)
    print(sql)
    df = pd.read_sql(sql,db.engine)
    df['effective_date'] = df['effective_date'].astype(int)
    df.set_index('effective_date',inplace = True)
    print(df.values)
    df.plot()
    plt.show()
    
def plot_adj_factor(ipc):
    shm = ShmPybind.Shm(ipc)
    length = shm.getLastSpot(0)
    ind_index = shm.id2index_ind(190)
    datas = shm.fetchDoubleDataList(ind_index,10,0,length)
    plt.plot(datas)
    plt.show()
    
    
def plot_all(ipc):      
    des_path = os.path.join('I:\data\datacheck',ipc)
    if not os.path.exists(des_path):
        os.mkdir(des_path)
    db = stock()
    stock_df = pd.read_sql_table(db.table_name,db.engine,index_col = 'stock_id',) 
    db2 = sw_industry()
    index_df = pd.read_sql_table(db2.table_name,db.engine)
    index_df.index = index_df['industry_code'].apply(lambda x: int(x.split('.')[0]))   
    shm = ShmPybind.Shm(ipc)
    header = shm.getHeader()
    figsize = (12,8)
    rows,cols = 3,4
    ind_index = shm.id2index_ind(103)
    ins_list  = header.getInstrumentsList()
    ins_count = header.getInstrumentsCount()
    length = shm.getLastSpot(0)
    for step in range(0,header.getInstrumentsCount(),12):
        print(step)
        cur = step
        axs = plt.figure(figsize=figsize, constrained_layout=True).subplots(rows, cols)
        for row in axs:
            for ax in row:
                datas = shm.fetchDoubleDataList(ind_index,cur,0,length)
                try:
                    title = ' '.join(stock_df.loc[ins_list[cur],['stock_code','stock_name']].values)
                except:
                    if int(ins_list[cur]) < 1000000:
                        title = ' '.join(index_df.loc[ins_list[cur],['industry_code','l1_name',]].values)
                    else:
                        title = str(ins_list[cur])
                ax.set_title(title)
                ax.plot(datas)
                cur += 1
                if cur >= ins_count:
                    plt.savefig(os.path.join(des_path,str(step)))
                    return
        plt.savefig(os.path.join(des_path,str(step)))
         
if __name__ == '__main__':
    get_nostruct_data_from_memory('0x0f0f0001','0x0e0e0001')
    