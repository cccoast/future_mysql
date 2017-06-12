import pandas as pd
import numpy as np
import ShmPython as sm
from table_struct import cffex_if
import matplotlib.pyplot as plt
import ShmPython

def get_table_from_sql_db(db_name,table_name):
    table = cffex_if(db_name,table_name)
    df = pd.read_sql_table(table_name,table.engine)
    print df[df["id"] == 'IF1502']['BidPrice']

def check_trading_day_list(ipckey):
    shm_api = sm.Shm(ipckey)
    trading_day_list = pd.Series(shm_api.getTradingDayList())
    print trading_day_list
    
def get_data_from_memory(ipckey,day = None):
    shm_api = sm.Shm(ipckey)
    shm_header = shm_api.getHeader()
    
    trading_day_list = pd.Series(shm_api.getTradingDayList())
    spots_count_perday = shm_header.getSpotsCountPerDay()
    if day is not None:
        nth_day = trading_day_list[ trading_day_list == day ].index[0]
        start_spot = nth_day * spots_count_perday
        end_spot = start_spot + spots_count_perday
    else:
        nth_day = 0
        start_spot = 0
        end_spot = shm_api.getLastSpot(0)
    
    print nth_day,spots_count_perday
    ind_id = 123
    ins_index = 0
    ind_index = shm_api.id2index_ind(ind_id)
    
    unit_size = shm_header.getIndicatorsUnitSizeList()

    if unit_size[ind_index] == 8:
        datas = shm_api.fetchDoubleDataList(ind_index,ins_index,start_spot,end_spot)
    else:
        datas = shm_api.fetchIntDataList(ind_index,ins_index,start_spot,end_spot)
        
    plt.plot(datas)
    plt.show()

def get_nostruct_data_from_memory(ipckey,nostruct_ipckey):
    shm_api = ShmPython.Shm(ipckey)
    shm_header = shm_api.getHeader()
    nostruct_shm_api = ShmPython.NoStructShm(nostruct_ipckey,0x1503)
    
    ins_list = shm_header.getInstrumentsList()
    ind_list = shm_header.getIndicatorsList()
    trading_days_list = nostruct_shm_api.getTradingDayList()
    print trading_days_list
    
    ins_name_list = [nostruct_shm_api.id2name_ind(i) for i in ind_list]
    ind_name_list = [nostruct_shm_api.id2name_ins(i) for i in ins_list]
    print ins_name_list
    print ind_name_list
    
    milli_sec_list = [nostruct_shm_api.getSpot2MilliSec(i) for i in xrange(shm_header.getSpotsCountPerDay())]
    spots_list     = [nostruct_shm_api.getMilliSec2Spot(i) for i in milli_sec_list]
    
    print milli_sec_list
    print spots_list
        
if __name__ == '__main__':
#     get_table_from_sql_db('cffex_if','20150119')
#     get_data_from_memory('0x0f0f0005')
#     get_nostruct_data_from_memory('0x0f0f0002','0x0e0e0002')
    check_trading_day_list('0x0f0f0002')
    