import pandas as pd
import ShmPython as sm
from data_import import cffex_if
import matplotlib.pyplot as plt

def get_table_from_sql_db(db_name,table_name):
    table = cffex_if(db_name,table_name)
    df = pd.read_sql_table(table_name,table.engine)
    print df[df["id"] == 'IF1502']['BidPrice']

def get_data_from_memory(ipckey,day):
    shm_api = sm.Shm(ipckey)
    shm_header = shm_api.getHeader()
    
    trading_day_list = pd.Series(shm_api.getTradingDayList())
    nth_day = trading_day_list[ trading_day_list == day ].index[0]
    spots_count_perday = shm_header.getSpotsCountPerDay()
    
    print nth_day,spots_count_perday
    ind_id = 6
    ins_index = 0
    ind_index = shm_api.id2index_ind(ind_id)
    
    start_spot = nth_day * spots_count_perday 
    end_spot   = start_spot + spots_count_perday
    
    datas = shm_api.fetchDoubleDataList(ind_index,ins_index,start_spot ,end_spot)
    plt.plot(datas)
    plt.show()
    
if __name__ == '__main__':
    get_table_from_sql_db('cffex_if','20150119')
#     get_data_from_memory('0x0f0f0001',20150119)
    
    