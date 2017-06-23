import pandas as pd
import numpy as np
from table_struct import data_model_tick
import matplotlib.pyplot as plt
import ShmPython


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


def get_data_from_memory(ipckey, day=None):
    shm_api = ShmPython.Shm(ipckey)
    shm_header = shm_api.getHeader()

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

    print nth_day, spots_count_perday, spots_count, start_spot, end_spot
    ind_id = 120
    ins_index = 0
    ind_index = shm_api.id2index_ind(ind_id)

    unit_size = shm_header.getIndicatorsUnitSizeList()

    if unit_size[ind_index] == 8:
        datas = shm_api.fetchDoubleDataList(ind_index, ins_index, start_spot,
                                            end_spot)
    else:
        datas = shm_api.fetchIntDataList(ind_index, ins_index, start_spot,
                                         end_spot)

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
    weekdays = [
        i
        for i in chain(*(get_nth_specical_weekday_in_daterange(
            20140101, 20151231, 5, 1).values()))
    ]
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


if __name__ == '__main__':
    #     get_table_from_sql_db('cffex_if','20150119')
    #     get_data_from_memory('0x0f0f0005')
    #     get_nostruct_data_from_memory('0x0f0f0002','0x0e0e0002')
    #     check_trading_day_list('0x0f0f0002')
    #     check_adjust('0x0f0f0002','0x0f0f0004')
    #     get_table_from_sql_db('shfex_au','20140228')
    get_data_from_memory('0x0f0f0016')
