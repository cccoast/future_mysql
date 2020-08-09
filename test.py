
from multiprocessing import Process,freeze_support,Pool

import numpy as np
import misc
    
    
trading_day_list = range(20010101,20010200)
multi_processor_numbers = 4
sub_day_list = map(list,np.split(trading_day_list, [
                    len(trading_day_list) / multi_processor_numbers * i
                    for i in range(1, multi_processor_numbers)
                ]))

def test1():
#     misc.run_paralell_tasks(_print,sub_day_list)
    misc.run_paralell_tasks(_print,sub_day_list)
    


if __name__ == '__main__':
    def _print(_arry):
        print _arry
    freeze_support()
    pool = Pool(4)
    pool.map(_print,sub_day_list)
    
    