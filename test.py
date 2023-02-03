
from multiprocessing import Process,freeze_support,Pool

import numpy as np
import misc

if __name__ == '__main__':
    import pandas as pd
    t = np.datetime64('2010-01-01T09:30:01',dtype='datetime64[s]')
    print(t.astype(int))
    t2 = np.array([0,1577957400],dtype = 'datetime64[s]')
    print(t2)
    
    
    