
import numpy as np

class DayMode(object):
    
    LOWWER_BOUND = 0
    UPPER_BOUND  = 46802
    INTERVAL     = 500
    BEGIN_MILLI_SEC   = 3600 * 9 * 1000
    
    CFFEX_BEGIN_MILLI_OF_DAY = 34200000 #9:30
    CFFEX_LUNCH_BREAK_BEGIN  = 41400000 #11:30
    CFFEX_LUNCH_BREAK_END    = 43200000 #13:00
    CFFEX_END_MILLI_OF_DAY   = 54000000 #15:00
    
    OTHER_BEGIN_MILLI_OF_DAY = 32400000 #9:00
    OTHER_LUNCH_BREAK_BEGIN  = 41400000 #11:30
    OTHER_LUNCH_BREAK_END    = 48600000 #13:30
    OTHER_END_MILLI_OF_DAY   = 54000000 #15:00
        
    class CFFEX(object):
        morning_begin = 1800
        morning_end   = 18000
        afternoon_begin = 28800
        afternoon_end = 45000
        break1 = 1800
        break2 = 10800
    
    
    class OtherBreak(object):
        morning_begin = 0
        morning_break_begin = 9000
        morning_break_end = 10800
        morning_end = 18000
        afternoon_begin = 32400
        afternoon_end = 43200
        break1 = 1800
        break2 = 14400
    
    def __init__(self):    
        
        self.cffex_time2point = [-1] * 46802
        self.other_time2point = [-1] * 46802
        
        count = 0
        
        for i in range(self.CFFEX.morning_begin,self.CFFEX.morning_end):
            self.cffex_time2point[i] = count
            count += 1
            
        for i in range(self.CFFEX.afternoon_begin,self.CFFEX.afternoon_end + 2):
            self.cffex_time2point[i] = count
            count += 1
        
        self.cffex_last = count
        count = 0
        
        for i in range(self.OtherBreak.morning_begin,self.OtherBreak.morning_break_begin):
            self.other_time2point[i] = count
            count += 1
            
        for i in range(self.OtherBreak.morning_break_end,self.OtherBreak.morning_end):
            self.other_time2point[i] = count
            count += 1

        for i in range(self.OtherBreak.afternoon_begin,self.OtherBreak.afternoon_end + 2):
            self.other_time2point[i] = count
            count += 1
        self.other_last = count
     
    def fcffex_time2spot(self,(hour,minn,sec,milli)):
        nth = int( ( ( hour * 3600000 + minn * 60000 + sec * 1000 + milli ) - self.BEGIN_MILLI_SEC ) / self.INTERVAL )
        if nth < self.LOWWER_BOUND or nth >= self.UPPER_BOUND:
            return -1
        else:
            return self.cffex_time2point[nth]
    
    def fother_time2spot(self,(hour,minn,sec,milli)):
        nth = int( ( ( hour * 3600000 + minn * 60000 + sec * 1000 + milli ) - self.BEGIN_MILLI_SEC ) / self.INTERVAL )
        if nth < self.LOWWER_BOUND or nth >= self.UPPER_BOUND:
            return -1
        else:
            return self.cffex_time2point[nth]
        
if __name__ == '__main__':
    import matplotlib.pyplot as plt 
    mode = DayMode()
    print mode.cffex_last,mode.other_last
    plt.plot( mode.other_time2point )
    plt.show()
    




