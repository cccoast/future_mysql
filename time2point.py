
class DayMode(object):
    
    LOWWER_BOUND = 0
    UPPER_BOUND  = 46802
    INTERVAL     = 500
    BEGIN_MILLI_SEC   = 3600 * 9 * 1000
        
    class CFFEX(object):
        morning_begin = 1800
        morning_end   = 18000
        afternoon_begin = 28800
        afternoon_end = 43200
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
            
        for i in range(self.CFFEX.afternoon_begin,self.CFFEX.afternoon_end):
            self.cffex_time2point[i] = count
            count += 1
        
        count = 0
        
        for i in range(self.OtherBreak.morning_begin,self.OtherBreak.morning_break_begin):
            self.other_time2point[i] = count
            count += 1
            
        for i in range(self.OtherBreak.morning_break_end,self.OtherBreak.morning_end):
            self.other_time2point[i] = count
            count += 1

        for i in range(self.OtherBreak.afternoon_begin,self.OtherBreak.afternoon_end):
            self.other_time2point[i] = count
            count += 1
            
if __name__ == '__main__':
    import matplotlib.pyplot as plt 
    mode = DayMode()
    plt.plot( mode.other_time2point )
    plt.show()
    




