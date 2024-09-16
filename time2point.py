import numpy as np

class DayMode(object):

    LOWWER_BOUND = 0
    UPPER_BOUND = 46802
    INTERVAL = 500
    SPOT_FREQ = int( 1000 / INTERVAL)
    BEGIN_MILLI_SEC = 3600 * 9 * 1000

    class CFFEX_before_2016(object):
        morning_begin = 1800      #9:15
        morning_end = 18000       #11:30
        afternoon_begin = 28800   #1:00
        afternoon_end = 45000     #15:15
        break1 = 1800             #break length
        break2 = 10800            #break length
        
        CFFEX_BEGIN_MILLI_OF_DAY = 33300000  #9:15
        CFFEX_LUNCH_BREAK_BEGIN  = 41400000  #11:30
        CFFEX_LUNCH_BREAK_END    = 43200000  #13:00
        CFFEX_END_MILLI_OF_DAY   = 54900000  #15:15
    
    class CFFEX_after_2016(object):
        morning_begin = 3600      #9:30
        morning_end = 18000       #11:30
        afternoon_begin = 28800   #1:00
        afternoon_end = 43200     #15:00
        break1 = 3600             #break length
        break2 = 10800            #break length
        
        CFFEX_BEGIN_MILLI_OF_DAY = 34200000  #9:30
        CFFEX_LUNCH_BREAK_BEGIN  = 41400000  #11:30
        CFFEX_LUNCH_BREAK_END    = 43200000  #13:00
        CFFEX_END_MILLI_OF_DAY   = 54000000  #15:00
    
    CFFEX = CFFEX_after_2016
    
    class OtherBreak(object):
        morning_begin = 0
        morning_break_begin = 9000
        morning_break_end = 10800
        morning_end = 18000
        afternoon_begin = 32400
        afternoon_end = 43200
        break1 = 1800            #break length 
        break2 = 14400           #break length
        
        OTHER_BEGIN_MILLI_OF_DAY = 32400000  #9:00
        OTHER_LUNCH_BREAK_BEGIN = 41400000  #11:30
        OTHER_LUNCH_BREAK_END = 48600000  #13:30
        OTHER_END_MILLI_OF_DAY = 54000000  #15:00

    def __init__(self,default_mode = 'after_2016'):
        
        if default_mode == 'after_2016':
            self.CFFEX = self.CFFEX_after_2016
        else:
            self.CFFEX = self.CFFEX_before_2016
            
        self.CFFEX_BEGIN_MILLI_OF_DAY = self.CFFEX.CFFEX_BEGIN_MILLI_OF_DAY  
        self.CFFEX_LUNCH_BREAK_BEGIN  = self.CFFEX.CFFEX_LUNCH_BREAK_BEGIN  
        self.CFFEX_LUNCH_BREAK_END    = self.CFFEX.CFFEX_LUNCH_BREAK_END  
        self.CFFEX_END_MILLI_OF_DAY   = self.CFFEX.CFFEX_END_MILLI_OF_DAY 

        self.cffex_time2point = [-1] * 46802
        self.other_time2point = [-1] * 46802

        self.cffex_point2time = [-1] * 46802
        self.other_point2time = [-1] * 46802

        count = 0

        for i in range(self.CFFEX.morning_begin, self.CFFEX.morning_end):
            self.cffex_time2point[i] = count
            self.cffex_point2time[count] = int(i * 1000 / self.SPOT_FREQ)   + self.BEGIN_MILLI_SEC
            count += 1    
        
        for i in range(self.CFFEX.afternoon_begin,self.CFFEX.afternoon_end + 2):
            self.cffex_time2point[i] = count
            self.cffex_point2time[count] = int(i * 1000 / self.SPOT_FREQ)  + self.BEGIN_MILLI_SEC
            count += 1

        self.cffex_last = count
#         print(self.cffex_last)
        
        count = 0
        for i in range(self.OtherBreak.morning_begin,self.OtherBreak.morning_break_begin):
            self.other_time2point[i] = count
            self.other_point2time[count] = int(i * 1000 / self.SPOT_FREQ)  + self.BEGIN_MILLI_SEC
            count += 1

        for i in range(self.OtherBreak.morning_break_end,self.OtherBreak.morning_end):
            self.other_time2point[i] = count
            self.other_point2time[count] = int(i * 1000/ self.SPOT_FREQ)  +  self.BEGIN_MILLI_SEC
            count += 1

        for i in range(self.OtherBreak.afternoon_begin,self.OtherBreak.afternoon_end + 2):
            self.other_time2point[i] = count
            self.other_point2time[count] = int(i * 1000/ self.SPOT_FREQ)   + self.BEGIN_MILLI_SEC
            count += 1

        self.other_last = count

    def fcffex_time2spot(self, xxx_todo_changeme):
        (hour, minn, sec, milli) = xxx_todo_changeme
        nth = int(((hour * 3600000 + minn * 60000 + sec * 1000 + milli) -
                   self.BEGIN_MILLI_SEC) / self.INTERVAL)
#         print(xxx_todo_changeme,nth)
        if nth < self.LOWWER_BOUND or nth >= self.UPPER_BOUND:
            return -1
        else:
            return self.cffex_time2point[nth]

    def fother_time2spot(self, xxx_todo_changeme1):
        (hour, minn, sec, milli) = xxx_todo_changeme1
        nth = int(((hour * 3600000 + minn * 60000 + sec * 1000 + milli) -
                   self.BEGIN_MILLI_SEC) / self.INTERVAL)
        if nth < self.LOWWER_BOUND or nth >= self.UPPER_BOUND:
            return -1
        else:
            return self.other_time2point[nth]

    def fcffex_spot2time(self, spot):
        return self.cffex_point2time[spot]

    def fother_spot2time(self, spot):
        return self.other_point2time[spot]
    
    def fcffex_spot2time_hhmmssms(self, spot):
        hhmmss = int( (self.cffex_point2time[spot]) / 1000 )
        ms     = (int( (self.cffex_point2time[spot]) % 1000 ))
        hh     = (int( hhmmss / 3600 ) )
        mm     = (int( ( hhmmss % 3600) / 60 ))
        ss     = (int( ( hhmmss % 3600) % 60 ))
        return((hh,mm,ss,ms))

    def fother_spot2time_hhmmssms(self, spot):
        hhmmss = int( (self.other_point2time[spot]) / 1000 )
        ms     = (int( (self.other_point2time[spot]) % 1000 ))
        hh     = (int( hhmmss / 3600 ) )
        mm     = (int( ( hhmmss % 3600) / 60 ))
        ss     = (int( ( hhmmss % 3600) % 60 ))
        return((hh,mm,ss,ms))

    def get_spot_count_perday(self, exchange='cffex'):
        if exchange == 'cffex':
            return self.cffex_last
        else:
            return self.other_last

    def get_other_break_info(self):
        break_spots = ( self.other_time2point[ (self.OtherBreak.morning_begin )  ],\
                        self.other_time2point[ (self.OtherBreak.morning_break_end )  ],\
                            self.other_time2point[ (self.OtherBreak.afternoon_begin  )  ] )
        break_millis = list(map(self.fother_spot2time, break_spots))
        return break_spots, break_millis

    def get_cffex_break_info(self):
        break_spots = ( self.cffex_time2point[ (self.CFFEX.morning_begin )  ],\
                            self.cffex_time2point[ (self.CFFEX.afternoon_begin ) ] )
        break_millis = list(map(self.fcffex_spot2time, break_spots))
        return break_spots, break_millis


if __name__ == '__main__':
    mode = DayMode('before_2016')
    for spot in range(10):
        print(spot,mode.fcffex_spot2time_hhmmssms(spot))
    
    
    
    
