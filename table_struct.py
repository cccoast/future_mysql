
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table
import dbBase as db

class cffex_if(db.DB_BASE):
    
    def __init__(self,db_name,table_name):
        super(cffex_if,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('id',String(20),primary_key = True),
                     Column('day',Integer,primary_key = True,autoincrement = False),
                     Column('spot',Integer,primary_key = True,autoincrement = False),
                     Column('Time',String(20)),
                     Column('LastPrice',Float),
                     Column('Volume',Integer),
                     Column('BidPrice',Float),
                     Column('BidVolume',Integer),
                     Column('AskPrice',Float),
                     Column('AskVolume',Integer),
                     Column('OpenInterest',Integer),
                    )
        
    def create_table(self):
        self.if_struct = self.quick_map(self.table_struct)
                
    def check_table_exist(self):
        return self.table_struct.exists()
    
class cffex_if_min(db.DB_BASE):
    
    def __init__(self,db_name,table_name):
        super(cffex_if_min,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('id',String(20),primary_key = True),
                     Column('day',Integer,primary_key = True,autoincrement = False),
                     Column('spot',Integer,primary_key = True,autoincrement = False),
                     Column('Time',String(20)),
                     Column('OpenPrice',Float),
                     Column('HighPrice',Float),
                     Column('LowPrice',Float),
                     Column('ClosePrice',Float),
                     Column('Volume',Integer),
                     Column('OpenInterest',Integer),
                    )
        
    def create_table(self):
        self.if_min_struct = self.quick_map(self.table_struct)
                
    def check_table_exist(self):
        return self.table_struct.exists()

class cffex_if_day(db.DB_BASE):
    
    def __init__(self,db_name,table_name):
        super(cffex_if_day,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('id',String(20),primary_key = True),
                     Column('day',Integer,primary_key = True,autoincrement = False),
                     Column('OpenPrice',Float),
                     Column('HighPrice',Float),
                     Column('LowPrice',Float),
                     Column('ClosePrice',Float),
                     Column('Volume',Integer),
                     Column('OpenInterest',Integer),
                    )
        
    def create_table(self):
        self.if_day_struct = self.quick_map(self.table_struct)
                
    def check_table_exist(self):
        return self.table_struct.exists()
    