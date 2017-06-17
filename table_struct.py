
from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table
import dbBase as db
import werkzeug.security as myhash

class User(db.DB_BASE):
    
    def __init__(self):
        db_name = 'user'
        table_name = 'password'
        super(User,self).__init__(db_name)
        
        self.table_struct = Table(table_name,self.meta,
                     Column('name',String(20),primary_key = True),
                     Column('password',String(160)),
                    )
        
        self.user_struct = self.quick_map(self.table_struct)
        
    def insert_user(self,name,pwd):
        indict = {'name':name,'password':myhash.generate_password_hash(pwd)}
        self.insert_dictlike(self.user_struct,indict)
        
    def check_user(self,name,pwd):
        session = self.get_session()
        re = session.query(self.user_struct).filter_by(name = name).all()
        ret = False
        for i in re:
            if myhash.check_password_hash(i.password, pwd):
                ret = True
            else:
                ret = False
        session.close()
        return ret

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
    