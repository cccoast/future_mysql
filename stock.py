import pandas as pd
import numpy as np

from stock_table_struct import stock_data_model_index_component
from sqlalchemy import and_

class StockIndex():
    
    hs300_code,zz500_code,zz800_code = '000300.CSI','000905.CSI','000906.CSI'
    def __init__(self):
        self.index_component = stock_data_model_index_component()
        self.index_component.create_table()
        self.default_ins_set = {}
        self.hs300_ins_set = self.get_index_component(index_code = self.hs300_code)
        self.zz500_ins_set = self.get_index_component(index_code = self.zz500_code)
        self.zz800_ins_set = self.get_index_component(index_code = self.zz800_code)
        self.default_ins_set[self.hs300_code] = self.hs300_ins_set
        self.default_ins_set[self.zz500_code] = self.zz500_ins_set
        self.default_ins_set[self.zz800_code] = self.zz800_ins_set
         
    def get_index_component(self,index_code = hs300_code, ineffective_date = 20200000):
        if not index_code.endswith('.CSI'):
            index_code = index_code + '.CSI'
        
        if index_code in self.default_ins_set.keys():
            return self.default_ins_set[index_code]
        
#         print self.index_component.table_struct
        ss = self.index_component.get_session()
        ret = ss.query(self.index_component.table_struct.instrument_code).filter(
            and_(
                self.index_component.table_struct.index_code == index_code,
                self.index_component.table_struct.ineffective_date > ineffective_date 
            )
        )
        print ret
        ret = ret.all()
        ss.close()
        
        return [i.instrument_code for i in ret]
    

if __name__ == '__main__':
    
    stock_index = StockIndex()
    print stock_index.get_index_component(stock_index.hs300_code)
    print stock_index.get_index_component(stock_index.zz500_code)
    print stock_index.get_index_component(stock_index.zz800_code)
    
    