#coding:utf8
# from stock_table_struct import stock_data_model_index_component,stock_data_model_industry,stock_data_model_stock_industry
from sqlalchemy import and_, distinct
import pandas as pd

import sys
if '..' not in sys.path:
    sys.path.append('..')
from tushare_feed.models import stock_index_weight,sw_industry,sw_industry_detail,csi_industry,csi_industry_detail,\
                                    csrc_industry,csrc_industry_detail
from tushare_feed.index import main_index_names,csi_sector_names,sw_sector_names,stock_index_names_all

_stock_index_name_to_id = None
_stock_index_id_to_name = None

def generate_index_name_id_dict():
    global _stock_index_name_to_id 
    global _stock_index_id_to_name
    _stock_index_name_to_id = {}
    _stock_index_id_to_name = {}
    for name in stock_index_names_all:
        v = name.split('.')
        #sw id
        if len(v) == 1:
            _stock_index_name_to_id[name] = int(name)
        elif name.upper() == '399300.SZ':
            _stock_index_name_to_id[name] = 900300
        else:
            _stock_index_name_to_id[name] = int(v[0]) + 900000
    for k,v in _stock_index_name_to_id.iteritems():
        _stock_index_id_to_name[v] = k
        
def stock_index_name_to_id(name):
    global stock_index_name_to_id
    if _stock_index_name_to_id is None:
        generate_index_name_id_dict()
    return _stock_index_name_to_id[name]

def stock_index_id_to_name(id):
    global _stock_index_name_to_id
    if _stock_index_id_to_name is None:
        generate_index_name_id_dict()
    return _stock_index_id_to_name[id]
    
def is_stock_index_name(name):
    return name in stock_index_names_all

def is_stock_index_id(id):
    name = stock_index_id_to_name(id)
    return is_stock_index_name(name)
    
def stock_id2name(_id):
    if isinstance(_id,str) and (_id.endswith('.SH') or _id.endswith('.SZ')):
        return _id
    inid = int(_id)
    _id = '{:0>6}'.format(_id)
    #index
    if inid >= 699999:
        return stock_index_id_to_name(inid)
    elif inid >= 600000:
        return _id + '.SH'
    elif inid >= 0:
        return _id + '.SZ'
    else:
        return None

def stock_name2id(name):
    is_index = name in stock_index_names_all
    if is_index is True:
        return stock_index_name_to_id(name)
    else:
        return int(name.split('.')[0])

    
class Ticker(object):

    def __init__(self):
        self.stock_index = StockIndex()
        self.stock_industry = StockIndustry()

    def get_id(self, ticker):
        return stock_name2id(ticker)
    
    def name2id(self,ticker):
        return stock_name2id(ticker)
        
    def id2name(self, _id):
        return stock_id2name(_id)

    def id2index(self, ins_id):
        return self.stock_index.insID2indexName(ins_id)

    def name2index(self, ins_name):
        return self.stock_index.insID2indexName(stock_name2id(ins_name))
    
    def name2industry(self, ins_name):
        return self.stock_industry.ins_name2industry_fast(ins_name)

    def id2industry(self, ins_id):
        return self.name2industry(stock_id2name(ins_id))

    
class StockIndex():

    hs300_code, zz500_code, zz800_code = '399300.SZ', '000905.SH', '000906.SH'
    default_effective_date   = 20090101
    default_ineffective_date = 20300101
    
    def __init__(self):
        #use ph data
        #self.index_component = stock_data_model_index_component()
        #use tushare data
        self.index_component = stock_index_weight()
        self.index_component.create_table()

        self.default_ins_set = {}
        self.hs300_ins_set = self.indexName2insIDs(index_code=self.hs300_code)
        self.zz500_ins_set = self.indexName2insIDs(index_code=self.zz500_code)
        self.zz800_ins_set = self.indexName2insIDs(index_code=self.zz800_code)
        self.default_ins_set[self.hs300_code] = self.hs300_ins_set
        self.default_ins_set[self.zz500_code] = self.zz500_ins_set
        self.default_ins_set[self.zz800_code] = self.zz800_ins_set

        self.stock2index = {}
        for ins_id in self.default_ins_set[self.zz800_code]:
            self.stock2index[ins_id] = set()
            self.stock2index[ins_id].add(self.zz800_code)
        for ins_id in self.default_ins_set[self.zz500_code]:
            if self.stock2index.has_key(ins_id):
                self.stock2index[ins_id].add(self.zz500_code)
        for ins_id in self.default_ins_set[self.hs300_code]:
            if self.stock2index.has_key(ins_id):
                self.stock2index[ins_id].add(self.hs300_code)
    
    #default the newest index component
    def indexName2insIDs(self, index_code=hs300_code, start_date = default_effective_date, end_date = default_ineffective_date):

        if ( start_date == self.default_effective_date) \
            and (end_date == self.default_ineffective_date ) \
                and ( index_code in self.default_ins_set.keys() ):
            return self.default_ins_set[index_code]

        ss = self.index_component.get_session()
        ret = ss.query(
            distinct(self.index_component.table_struct.stock_code)).filter(
                        and_(self.index_component.table_struct.index_code == index_code,
                                self.index_component.table_struct.effective_date >= start_date,
                                    self.index_component.table_struct.effective_date < end_date))
        ret = ret.all()
        ss.close()
        return [int(i[0].split('.')[0]) for i in ret if len(i) > 0]

    def insID2indexName(self, ins_id, start_date = default_effective_date, end_date = default_ineffective_date):
        ins_id = int(ins_id)
        ins_name = stock_id2name(ins_id)    
#         if ins_id in self.stock2index:
#             return self.stock2index[ins_id]
        ss = self.index_component.get_session()
        ret = ss.query(
            distinct(self.index_component.table_struct.index_code)).filter(
                and_(self.index_component.table_struct.stock_code == ins_name, 
                        self.index_component.table_struct.effective_date >= start_date,
                            self.index_component.table_struct.effective_date < end_date))
        index_code = [i[0] for i in ret.all()]
        ss.close()
        if len(index_code) > 0:
            if self.stock2index.has_key(ins_id):
                self.stock2index[ins_id] |= set(index_code)
            else:
                self.stock2index[ins_id] = set(index_code)
            return index_code
        else:
            return None

class StockIndustry():

    def __init__(self,industry_type_code='sw'):
        #ph
#         self.industry_table = stock_data_model_industry()
#         self.industry_stock_table = stock_data_model_stock_industry()
        #tushare
        self.industry_type_code = industry_type_code.lower()
        if industry_type_code == 'sw':
            self.industry_table = sw_industry()
            self.industry_stock_table = sw_industry_detail()
        elif industry_type_code == 'csi':
            self.industry_table = csi_industry()
            self.industry_stock_table = csi_industry_detail()
        elif industry_type_code == 'csrc':
            self.industry_table = csrc_industry()
            self.industry_stock_table = csrc_industry_detail()
        
        self.industry_table.create_table()
        self.industry_stock_table.create_table()

        self.industry_df = pd.read_sql_table(self.industry_table.table_name,self.industry_table.engine)
        self.industry_stock_df = pd.read_sql_table(self.industry_stock_table.table_name,self.industry_stock_table.engine)
    
    def get_industry_names(self,lv = 'l1'):
        lvl_code = '_'.join((lv,'code'))
        return pd.unique(self.industry_df.loc[:,lvl_code].values)
        
    def ins_name2industry(self,ins_name,lv = 'l1'):
        lvl_code = '_'.join(('industry_code',lv))
        ss = self.industry_stock_table.get_session()
        ret = ss.query(self.industry_stock_table.table_struct).filter(\
                            self.industry_stock_table.table_struct.stock_code == stock_name2id(ins_name)).first()
        ss.close()
        try:
            if self.industry_type_code == 'sw':
                return getattr(ret,lvl_code).split('.')[0]
            else:
                return getattr(ret,lvl_code)
        except:
            return ''

    def ins_name2industry_fast(self,ins_name,lv = 'l1'):
        lvl_code = '_'.join(('industry_code',lv))
        name =  self.industry_stock_df.loc[ self.industry_stock_df['stock_code'] == stock_name2id(ins_name), lvl_code]
        try:
            if self.industry_type_code == 'sw':
                return name.values[0].split('.')[0]
            else:
                return name.values[0]
        except:
            return ''

def test_index():
    print stock_index_name_to_id('399300.SZ')
    print stock_index_id_to_name(900300)
    print _stock_index_name_to_id
    print stock_id2name(900300)
    print stock_id2name(600030)
    print stock_name2id('399300.SZ')
    print stock_name2id('600300.SH')
    
def test_stock_index():
    si = StockIndex()
    for k,v in si.default_ins_set.iteritems():
        print k,len(v)
        print v
        
def test_stock_industry():
    si = StockIndustry('sw')
    print si.get_industry_names(lv = 'l2')
    print si.ins_name2industry('000001.SZ')
    print si.ins_name2industry_fast('000001.SZ')
    
if __name__ == '__main__':
    test_stock_industry()
    