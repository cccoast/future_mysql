#coding:utf8
# from stock_table_struct import stock_data_model_index_component,stock_data_model_industry,stock_data_model_stock_industry
from sqlalchemy import and_, distinct
import pandas as pd

import os,sys
parent_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_path)

from tushare_feed.models import stock_index_weight,sw_industry,sw_industry_detail,csi_industry,csi_industry_detail,\
                                    csrc_industry,csrc_industry_detail
from tushare_feed.index import main_index_names,csi_sector_names,sw_sector_names,stockindex_names_all

global_stock_ins_id_list = None

def get_global_stock_ins_id_list():
    return global_stock_ins_id_list
def set_global_stock_ins_id_list(l):
    global global_stock_ins_id_list
    global_stock_ins_id_list = l
    
class GlobalVar(object):

    def __init__(self):
        self.basic_path = r'D:\win_datacenter'

        self.sample_freq = {}
        self.sample_freq['tick'] = 500
        self.sample_freq['1min'] = 60000
        self.sample_freq['5min'] = 300000
        
        self.STOCK_ID_MAX = 1000000
        self.SW_SECTOR_ID_MIN,self.SW_SECTOR_ID_MAX = 1800000,1900000
        self.STOCKINDEX_ID_MAX = 2000000
        
        self.BJ_ID_BEGIN = 800000
        self.SH_ID_BEGIN = 600000
        self.SZ_ID_BEGIN = 0
        
    def get_freq(self, freq):
        if freq == 'tick':
            return 500
        elif freq.endswith('min'):
            return int(freq[:-3]) * 60 * 1000
        elif freq.endswith('day'):
            return 3600 * 24 * 1000
        
_stockindex_name_to_id = None
_stockindex_id_to_name = None

gv = GlobalVar()

#指数 > 1000000
def generate_index_name_id_dict():
    global _stockindex_name_to_id 
    global _stockindex_id_to_name
    _stockindex_name_to_id = {}
    _stockindex_id_to_name = {}
    for name in stockindex_names_all:
#         if name == '399300.SZ':
#             v = '000300.SZ'
        v = name.split('.')
        _stockindex_name_to_id[name] = int(v[0]) + gv.STOCK_ID_MAX
    for k,v in _stockindex_name_to_id.items():
        _stockindex_id_to_name[v] = k
        
def stockindex_name_to_id(name):
    global stockindex_name_to_id
    if _stockindex_name_to_id is None:
        generate_index_name_id_dict()
    return _stockindex_name_to_id[name]

def stockindex_id_to_name(id):
    global _stockindex_name_to_id
    if _stockindex_id_to_name is None:
        generate_index_name_id_dict()
    return _stockindex_id_to_name[id]
    
def is_stock_index_name(name):
    return name in stockindex_names_all

def is_stock_index_id(id):
    name = stockindex_id_to_name(id)
    return is_stock_index_name(name)

#sector && ETF > 1000000
def stock_id2name(_id):
    if isinstance(_id,str) and (_id.endswith('.SH') or _id.endswith('.SZ')):
        return _id
    inid = int(_id)
    _id = '{:0>6}'.format(_id)
    #index
    if inid >= gv.STOCK_ID_MAX:
        return stockindex_id_to_name(inid)
    elif inid >= gv.BJ_ID_BEGIN:
        return _id + '.BJ'
    elif inid >= gv.SH_ID_BEGIN:
        return _id + '.SH'
    elif inid >= gv.SZ_ID_BEGIN:
        return _id + '.SZ'
    else:
        return None

def stock_name2id(name):
    is_index = name in stockindex_names_all
    if is_index is True:
        return stockindex_name_to_id(name)
    else:
        return int(name.split('.')[0])

#股票ticker转化为id,stockindex,sector
class StockTicker(object):

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


#stock_code to ETF    
class StockIndex():

    sz50_code, hs300_code, zz500_code, zz800_code = '000016.SH','399300.SZ', '000905.SH', '000906.SH'
    default_effective_date = 20100101
    default_ineffective_date = 20300101
    
    def __init__(self):
        #use ph data
        #self.index_component = stock_data_model_index_component()
        #use tushare data
        self.index_component = stock_index_weight()
        self.index_component.create_table()

        self.default_ins_set = {}
        self.sz50_ins_set  = self.indexName2insIDs(index_code=self.sz50_code)
        self.hs300_ins_set = self.indexName2insIDs(index_code=self.hs300_code)
        self.zz500_ins_set = self.indexName2insIDs(index_code=self.zz500_code)
        self.zz800_ins_set = self.indexName2insIDs(index_code=self.zz800_code)
        
        self.default_ins_set[self.sz50_code] = self.sz50_ins_set
        self.default_ins_set[self.hs300_code] = self.hs300_ins_set
        self.default_ins_set[self.zz500_code] = self.zz500_ins_set
        self.default_ins_set[self.zz800_code] = self.zz800_ins_set

        self.stock2index = {}
        for ins_id in self.default_ins_set[self.zz800_code]:
            self.stock2index[ins_id] = set()
            self.stock2index[ins_id].add(self.zz800_code)
        for ins_id in self.default_ins_set[self.zz500_code]:
            if ins_id in self.stock2index:
                self.stock2index[ins_id].add(self.zz500_code)
        for ins_id in self.default_ins_set[self.hs300_code]:
            if ins_id in self.stock2index:
                self.stock2index[ins_id].add(self.hs300_code)
        for ins_id in self.default_ins_set[self.sz50_code]:
            if ins_id in self.stock2index:
                self.stock2index[ins_id].add(self.sz50_code)
    
    #default the newest index component
    def indexName2insIDs(self, index_code=hs300_code, start_date = default_effective_date, end_date = default_ineffective_date):
        
        start_date = self.default_effective_date   if start_date is None else start_date
        end_date   = self.default_ineffective_date if end_date   is None else end_date
        
        if ( start_date == self.default_effective_date) \
            and (end_date == self.default_ineffective_date ) \
                and ( index_code in list(self.default_ins_set.keys()) ):
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
    
    #一个insID对应多少个index
    def insID2indexName(self, ins_id, start_date = default_effective_date, end_date = default_ineffective_date):
        
        start_date = self.default_effective_date   if start_date is None else start_date
        end_date   = self.default_ineffective_date if end_date   is None else end_date
        
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
            if ins_id in self.stock2index:
                self.stock2index[ins_id] |= set(index_code)
            else:
                self.stock2index[ins_id] = set(index_code)
            return index_code
        else:
            return None

#stock_code to sector
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
        self.industry_df.set_index(['l1_code','level'],inplace = True)
        self.industry_df = self.industry_df.sort_index()
        self.industry_stock_df = pd.read_sql_table(self.industry_stock_table.table_name,self.industry_stock_table.engine)
        self.industry_stock_df.set_index('symbol',inplace = True)
        self.industry_stock_df = self.industry_stock_df.sort_index()
    
    def get_industry_names(self,lv = 'l1'):
        lvl_code = '_'.join((lv,'code'))
        return pd.unique(self.industry_df.loc[:,lvl_code].dropna().apply(int).values)
        
    def ins_name2industry(self,ins_name,lv = 'l1'):
        lvl_code = '_'.join(('industry_code',lv))
        ss = self.industry_stock_table.get_session()
        ret = ss.query(self.industry_stock_table.table_struct).filter(\
                            self.industry_stock_table.table_struct.stock_code == stock_name2id(ins_name)).first()
        ss.close()
        try:
            return self.industry_df.loc[ (int(getattr(ret,lvl_code)),0) ,'industry_code'].values[0]
        except:
            return '-1'

    def ins_name2industry_fast(self,ins_name,lv = 'l1'):
        lvl_code = '_'.join(('industry_code',lv))
        name =  int(self.industry_stock_df.loc[ ins_name , lvl_code])
        try:
            return self.industry_df.loc[ (name,0) ,'industry_code'].values[0]
        except:
            return '-1'

### get instruments list
def indexs2insList(index_list = None,start_date = None,end_date = None,level = 'day'):
    if index_list is not None:
        global global_stock_ins_id_list
        if global_stock_ins_id_list is not None:
            return global_stock_ins_id_list
        else:
            stock_index = StockIndex()
            ins_list = set()
            for index_name in index_list:
                #take care of the effective date
                sub_ins_list = stock_index.indexName2insIDs(index_name,start_date,end_date)
                ins_list |= set(sub_ins_list)
            ins_id_list = sorted(list(ins_list))
#             ins_id_list.append(1399300)
#             ins_id_list.append(1000905)
#             ins_id_list.append(1000906)
            for i in main_index_names:
                ins_id_list.append(stockindex_name_to_id(i))
            if level == 'day':
                for i in sw_sector_names:
                    ins_id_list.append(stockindex_name_to_id(i))
            global_stock_ins_id_list = ins_id_list 
            return global_stock_ins_id_list
    return None

def get_default_index_code_ids():
    ins_id_list = []
#             ins_id_list.append(1399300)
#             ins_id_list.append(1000905)
#             ins_id_list.append(1000906)
    for i in main_index_names:
        ins_id_list.append(stockindex_name_to_id(i))
    for i in sw_sector_names:
        ins_id_list.append(stockindex_name_to_id(i))
    return ins_id_list


def test_index():
    print(stockindex_name_to_id('000016.SH'))
    print(stockindex_name_to_id('399300.SZ'))
    print(stockindex_id_to_name(1399300))
    print(_stockindex_name_to_id)
    print(stock_id2name(1399300))
    print(stock_id2name(600030))
    print(stock_name2id('399300.SZ'))
    print(stock_name2id('600300.SH'))
    
def test_stock_index():
    si = StockIndex()
    for k,v in si.default_ins_set.items():
        print(k,len(v))
        print(v)
        
def test_stock_industry():
    si = StockIndustry('sw')
    print(si.get_industry_names(lv = 'l1'))
    print(si.get_industry_names(lv = 'l2'))
    print(si.get_industry_names(lv = 'l3'))
    print(si.ins_name2industry('000001.SZ'))
    print(si.ins_name2industry('000001.SZ',lv = 'l2'))
    print(si.ins_name2industry('000001.SZ',lv = 'l3'))
    
    print(si.ins_name2industry_fast('000001.SZ'))
    print(si.ins_name2industry_fast('000001.SZ',lv = 'l2'))
    print(si.ins_name2industry_fast('000001.SZ',lv = 'l3'))

def test():
    test_index()
    test_stock_index()
    test_stock_industry()
   
if __name__ == '__main__':
    si = StockIndustry()
    a = si.ins_name2industry_fast(ins_name='000001.SZ')
    
    st = StockTicker()
    b = st.get_id(a)
    
    print(a,b)