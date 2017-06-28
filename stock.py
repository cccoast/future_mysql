from stock_table_struct import stock_data_model_index_component,stock_id2name,stock_name2id,\
                                stock_data_model_industry,stock_data_model_stock_industry
from sqlalchemy import and_, distinct
import pandas as pd


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
        
    def id2dbname(self, ticker):
        return 'stock'

    def id2index(self, ins_id):
        return self.stock_index.insID2index(ins_id)

    def name2index(self, ins_name):
        return self.stock_index.insID2index(stock_name2id(ins_name))

    def id2industry(self, ins_id):
        return self.stock_industry.get_ins_name_industry_fast(
            stock_id2name(ins_id))

    def name2industry(self, ins_name):
        return self.stock_industry.get_ins_name_industry_fast(ins_name)


class StockIndex():

    hs300_code, zz500_code, zz800_code = '000300.CSI', '000905.CSI', '000906.CSI'

    def __init__(self):
        self.index_component = stock_data_model_index_component()
        self.index_component.create_table()
        self.default_ins_set = {}
        self.default_index_effective_day = 20300000
        self.hs300_ins_set = self.index2insIDs(index_code=self.hs300_code)
        self.zz500_ins_set = self.index2insIDs(index_code=self.zz500_code)
        self.zz800_ins_set = self.index2insIDs(index_code=self.zz800_code)
        self.default_ins_set[self.hs300_code] = self.hs300_ins_set
        self.default_ins_set[self.zz500_code] = self.zz500_ins_set
        self.default_ins_set[self.zz800_code] = self.zz800_ins_set

        self.stock2index = {}
        for ins_id in self.default_ins_set[self.zz800_code]:
            self.stock2index[ins_id] = []
            self.stock2index[ins_id].append(self.zz800_code)

        for ins_id in self.default_ins_set[self.zz500_code]:
            if self.stock2index.has_key(ins_id):
                self.stock2index[ins_id].append(self.zz500_code)

        for ins_id in self.default_ins_set[self.hs300_code]:
            if self.stock2index.has_key(ins_id):
                self.stock2index[ins_id].append(self.hs300_code)
    
    #default the newest index component
    def index2insIDs(self, index_code=hs300_code, selected_date = 20300000):
        index_code = str(index_code).upper()
        if not index_code.endswith('.CSI') and not index_code.startswith('9'):
            index_code = index_code + '.CSI'
        elif index_code.startswith('9'):
            index_code = stock_id2name(index_code)

        if ( selected_date == self.default_index_effective_day ) and \
                ( index_code in self.default_ins_set.keys() ):
            return self.default_ins_set[index_code]

        ss = self.index_component.get_session()
        ret = ss.query(
            distinct(self.index_component.table_struct.instrument_code)).filter(
                and_(self.index_component.table_struct.index_code == index_code,
                     self.index_component.table_struct.ineffective_date > selected_date,
                     self.index_component.table_struct.effective_date <= selected_date))
        ret = ret.all()
        ss.close()

        return [int(i[0].split('.')[0]) for i in ret if len(i) > 0]

    def insID2index(self, ins_id, selected_date = 20300000):
        ins_id = int(ins_id)
        ins_name = stock_id2name(ins_id)    
        if ins_id in self.stock2index:
            return self.stock2index[ins_id]
        ss = self.index_component.get_session()
        ret = ss.query(
            distinct(self.index_component.table_struct.index_code)).filter(
                and_(self.index_component.table_struct.instrument_code == ins_name, 
                     self.index_component.table_struct.ineffective_date > selected_date,
                     self.index_component.table_struct.effective_date <= selected_date))
        index_code = [i[0] for i in ret.all()]
        ss.close()
        if len(index_code) > 0:
            if self.stock2index.has_key(ins_id):
                self.stock2index[ins_id].append(index_code)
            else:
                self.stock2index[ins_id] = [index_code,]
            return index_code
        else:
            return None


class StockIndustry():

    def __init__(self):
        self.industry_table = stock_data_model_industry()
        self.industry_stock_table = stock_data_model_stock_industry()
        self.industry_table.create_table()
        self.industry_stock_table.create_table()

        self.industry_df = pd.read_sql_table(self.industry_table.table_name,
                                             self.industry_table.engine)

        self.industry_stock_df = pd.read_sql_table(
            self.industry_stock_table.table_name,
            self.industry_stock_table.engine)

    def get_industry_info(self,
                          value_field='industry_code',
                          industry_level=1,
                          industry_type_code='ZZ'):
        ss = self.industry_table.get_session()
        ret = ss.query(self.industry_table.table_struct).filter_by(
            industry_level=industry_level,
            industry_type_code=industry_type_code)
        ss.close()
        return [getattr(i, value_field) for i in ret]

    def get_industry_info_fast(self,
                               value_field='industry_code',
                               industry_level=1,
                               industry_type_code='ZZ'):
        return [
            i[0]
            for i in self.industry_df.
            loc[(self.industry_df['industry_level'] == industry_level) & (
                self.industry_df['industry_type_code'] == industry_type_code), [
                    value_field,
                ]].values if len(i) > 0
        ]

    def get_ins_name_industry(self,
                              ins_name,
                              industry_type_code='ZZ',
                              selected_date=20200000):
        ss = self.industry_stock_table.get_session()
        ret = ss.query(self.industry_stock_table.table_struct).filter(
            and_(self.industry_stock_table.table_struct.stock_code == ins_name,
                 self.industry_stock_table.table_struct.ineffective_date > selected_date,
                 self.industry_stock_table.table_struct.effective_date <= selected_date))
        ss.close()
        return [
            i.industry_code for i in ret
            if i.industry_code.endswith(industry_type_code)
        ]

    def get_ins_name_industry_fast(self,
                                   ins_name,
                                   industry_type_code='ZZ',
                                   ineffective_date=20200000):
        return [
            i[0]
            for i in self.industry_stock_df.loc[
                (self.industry_stock_df['ineffective_date'] >= ineffective_date
                ) & (self.industry_stock_df['stock_code'] == ins_name) &
                (self.industry_stock_df['industry_code'].apply(
                    lambda x: x.endswith(industry_type_code))), [
                        'industry_code',
                    ]].values if len(i) > 0
        ]


def test_index():
    stock_index = StockIndex()
    print len(stock_index.index2insIDs(stock_index.hs300_code))
    print len(stock_index.index2insIDs(stock_index.zz500_code))
    print len(stock_index.index2insIDs(stock_index.zz800_code))
    print stock_index.insID2index('000001')


def test_industry():
    stock_industry = StockIndustry()
    #     for industry_level,sub_df in stock_industry.industry_df.groupby('industry_level'):
    #         print industry_level
    #         print sub_df['industry_type_code'].unique()
    print stock_industry.get_industry_info()
    print stock_industry.get_industry_info_fast()
    print stock_industry.get_ins_name_industry('300002.SZ')
    print stock_industry.get_ins_name_industry_fast('300002.SZ')


def test_ticker():
    ticker_info = Ticker()
    print ticker_info.get_id('300002.SZ')
    print [stock_name2id(i) for i in ticker_info.id2index('300002')]
    print ticker_info.id2industry('300002')
    print ticker_info.name2index('300002.SZ')
    print ticker_info.name2industry('300002.SZ')

if __name__ == '__main__':
    stock_index = StockIndex()
    print len(stock_index.index2insIDs('000906.CSI',20100104))
