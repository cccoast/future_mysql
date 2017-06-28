from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table
import dbBase as db


def stock_id2name(_id):
    inid = int(_id)
    _id = '{:0>6}'.format(_id)
    if inid >= 900000:
        return _id + '.CSI'
    elif inid >= 600000:
        return _id + '.SH'
    elif inid >= 0:
        return _id + '.SZ'
    else:
        return None


def stock_name2id(_id):
    index = False if ((not _id.endswith('.SZ')) and
                      (not _id.endswith('.SH'))) else True
    if index:
        return _id.split('.')[0]
    else:
        return str(900000 + int(_id.split('.')[0]))


class stock_data_model_base(db.DB_BASE):

    def __init__(self, db_name='stock', table_name=None):
        super(stock_data_model_base, self).__init__(db_name)
        self.table_struct = None

    def create_table(self):
        if self.table_struct is not None:
            self.table_struct = self.quick_map(self.table_struct)

    def check_table_exist(self):
        if self.table_struct is not None:
            return self.table_struct.exists()
        else:
            raise Exception("no table specified")


class stock_dates(db.DB_BASE):

    def __init__(self, db_name='stock', table_name='b_calendar'):
        super(stock_dates, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                table_name, self.meta,
                Column('dates', Numeric(10, 0), primary_key=True),
                Column('day_of_week', Numeric(10, 0)),
                Column('day_of_month', Numeric(10, 0)),
                Column('month_of_year', Numeric(10, 0)),
                Column('quarter_of_year', Numeric(10, 0)),
                Column('year', Numeric(10, 0)),
                Column('is_end_of_month', String(100)),
                Column('is_end_of_quarter', String(100)),
                Column('is_end_of_halfayear', String(100)),
                Column('is_end_of_year', String(100)),
                Column('is_open_of_stock_cn', String(100)),
                Column('is_open_of_ib_cn', String(100)),
                Column('is_final_open_of_week', String(100)),
                Column('is_final_open_of_month', String(100)),
                Column('is_final_open_of_quarter', String(100)),
                Column('is_final_open_of_halfayear', String(100)),
                Column('is_final_open_of_year', String(100)),
                Column('is_final_open_of_week_ib', String(100)),
                Column('is_final_open_of_month_ib', String(100)),
                Column('is_final_open_of_quarter_ib', String(100)),
                Column('is_final_open_of_halfayear_ib', String(100)),
                Column('is_final_open_of_year_ib', String(100)),
                Column('is_day_of_open_fund', String(100)),
                Column('is_day_of_close_fund', String(100)),
                Column('is_day_of_sb', String(100)),
                Column('start_date_of_week_sb', Numeric(10, 0)),
                Column('end_date_of_week_sb', Numeric(10, 0)),
                Column('first_created_date', DateTime),
                Column('last_updated_date', DateTime))


class stock_data_model_index_component(stock_data_model_base):

    def __init__(self, db_name='stock', table_name='b_index_component'):
        super(stock_data_model_index_component, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(table_name, self.meta,
                                      Column(
                                          'index_code',
                                          String(100),
                                          primary_key=True,
                                          nullable=False),
                                      Column(
                                          'instrument_code',
                                          String(100),
                                          primary_key=True,
                                          nullable=False),
                                      Column(
                                          'effective_date',
                                          Numeric(10, 0),
                                          primary_key=True,
                                          nullable=False),
                                      Column('ineffective_date', Numeric(10,
                                                                         0)),
                                      Column('first_created_date', DateTime),
                                      Column('last_updated_date', DateTime),
                                      Index('b_index_component_u1',
                                            'index_code', 'instrument_code',
                                            'effective_date'))


class stock_data_model_industry(stock_data_model_base):

    def __init__(self, db_name='stock', table_name='b_industry'):
        super(stock_data_model_industry, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                table_name, self.meta,
                Column('industry_code', String(100), primary_key=True),
                Column('industry_type_code', String(100)),
                Column('industry_name', String(100)),
                Column('industry_level', Float(asdecimal=True)),
                Column('parent_industry_code', String(100)),
                Column('industry_description', String(100)),
                Column('first_created_date', DateTime),
                Column('last_updated_date', DateTime))


class stock_data_model_stock_name(stock_data_model_base):

    def __init__(self, db_name='stock', table_name='b_stock'):
        super(stock_data_model_stock_name, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(table_name, self.meta,
                                      Column(
                                          'stock_code',
                                          String(100),
                                          primary_key=True),
                                      Column('stock_name', String(100)),
                                      Column('first_created_date', DateTime),
                                      Column('last_updated_date', DateTime))


class stock_data_model_index_price(stock_data_model_base):

    def __init__(self, db_name='stock', table_name='b_stock_index_price'):
        super(stock_data_model_index_price, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                table_name, self.meta,
                Column('index_code', String(100)),
                Column('effective_date', Numeric(10, 0)),
                Column('high_price', Float(asdecimal=True)),
                Column('open_price', Float(asdecimal=True)),
                Column('low_price', Float(asdecimal=True)),
                Column('close_price', Float(asdecimal=True)),
                Column('volume', Float(asdecimal=True)),
                Column('turnover', Float(asdecimal=True)),
                Column('market_value', Float(asdecimal=True)),
                Column('last_close_price', Float(asdecimal=True)),
                Column('first_created_date', DateTime),
                Column('last_updated_date', DateTime),
                Index('b_stock_index_price_u1', 'effective_date', 'index_code'))


class stock_data_model_stock(stock_data_model_base):

    def __init__(self, db_name='stock', table_name='b_stock_price'):
        super(stock_data_model_stock, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                table_name, self.meta,
                Column(
                    'stock_code', String(100), primary_key=True,
                    nullable=False),
                Column(
                    'effective_date',
                    Numeric(10, 0),
                    primary_key=True,
                    nullable=False),
                Column('price_type_code', Numeric(10, 0)),
                Column('high_price', Float(asdecimal=True)),
                Column('open_price', Float(asdecimal=True)),
                Column('low_price', Float(asdecimal=True)),
                Column('close_price', Float(asdecimal=True)),
                Column('volume', Float(asdecimal=True)),
                Column('turnover', String(100)),
                Column('last_close_price', Float(asdecimal=True)),
                Column('ab_rate', Float(asdecimal=True)),
                Column('average_price', Float(asdecimal=True)),
                Column('turnover_rate', Float(asdecimal=True)),
                Column('change_rate', Float(asdecimal=True)),
                Column('amplitude_rate', Float(asdecimal=True)),
                Column('total_market_value', Float(asdecimal=True)),
                Column('free_float_market_value', Float(asdecimal=True)),
                Column('high_price_of_52week', Float(asdecimal=True)),
                Column('low_price_of_53week', Float(asdecimal=True)),
                Column('average_volume_of_3month', Float(asdecimal=True)),
                Column('pe_rate', Float(asdecimal=True)),
                Column('pb_rate', Float(asdecimal=True)),
                Column('pe_rate_ttm', Float(asdecimal=True)),
                Column('unit_cash_flow', Float(asdecimal=True)),
                Column('unit_cash_flow_ttm', Float(asdecimal=True)),
                Column('unit_income', Float(asdecimal=True)),
                Column('unit_income_ttm', Float(asdecimal=True)),
                Column('unit_dividend', Float(asdecimal=True)),
                Column('first_created_date', DateTime),
                Column('last_updated_date', DateTime))


class stock_data_model_stock_price(stock_data_model_base):

    def __init__(self, db_name='stock', table_name='b_stock_reprice'):
        super(stock_data_model_stock_price, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                'b_stock_reprice', self.meta,
                Column(
                    'stock_code', String(100), primary_key=True,
                    nullable=False),
                Column(
                    'effective_date', Integer, primary_key=True,
                    nullable=False),
                Column('price_type_code', Numeric(10, 0)),
                Column('high_price', Float(asdecimal=True)),
                Column('open_price', Float(asdecimal=True)),
                Column('low_price', Float(asdecimal=True)),
                Column('close_price', Float(asdecimal=True)),
                Column('volume', Float(asdecimal=True)),
                Column('turnover', Float(asdecimal=True)),
                Column('last_close_price', Float(asdecimal=True)),
                Column('factor', Float(asdecimal=True)),
                Column('first_created_date', DateTime),
                Column('last_updated_date', DateTime),
                Index('b_stock_reprice_u1', 'effective_date', 'stock_code'))


class stock_data_model_stock_industry(stock_data_model_base):

    def __init__(self, db_name='stock', table_name='b_stock_rs_industry'):
        super(stock_data_model_stock_industry, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(table_name, self.meta,
                                      Column(
                                          'stock_code',
                                          String(100),
                                          primary_key=True,
                                          nullable=False),
                                      Column(
                                          'industry_code',
                                          String(100),
                                          primary_key=True,
                                          nullable=False),
                                      Column(
                                          'effective_date',
                                          Numeric(10, 0),
                                          primary_key=True,
                                          nullable=False),
                                      Column('ineffective_date', Numeric(10,
                                                                         0)),
                                      Column('first_created_date', DateTime),
                                      Column('last_updated_date', DateTime),
                                      Index('b_stock_rs_industry_u1',
                                            'stock_code', 'industry_code',
                                            'effective_date'))


def import_new_trading_days():
    from trading_day_list import AllTradingDays
    trading_day_obj = AllTradingDays()
    all_days = set(trading_day_obj.get_trading_day_list())

    reprice = stock_data_model_stock_price()
    reprice.create_table()
    stock_days = set([
        int(i.effective_date)
        for i in reprice.query_obj(
            reprice.table_struct, stock_code='000001.SZ')
    ])

    print len(all_days)
    print len(stock_days)

    new_trading_days = sorted(stock_days - all_days)
    trading_day_obj.insert_lists(
        trading_day_obj.table_struct, new_trading_days, merge=True)


if __name__ == '__main__':
    pass
