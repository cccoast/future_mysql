from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import Table
import dbBase as db
import werkzeug.security as myhash


class User(db.DB_BASE):

    def __init__(self):
        db_name = 'user'
        table_name = 'password'
        super(User, self).__init__(db_name)

        self.table_struct = Table(
            table_name,
            self.meta,
            Column('name', String(20), primary_key=True),
            Column('password', String(160)),)

        self.user_struct = self.quick_map(self.table_struct)

    def insert_user(self, name, pwd):
        indict = {'name': name, 'password': myhash.generate_password_hash(pwd)}
        self.insert_dictlike(self.user_struct, indict)

    def check_user(self, name, pwd):
        session = self.get_session()
        re = session.query(self.user_struct).filter_by(name=name).all()
        ret = False
        for i in re:
            if myhash.check_password_hash(i.password, pwd):
                ret = True
            else:
                ret = False
        session.close()
        return ret


class data_model_tick(db.DB_BASE):

    def __init__(self, db_name, table_name=None):
        super(data_model_tick, self).__init__(db_name)

        self.table_struct = None
        if table_name is not None:
            self.table_struct = Table(
                table_name,
                self.meta,
                Column('id', String(20), primary_key=True),
                Column('day', Integer, primary_key=True, autoincrement=False),
                Column('spot', Integer, primary_key=True, autoincrement=False),
                Column('Time', String(20)),
                Column('LastPrice', Float),
                Column('Volume', Integer),
                Column('BidPrice', Float),
                Column('BidVolume', Integer),
                Column('AskPrice', Float),
                Column('AskVolume', Integer),
                Column('OpenInterest', Integer),)

    def create_table(self):
        if self.table_struct is not None:
            self.tick_struct = self.quick_map(self.table_struct)

    def check_table_exist(self):
        if self.table_struct is not None:
            return self.table_struct.exists()


class data_model_min(db.DB_BASE):

    def __init__(self, db_name, table_name=None):
        super(data_model_min, self).__init__(db_name)

        self.table_struct = None
        if table_name is not None:
            self.table_struct = Table(
                table_name,
                self.meta,
                Column('id', String(20), primary_key=True),
                Column('day', Integer, primary_key=True, autoincrement=False),
                Column('spot', Integer, primary_key=True, autoincrement=False),
                Column('Time', String(20)),
                Column('OpenPrice', Float),
                Column('HighPrice', Float),
                Column('LowPrice', Float),
                Column('ClosePrice', Float),
                Column('Volume', Integer),
                Column('OpenInterest', Integer),)

    def create_table(self):
        if self.table_struct is not None:
            self.min_struct = self.quick_map(self.table_struct)

    def check_table_exist(self):
        if self.table_struct is not None:
            return self.table_struct.exists()


class data_model_day(db.DB_BASE):

    def __init__(self, db_name, table_name=None):
        super(data_model_day, self).__init__(db_name)

        self.table_struct = None
        if table_name is not None:
            self.table_struct = Table(
                table_name,
                self.meta,
                Column('id', String(20), primary_key=True),
                Column('day', Integer, primary_key=True, autoincrement=False),
                Column('OpenPrice', Float),
                Column('HighPrice', Float),
                Column('LowPrice', Float),
                Column('ClosePrice', Float),
                Column('Volume', Integer),
                Column('OpenInterest', Integer),)

    def create_table(self):
        if self.table_struct is not None:
            self.day_struct = self.quick_map(self.table_struct)

    def check_table_exist(self):
        if self.table_struct is not None:
            return self.table_struct.exists()


class Ticker(object):

    def __init__(self):
        self.tid_dict = {}
        self.cffex = ['if', 'tf', 'ic', 'ih']
        j = 1
        for i in self.cffex:
            self.tid_dict[i] = 11000 + j
            j += 1
        self.shfex = ['au', 'ag', 'cu', 'al', 'zn', 'rb', 'ru']
        j = 1
        for i in self.shfex:
            self.tid_dict[i] = 12000 + j
            j += 1

    def get_exchage_name(self, ticker):
        ticker = str.lower(ticker)[:2]
        #         print ticker
        if ticker in self.cffex:
            return 'cffex'
        elif ticker in self.shfex:
            return 'shfex'
        return None

    def get_market_id(self, ticker):
        ticker = str.lower(ticker)[:2]
        if ticker in self.cffex:
            return int(self.tid_dict[ticker] / 1000)
        if ticker in self.shfex:
            return int(self.tid_dict[ticker] / 1000)
        return None

    def get_market_no(self, ticker):
        ticker = str.lower(ticker)[:2]
        if ticker in self.cffex:
            return int(self.tid_dict[ticker] % 1000)
        if ticker in self.shfex:
            return int(self.tid_dict[ticker] % 1000)
        return None

    def get_id(self, ticker):
        market_id, market_no = self.get_market_id(ticker), self.get_market_no(
            ticker)
        last = int(filter(lambda x: str.isdigit(x), ticker))
        if market_id:
            return (market_id * 1000 + market_no) * 10000 + last
        else:
            return None

    def get_dbname(self, ticker, level='tick'):
        ticker = str.lower(ticker)[:2]
        if ticker in self.cffex:
            ret = '_'.join(('cffex', ticker))
        elif ticker in self.shfex:
            ret = '_'.join(('shfex', ticker))
        else:
            return None
        if level == 'tick':
            return ret
        elif level.endswith('min'):
            return '_'.join((ret, 'min'))
        else:
            return '_'.join((ret.split('_')[0], 'day'))

    def get_table_name(self, ticker, date=None, level='tick'):
        ticker = str.lower(ticker[:2])
        if level == 'tick' or level.endswith('min'):
            return str(date)
        elif level == 'day':
            return ticker

    def get_num_of_tickers(self, ticker, day):
        ticker = str.lower(ticker[:2])
        dbname = self.get_dbname(ticker, 'day')
        table_name = self.get_table_name(ticker, day, level='day')
        #print dbname, table_name
        table = data_model_day(dbname, table_name)
        if table.check_table_exist():
            table.create_table()
            records = table.query_obj(table.day_struct, day=day)
            tickers = [rec.id for rec in records]
        else:
            raise LookupError
        return len(tickers)

    def get_break_table_name(self, ticker='if'):
        exchange_name = self.get_exchage_name(ticker)
        if exchange_name is not None:
            table_name = '_'.join(('break', exchange_name))
            return table_name
        else:
            return None


if __name__ == '__main__':
    tick_info = Ticker()
    print tick_info.get_break_table_name('au')
    print tick_info.get_break_table_name('if')
