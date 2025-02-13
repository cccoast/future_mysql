#sqlacodegen
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, text

from collections.abc import Iterable
import itertools

class DB_BASE(object):

    def __init__(self, db_name, _connect_str = "mysql+pymysql://xudi:123456@127.0.0.1:3306/{0}?charset=utf8" ):

        self.db_name = db_name
        connect_str = _connect_str.format(db_name)

        self.engine = create_engine(connect_str, echo=False)
        self.meta = MetaData()
        self.meta.bind = self.engine
        self.session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.session()

    def quick_map(self, table):
        table.metadata.create_all(self.engine)
        Base = declarative_base()

        class GenericMapper(Base):
            __table__ = table

        return GenericMapper

    def execute_sqls(self, sqls):
        session = self.get_session()
        for isql in sqls:
            session.execute(isql)
        session.commit()
        session.close()

    def execute_sql(self, sql):
        session = self.get_session()
        re = session.execute(sql)
        session.commit()
        return re

    def drop_table(self, table_name):
        self.execute_sql(text('Drop Table {0}.`{1}`'.format(self.db_name, table_name)))

    def insert_data_frame(self, _class, df, merge = False, chunk_size = 1024):
        magic_number = chunk_size
        print((len(df)))
        if len(df) < magic_number:
            self.insert_dicts(_class, df.to_dict('records'), merge)
        else:
            for _begin in range(0, len(df), magic_number):
                _end = _begin + magic_number if _begin + magic_number < len(
                    df) else len(df)
                self.insert_dicts(
                    _class, df.iloc[_begin:_end].to_dict('records'), merge)

    def insert_dicts(self, _class, _dicts, merge=False):
        session = self.get_session()
        if not merge:
            session.execute(_class.__table__.insert(), _dicts)
        else:
            for _idict in _dicts:
                session.merge(_class(**_idict))
        session.commit()
        session.close()

    def insert_dictlike(self, _class, _dict, merge=False):
        session = self.get_session()
        if not merge:
            session.add(_class(**_dict))
        else:
            session.merge(_class(**_dict))
        session.commit()
        session.close()

    def insert_lists(self, _class, _lists, merge=False):
        cols = list(_class.__table__.c.keys())
        if len(cols) > 1:
            if isinstance(_lists[0], Iterable):
                _dicts = [dict(list(zip(cols, val))) for val in _lists]
            else:
                _dicts = [{cols[i]: val} for i, val in enumerate(_lists)]
        else:
            _dicts = [{cols[0]: val} for val in _lists]

        self.insert_dicts(_class, _dicts, merge)

    def insert_listlike(self, _class, val, merge=False):
        _dict = dict(list(zip(list(_class.__table__.c.keys()), val)))
        self.insert_dictlike(_class, _dict, merge)

    def insert_objs(self, obj_lists):
        session = self.get_session()
        for iobj in obj_lists:
            session.add(iobj)
        session.commit()
        session.close()

    def insert_obj(self, iobj):
        session = self.get_session()
        session.add(iobj)
        session.commit()
        session.close()

    def delete_lists_obj(self, obj_lists):
        session = self.get_session()
        for i in obj_lists:
            session.delete(i)
        session.flush()
        session.commit()
        session.close()

    def delete_lists(self, _class, _values, _key=None):
        session = self.get_session()
        table = _class.__table__
        if not _key:
            _key = list(table.primary_key.columns.keys())
        _lists = None
        if isinstance(_values, list):
            if isinstance(_values[0], collections.Iterable):
                _lists = [dict(list(zip(_key, iv))) for iv in _values]
            else:
                _lists = [{_key[0]: val} for val in _values]
        else:
            return False

#         session.execute(table.delete(),_lists)
        for _idict in _lists:
            objs = session.query(_class).filter_by(**_idict).all()
            for iobj in objs:
                session.delete(iobj)
        session.flush()
        session.commit()

    def get_column_names(self, _class):
        return list(_class.__table__.c.keys())

    def get_column_obj(self, _class, key):
        return _class.__table__.columns[key]

    def get_columns_obj(self, _class):
        return _class.__table__.columns
        
    def get_primary_key(self, _class):
        return list(_class.__table__.primary_key.columns.keys())

    def get_primary_key_obj(self, _class):
        return _class.__table__.primary_key.columns

    def query_obj(self, obj, **kw):
        ss = self.get_session()
        ret = ss.query(obj).filter_by(**kw).all()
        ss.close()
        return ret
    
def get_all_table_names(dbname):
    sql = r"select table_name from information_schema.tables where table_schema='{0}' and table_type='base table';".format(dbname)
    connect_str = "mysql+pymysql://xudi:123456@localhost:3306/{0}".format(dbname)
    engine = create_engine(connect_str, echo=False)
    session = sessionmaker(bind=engine)
    ss = session()
    records = ss.execute(sql)
    ss.close()
    return [j for j in itertools.chain(*[i for i in records])]

def clear_table(db_name,db_model):
    sql = r'DELETE FROM {}.{} WHERE 1=1'
    if db_model.check_table_exist():
        db_model.execute_sql(sql.format(db_name,db_model.table_struct.name))

class DB_UNI_TEST(DB_BASE):

    def __init__(self, db_name):
        super(DB_UNI_TEST, self).__init__(db_name)

    def show(self):
        t201301 = Table('test', self.meta,
                        Column('point',Integer,primary_key=True,autoincrement=False),
                        Column('_id', String(45)))
        table_struct = self.quick_map(t201301)

        print((table_struct.__table__.columns))

        _id = "if"

        session = self.get_session()
        all_records = session.query(table_struct).all()
        for irecord in all_records:
            print((irecord._id, irecord.point))
            session.delete(irecord)
        session.commit()
        session.close()

        col_names = self.get_column_names(table_struct)

        dicts = []
        for i in range(10):
            _idict = dict(list(zip(col_names, (i, _id + str(i)))))
            dicts.append(_idict)

        print(dicts)
        self.insert_dicts(table_struct, dicts, True)

        self.insert_obj(table_struct(**dict(list(zip(col_names, (99, 'test'))))))

        objs = [
            table_struct(**dict(list(zip(col_names, (100, 'xdd'))))),
            table_struct(**dict(list(zip(col_names, (101, 'lyx')))))
        ]
        self.insert_objs(objs)

        lists = []
        for i in range(20, 30):
            #             self.insert_listlike(table_struct, (i,_id),True)
            ilist = (i, _id + str(i))
            lists.append(ilist)

        print(lists)
        self.insert_lists(table_struct, lists, True)


if __name__ == '__main__':
    dbapi = DB_UNI_TEST('test')
    print(dbapi.show())
