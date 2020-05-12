import pytest

from app.model import curd
import app.exc as exc
from app.schema import TABLES

from app.test import app_with_db_inited

def test_curd_c(app_with_db_inited):
    """ curd create   dbsess.add_all() 这个效率低是逐条插入
    """

    data = {
        'user': [
           { 'name': 'aaa'},
           { 'name': 'bbb'},
           { 'name': 'ccc'},
        ]
    }
    with app_with_db_inited.app_context():
        dbsess = curd.create(data, bulk=False)
        csr = dbsess.bind.connect().execute('select name from user')
        rs = csr.fetchall()
        # print(rs)
        assert len(rs) == 3
        assert rs[0][0] == 'aaa'


def test_curd_c_bulk_insert(app_with_db_inited):
    """ curd create  高效批量插入
    """

    data = {
        'user': [
           { 'name': 'aaa'},
           { 'name': 'bbb'},
           { 'name': 'ccc'},
        ]
    }
    with app_with_db_inited.app_context():
        dbsess = curd.create(data)
        csr = dbsess.bind.connect().execute('select name, * from user')
        rs = csr.fetchall()
        # ars = dbsess.query(Table).all()
        # print(ars)
        # print(rs)
        # print(dir(csr))
        # print(csr.keys())
        assert len(rs) == 3
        assert rs[0][0] == 'aaa'

def test_curd_c_exc_table_not_exists(app_with_db_inited):
    """ 表名称错误 （表不存在）抛出异常 YcmsTableNotExistsError
    """

    data = {
        'some_table': []
    }
    data_01 = {
        'some_table': [{'a': 'b'}]
    }
    data_02 = {
        'user': [{'a': 'b'}]
    }
    with app_with_db_inited.app_context():
        with pytest.raises(exc.YcmsTableNotExistsError):
            dbsess, Table = curd.create(data)
            dbsess_01, Table_01 = curd.create(data_01)

        with pytest.raises(exc.YcmsDBFieldNotExistsError):
            dbsess_02 = curd.create(data_02)
