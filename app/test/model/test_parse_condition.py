import pytest

from app.test import app_with_db_inited

from app.model.parse_condition import (ParseCondition, ParseOrderby, ParseFields, ParseGroupBy)
from app.schema import TABLES


@pytest.fixture
def data():
    d = {
        'dist_tables': ['user'],
        'where': [
            [('user.name', 'eq', 9), ('user.id', 'gt', '22')],
            [('user.name', 'like', '%ab%'), ('user.id', 'le', '23'), ('user.id', 'lt', '23')],
            [('user.name', 'like', '%ab%'), ('user.id', 'in', '2_@$@_5_@$@_3'), ('user.id', 'lt', '23')],
            [('user.name', 'between', 'aa_@$@_bb'), ('user.id', 'ge', '_@$@_user.password_@$@_'), ('user.id', 'neq', '7')],
        ],
        'order_by': [
            ('user.name', 'desc'),
            # ('user.id', 'desc'),
            ('user.add_time', 'desc'),
            ('user.last_update_time', 'asc')
        ],
        'group_by': 'user.last_update_time$avg|user.id,sum|user.add_time,count|user.id',
        'fields': ['user.name', 'user.id', 'user.add_time']
    }
    yield d


def test_parse_condition(data):
    """ 解析 where / limit /offset 等sql子句
    """
    pc = ParseCondition(data['where'])
    stm = pc.parse()
    print('\n********************** where ***************************')
    print(stm)
    print(pc.params)
    print('********************** where ***************************')


## order by

def test_parse_condition_order_by(data, app_with_db_inited):
    """ 测试解析orderby
    """
    import time
    from app.db import get_dbsess

    t_map = TABLES['user']

    od = ParseOrderby(data['order_by'])
    order_by = od.parse()
    with app_with_db_inited.app_context():
        dbsess = get_dbsess()

        rows = [
            t_map(name='aaa'),
            t_map(name='bbb'),
            t_map(name='ccc'),
            t_map(name='bbb'),
            t_map(name='aaa'),
            t_map(name='fff'),
        ]
        for r in rows:
            # time.sleep(1)
            dbsess.add(r)
            dbsess.commit()
        rs = dbsess.query(t_map).order_by(*order_by).all()
        # for row in rs:
        #     print(row)
        assert len(rs) == len(rows)
        assert rs[0].name == 'fff'

        dbsess.query(t_map).filter(t_map.id>3).update({'last_update_time':999})
        dbsess.commit()
        rs = dbsess.query(t_map).order_by(*order_by).all()
        for row in rs:
            print(row)
        assert len(rs) == len(rows)
        assert rs[0].last_update_time == 999
        csr = dbsess.bind.execute('select *, avg(last_update_time), count(*) as cnt from user group by name')

        for i in csr.fetchall():
            print(i)

## fields

def test_parse_condition_fields(data):
    """ field to select 
    """
    from sqlalchemy import text
    rs = ParseFields(data['fields']).parse()
    print(rs)
    assert str(rs) == 'user.name, user.id, user.add_time'

## group by

def test_parse_condition_group_by(data):
    """ group by
    """
    rs = ParseGroupBy(data['group_by']).parse()
    print(rs)



## 耗时的放最后

@pytest.mark.skip(reason='这个比较耗时')
def test_parse_condition_benchmark(data):
    """ 解析 where / limit /offset 等sql子句
    """
    import time
    cnt = 10000
    st = time.time()
    for i in range(cnt):
        pc = ParseCondition(data['where'])
        stm = pc.parse()
    total = time.time() - st

    print('total: %s S, cnt:%s, avg: %s S' % (total, cnt, total/cnt))

