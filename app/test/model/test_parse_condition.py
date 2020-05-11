import pytest

from app.test import app_with_db_inited

from app.model.parse_condition import (ParseCondition, ParseOrderby)
from app.schema import TABLES


@pytest.fixture
def data():
    d = {
        'where': [
            [('_@$@_user.name_@$@_', 'eq', 9), ('_@$@_user.id_@$@_', 'gt', '22')],
            [('_@$@_user.name_@$@_', 'like', '%ab%'), ('_@$@_user.id_@$@_', 'le', '23'), ('_@$@_user.id_@$@_', 'lt', '23')],
            [('_@$@_user.name_@$@_', 'like', '%ab%'), ('_@$@_user.id_@$@_', 'in', '2_@$@_5_@$@_3'), ('_@$@_user.id_@$@_', 'lt', '23')],
            [('_@$@_user.name_@$@_', 'between', 'aa_@$@_bb'), ('_@$@_user.id_@$@_', 'ge', '6'), ('_@$@_user.id_@$@_', 'neq', '7')],
        ],
        'order_by': [
            ('_@$@_user.name_@$@_', 'desc'),
            # ('_@$@_user.id_@$@_', 'desc'),
            ('_@$@_user.add_time_@$@_', 'desc'),
            ('_@$@_user.last_update_time_@$@_', 'asc')
        ]
    }
    yield d


def test_parse_condition(data):
    """ 解析 where / limit /offset 等sql子句
    """
    pc = ParseCondition(data['where'], TABLES['user'])
    stm = pc.parse()
    print(stm)


## order by

def test_parse_condition_order_by(data, app_with_db_inited):
    """ 测试解析orderby
    """
    import time
    from app.db import get_dbsess

    t_map = TABLES['user']

    od = ParseOrderby(data['order_by'], t_map)
    order_by = od.parse()
    with app_with_db_inited.app_context():
        dbsess = get_dbsess()

        rows = [
            t_map(name='aaa'),
            t_map(name='bbb'),
            t_map(name='ccc'),
            t_map(name='ddd'),
            t_map(name='eee'),
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
        rs = dbsess.query(t_map).order_by(*order_by).all()
        # for row in rs:
        #     print(row)
        assert len(rs) == len(rows)
        assert rs[0].last_update_time == 999


## 耗时的放最后

@pytest.mark.skip(reason='这个比较耗时')
def test_parse_condition_benchmark(data):
    """ 解析 where / limit /offset 等sql子句
    """
    import time
    cnt = 10000
    st = time.time()
    for i in range(cnt):
        pc = ParseCondition(data['where'], TABLES['user'])
        stm = pc.parse()
    total = time.time() - st

    print('total: %s S, cnt:%s, avg: %s S' % (total, cnt, total/cnt))

