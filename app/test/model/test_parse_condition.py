import pytest

from app.model.parse_condition import ParseCondition
from app.schema import TABLES


@pytest.fixture
def data():
    d = {
        'where': [
            [('_@$@_user.name_@$@_', 'eq', 9), ('_@$@_user.id_@$@_', 'gt', '22')],
            [('_@$@_user.name_@$@_', 'like', '%ab%'), ('_@$@_user.id_@$@_', 'le', '23'), ('_@$@_user.id_@$@_', 'lt', '23')],
            [('_@$@_user.name_@$@_', 'like', '%ab%'), ('_@$@_user.id_@$@_', 'in', '2_@$@_5_@$@_3'), ('_@$@_user.id_@$@_', 'lt', '23')],
            [('_@$@_user.name_@$@_', 'between', 'aa_@$@_bb'), ('_@$@_user.id_@$@_', 'ge', '6'), ('_@$@_user.id_@$@_', 'neq', '7')],
        ]
    }
    yield d


def test_parse_condition(data):
    """ 解析 where / limit /offset 等sql子句
    """
    pc = ParseCondition(data, TABLES['user'])
    stm = pc.parse()
    print(stm)




def test_parse_condition_benchmark(data):
    """ 解析 where / limit /offset 等sql子句
    """
    import time
    cnt = 10000
    st = time.time()
    for i in range(cnt):
        pc = ParseCondition(data, TABLES['user'])
        stm = pc.parse()
    total = time.time() - st

    print('total: %s S, cnt:%s, avg: %s S' % (total, cnt, total/cnt))

