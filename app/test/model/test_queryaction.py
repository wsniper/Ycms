import pytest
from sqlalchemy import text

from app.db import get_dbsess
from app.test import app_with_db_inited

from app.schema import TABLES
from app.model.queryaction import (CreateAction, UpdateAction, DeleteAction, ListAction, OneAction)


@pytest.fixture
def data():
    d = {
        'create': {
            'user': [
                dict(name='abc', password='apassword'),
                dict(name='bbc', password='bpassword'),
                dict(name='cbc', password='cpassword'),
                dict(name='dbc', password='dpassword'),
                dict(name='ebc', password='epassword'),
                dict(name='fbc', password='fpassword')
            ],
            'roll': [
                dict(name='aaaaaaaaa', title='title_a'),
                dict(name='bbbbbbbbb', title='title_b'),
                dict(name='ccccccccc', title='title_c'),
                dict(name='ddddddddd', title='title_d'),
                dict(name='eeeeeeeee', title='title_e'),
                dict(name='fffffffff', title='title_f'),
                dict(name='ggggggggg', title='title_g'),
                dict(name='hhhhhhhhh', title='title_h'),
                dict(name='iiiiiiiii', title='title_i'),
            ]
        },
        'update': {
            'dist_tables': ['roll'],
            'data': {'name': 'updated name', 'last_update_time': 9777777},
            'condition': [
                [('roll.id', 'lt', 12), ('roll.id', 'gt', '7'), ('roll.name', 'like', '%h%')],
                [('roll.name', 'like', '%a'), ('roll.id', 'lt', 6)]
            ]
        }
    }
    yield d


def test_create(app_with_db_inited, data):
    """
    """
    d = data
    with app_with_db_inited.app_context():
        dbsess = get_dbsess()
        CreateAction(dbsess, d['create'].keys(), TABLES, data=d['create']).do()
        for t in d['create'].keys():
            rs = dbsess.query(TABLES[t])\
                    .filter(text('name in (%s)' %
                            ','.join(['"' + row['name'] + '"' for row in d['create'][t] ]))).all()

            assert len(rs) == len(d['create'][t])


# @pytest.mark.skip('zs')
def test_update(app_with_db_inited, data):
    """ 根据条件update
    """
    d = data['update']

    with app_with_db_inited.app_context():
        dbsess = get_dbsess()
        UpdateAction(dbsess, dist_tables=d['dist_tables'],table_map_dict=TABLES, 
                     condition=d['condition'], data=d['data']).do()


