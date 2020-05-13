import pytest
from sqlalchemy import text

import app.exc as exc
from app.db import get_dbsess
from app.test import app_with_db_inited

from app.schema import TABLES
from app.model.queryaction import (CreateAction, UpdateAction, DeleteAction, ListAction, OneAction)


skip_all = True
@pytest.fixture
def create_some_row(app_with_db_inited, data):
    with app_with_db_inited.app_context():
        dbsess = get_dbsess()
        CreateAction(dbsess, data['create'].keys(), TABLES, data=data['create']).do()
        dbsess.commit()
        return dbsess


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
            'dist_tables': ['roll'], #!!!仅允许具有一个元素的list/tuple/set 且 该元素必须是condition中引用的表
            'data': {'name': 'updated name', 'last_update_time': 9777777},
            'condition': [
                [('roll.id', 'lt', 12), ('roll.id', 'gt', '7'), ('roll.name', 'like', '%h%')],
                [('roll.name', 'like', '%a'), ('roll.id', 'lt', 6)]
            ]
        },
        'delete':{
            'dist_tables': ['roll'], #!!!仅允许具有一个元素的list/tuple/set 且 该元素必须是condition中引用的表
            'condition': [
                [('roll.id', 'lt', 5), ('roll.id', 'gt', '2')],
            ]
        },
        'delete_2':{
            'dist_tables': ['roll'], #!!!仅允许具有一个元素的list/tuple/set 且 该元素必须是condition中引用的表
            'condition': [
                [('roll.name', 'like', '%aa')],
            ]
        },
        'delete_3':{
            'dist_tables': ['roll'], #!!!仅允许具有一个元素的list/tuple/set 且 该元素必须是condition中引用的表
            'condition': [
                [('roll.id', 'in', 'a_@$@_b')],
            ]
        },
        'delete_4':{
            'dist_tables': ['roll'], #!!!仅允许具有一个元素的list/tuple/set 且 该元素必须是condition中引用的表
            'condition': [
                [('roll.id', 'between', '8_@$@_11')],
            ]
        },
        'list': {
            'dist_tables': ['roll'],
            'condition': [
                # [('roll.id', 'like', '%2')],
                [('roll.id', 'lt', '99'), ]
            ],
            'limit': '3,3',
            'fields': ['roll.id', 'roll.name'],
            'order_by': [('roll.id', 'desc'), ('roll.name', 'asc')] 
        },
        'error_data':{
            'create': {
                'table_not_exists': {
                    'some_table': []
                },
                # fields error
                'fields_error': {
                    'user': [{'a':1, 'b':2}]
                }
            },
            'update': {
                'table_not_exists': {
                    'dist_tables': ['rollc'],
                    'data': {'name': 'updated name', 'last_update_time': 9777777},
                    'condition': [
                        [('roll.id', 'lt', 12), ('roll.id', 'gt', '7'), ('roll.name', 'like', '%h%')],
                        [('roll.name', 'like', '%a'), ('roll.id', 'lt', 6)]
                    ]
                },
                # fields error
                'fields_error': {
                    'dist_tables': ['roll'],
                    'data': {'name': 'updated name', 'last_update_time': 9777777},
                    'condition': [
                        [('some_other_table_may_make_fileds_error.id', 'lt', 12), ('roll.id', 'gt', '7'), ('roll.name', 'like', '%h%')],
                        [('roll.name', 'like', '%a'), ('roll.id', 'lt', 6)]
                    ]
                },
                'fields_error_2': {
                    'dist_tables': ['roll'],
                    'data': {'name': 'updated name', 'last_update_time': 9777777},
                    'condition': [
                        [('roll.wrong_field_not_in_table', 'lt', 12), ('roll.id', 'gt', '7'), ('roll.name', 'like', '%h%')],
                        [('roll.wrong_field_not_in_table_2', 'like', '%a'), ('roll.id', 'lt', 6)]
                    ]
                },
            },
        }
    }
    yield d


@pytest.mark.skipif(skip_all, reason='just skip it')
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



@pytest.mark.skipif(skip_all, reason='just skip it')
def test_create_table_not_exists_error(app_with_db_inited, data):
    """ 
    """
    d = data['error_data']['create']['table_not_exists']
    with pytest.raises(exc.YcmsTableNotExistsError):
        with app_with_db_inited.app_context():
            dbsess = get_dbsess()
            CreateAction(dbsess, d.keys(), TABLES, data=d).do()
            for t in d.keys():
                rs = dbsess.query(TABLES[t])\
                        .filter(text('name in (%s)' %
                                ','.join(['"' + row['name'] + '"' for row in d[t] ]))).all()

                assert len(rs) == len(d[t])


@pytest.mark.skipif(skip_all, reason='just skip it')
def test_create_fields_error(app_with_db_inited, data):
    """ 仅 bulk=True 
    """
    d = data['error_data']['create']['fields_error']
    with pytest.raises(exc.YcmsDBFieldNotExistsError):
        with app_with_db_inited.app_context():
            dbsess = get_dbsess()
            CreateAction(dbsess, d.keys(), TABLES, bulk=True, data=d).do()
            for t in d.keys():
                rs = dbsess.query(TABLES[t]).all()
                print(rs)



@pytest.mark.skipif(skip_all, reason='just skip it')
def test_update(app_with_db_inited, data):
    """ 根据条件update
    """
    d = data['update']

    with app_with_db_inited.app_context():
        dbsess = get_dbsess()
        UpdateAction(dbsess, dist_tables=d['dist_tables'],table_map_dict=TABLES, 
                     condition=d['condition'], data=d['data']).do()


@pytest.mark.skipif(skip_all, reason='just skip it')
def test_update_when_exec_update_sqltable_not_exists_error(app_with_db_inited, data):
    """ 
    """
    d = data['error_data']['update']['table_not_exists']

    with pytest.raises(exc.YcmsTableNotExistsError):
        with app_with_db_inited.app_context():
            dbsess = get_dbsess()
            UpdateAction(dbsess, dist_tables=d['dist_tables'],table_map_dict=TABLES, 
                         condition=d['condition'], data=d['data']).do()



@pytest.mark.skipif(skip_all, reason='just skip it')
def test_update_use_other_table_name_on_field_prefix_raise_when_parse_condition_fields_not_exitsts_error(app_with_db_inited, data):
    """ 根据条件update
    """
    d = data['error_data']['update']['fields_error']

    with pytest.raises(exc.YcmsSqlConditionParseError):
        with app_with_db_inited.app_context():
            dbsess = get_dbsess()
            UpdateAction(dbsess, dist_tables=d['dist_tables'],table_map_dict=TABLES, 
                         condition=d['condition'], data=d['data']).do()



@pytest.mark.skipif(skip_all, reason='just skip it')
def test_update_use_wrong_field_name_of_right_table_raise_when_parse_condition_fields_not_exitsts_error(app_with_db_inited, data):
    """ 根据条件update
    """
    d = data['error_data']['update']['fields_error_2']

    with pytest.raises(exc.YcmsSqlConditionParseError):
        with app_with_db_inited.app_context():
            dbsess = get_dbsess()
            UpdateAction(dbsess, dist_tables=d['dist_tables'],table_map_dict=TABLES, 
                         condition=d['condition'], data=d['data']).do()


@pytest.mark.skipif(skip_all, reason='just skip it')
def test_update_no_condition_error(app_with_db_inited, data):
    """ 缺失condition 报异常 exc.YcmsDangerActionError 
    """
    d = data['update']

    with pytest.raises(exc.YcmsDangerActionError):
        with app_with_db_inited.app_context():
            dbsess = get_dbsess()
            UpdateAction(dbsess, dist_tables=d['dist_tables'],table_map_dict=TABLES, 
                         condition=[], data=d['data']).do()


@pytest.mark.skipif(skip_all, reason='just skip it')
def test_delete(app_with_db_inited, data):
    """ 删除
    """
    d = data['delete']
    d_2 = data['delete_2']
    d_3 = data['delete_3']
    d_4 = data['delete_4']
    with app_with_db_inited.app_context():
        dbsess = get_dbsess()
        CreateAction(dbsess, data['create'].keys(), TABLES, data=data['create']).do()
        dbsess.commit()
        DeleteAction(dbsess, dist_tables=d['dist_tables'],table_map_dict=TABLES, 
                     condition=d['condition']).do()
        dbsess.commit()
        ids = []
        for row in dbsess.query(TABLES[d['dist_tables'][0]]).with_entities(
                                TABLES[d['dist_tables'][0]].id, TABLES[d['dist_tables'][0]].name
                            ).all():
            ids.append(row[0])
            print(row)
        assert ids == [1, 2, 5, 6, 7, 8, 9]
        DeleteAction(dbsess, dist_tables=d_2['dist_tables'],table_map_dict=TABLES, 
                     condition=d_2['condition']).do()
        dbsess.commit()
        ids = []
        for row in dbsess.query(TABLES[d['dist_tables'][0]]).with_entities(
                                TABLES[d['dist_tables'][0]].id, TABLES[d['dist_tables'][0]].name
                            ).all():
            ids.append(row[0])
        assert len(ids) == 6 and ids == [2, 5, 6, 7, 8, 9]
        DeleteAction(dbsess, dist_tables=d_3['dist_tables'],table_map_dict=TABLES, 
                     condition=d_3['condition']).do()
        dbsess.commit()
        ids = []
        for row in dbsess.query(TABLES[d['dist_tables'][0]]).with_entities(
                                TABLES[d['dist_tables'][0]].id, TABLES[d['dist_tables'][0]].name
                            ).all():
            ids.append(row[0])
        assert len(ids) == 6 and ids == [2, 5, 6, 7, 8, 9]
        DeleteAction(dbsess, dist_tables=d_4['dist_tables'],table_map_dict=TABLES, 
                     condition=d_4['condition']).do()
        dbsess.commit()

        ids = []
        for row in dbsess.query(TABLES[d['dist_tables'][0]]).with_entities(
                                TABLES[d['dist_tables'][0]].id, TABLES[d['dist_tables'][0]].name
                            ).all():
            ids.append(row[0])
        assert ids == [2, 5, 6, 7]


# @pytest.mark.skipif(skip_all, reason='just skip it')
def test_list(app_with_db_inited, data, create_some_row):
    with app_with_db_inited.app_context():
        d = data['list']
        dbsess = get_dbsess()
        rs = ListAction(dbsess, table_map_dict=TABLES, dist_tables=d['dist_tables'],
                        condition=d['condition'], order_by=d['order_by'],
                        fields=d['fields'], limit=d['limit']).do()
        print(rs)

# @pytest.mark.skipif(skip_all, reason='just skip it')
def test_one(app_with_db_inited, data, create_some_row):
    with app_with_db_inited.app_context():
        d = data['list']
        dbsess = get_dbsess()
        rs = OneAction(dbsess, table_map_dict=TABLES, dist_tables=d['dist_tables'],
                        condition=d['condition'], fields=d['fields']).do()
        print([rs.name, rs.id])
