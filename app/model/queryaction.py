""" 封装sqlalchemy URD操作
"""
import logging
from flask import current_app
from sqlalchemy import text

from .. import exc
from .parse_condition import (
    ParseCondition,
    ParseFields,
    ParseOrderBy,
    ParseGroupBy,
    ParseLimit
)


class BaseAction:
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str='', data=None):
        """ 数据库操作（URD）基类

            组装sqlalchemy.Query

            updata/delete/list/one 等 只传自己需要的参数 
        """
        self.dbsession = dbsession
        self.dist_tables = dist_tables
        self.table_map_dict = table_map_dict or TABLES
        self.condition =condition
        self.order_by_str = order_by_str
        self.group_by_str = group_by_str
        self.limit_str = limit_str
        self.data = data 
        self.action_name = ''

    def get_dist_table_map(self):
        """ 获取要查询表的map

            根据dist_tables 列表 获取map
        """
        t_maps = []
        t_not_exists = []
        for t_name in self.dist_tables:
            t_map = self.table_map_dict.get(t_name)
            if t_map:
                t_maps.append(t_map)
            else:
                t_not_exists.append(t_name)
        if t_not_exists:
            raise exc.YcmsTableNotExistsError('要查询的表不存在: ' + str(t_not_exists))
        return t_maps

    def query(self):
        """ 组装sqlalchemy 查询语句

            TODO 缺少fields解析
            TODO 需要将解析异常全部包裹 并抛出自定义异常 具体是从这里还是各个解析器做 待定
        """
        query = self.dbsession.query(*self.get_dist_table_map())
        where = ParseCondition(self.condition)
        params = where.params
        where = where.parse()
        order_by = ParseOrderBy(self.order_by_str).parse()
        group_by = ParseGroupBy(self.group_by_str).parse()
        offset, limit = ParseLimit(self.limit_str).parse()
        print('\n\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\')
        print(where, params)
        print('\n\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\')
        # !!! 不能直接使用if where  因为其未sqlalchemy.text且未实现预期的 __bool__
        if str(where): 
            query = query.filter(where)
            if params:
                query = query.params(**params)
        elif self.action_name in ('update', 'delete'):
            raise exc.YcmsDangerActionError('数据库操作，更新/删除必须限定条件')
        if self.action_name in ('list', 'one') and str(order_by):
            query = query.order_by(order_by)
        if self.action_name in ('list', 'one') and  str(group_by):
            query = query.group_by(group_by)
        if self.action_name in ('list', 'one') and  limit:
            query = query.offset(offset or 0).limit(limit)
        return query


class CreateAction(BaseAction):
    """ (批量)新增数据
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                  condition=None, order_by_str='', group_by_str='', limit_str='', data=None, bulk=True):
        """
            :param buld: 是否使用sqlalchem的 bulk_insert_mappings
            其他param参加 BaseAction
        """
        self.bulk = bulk
        self.action_name = 'create'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_str, limit_str, data)

    def do(self):
        logging.getLogger('debug').info(self.data)
        for table_name, values in self.data.items():
            t_schema = self.table_map_dict.get(table_name)
            if not t_schema:
                raise exc.YcmsTableNotExistsError(None, 'app.schema.' + table_name)
            if self.bulk: 
                if values:
                    self.dbsession.bulk_insert_mappings(t_schema, values)
            else:
                values = [t_schema(**value) for value in values]
                if values:
                    self.dbsession.add_all(values)
        return self.dbsession


class UpdateAction(BaseAction):
    """ 根据条件更新
        !!! 没有 condition（filter）直接不能操作。防止误改全表
        dbsess.query(tableMap).filter(where).update(data)
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str='', data=None):
        self.action_name = 'update'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_str, limit_str, data)

    def do(self):
        if not self.data:
            raise exc.YcmsDBDataRequiredError('缺少update所需的数据')
        # !!! 因为使用了 text() 函数 所以不能自动同步session
        # False - don’t synchronize the session. This option is the most efficient and is reliable
        # once the session is expired, which typically occurs after a commit(), or explicitly using
        # expire_all(). Before the expiration, updated objects may still remain in the session with
        # stale values on their attributes, which can lead to confusing results.

        # 'fetch' - performs a select query before the update to find objects that are matched by the
        # update query. The updated attributes are expired on matched objects.

        # 'evaluate' - Evaluate the Query’s criteria in Python straight on the objects in the
        # session. If evaluation of the criteria isn’t implemented, an exception is raised.

        # The expression evaluator currently doesn’t account for differing string collations between
        # the database and Python.
        return  self.query().update(self.data, synchronize_session=False)


class DeleteAction(BaseAction):
    """ 根据条件删除
        !!! 没有 condition（filter）直接不能操作。防止误删全表
        dbsess.query(tableMap).filter(where).update(data)
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str='', data=None):
        self.action_name = 'delete'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_str, limit_str)

    def do(self):
        return  self.query().delete()


class ListAction(BaseAction):
    """ 取多条信息
        dbsess.query(tableMap).fileds(fields).filter(where).order_by(order_by)\
            .group_by(group_by).offset(offset).limit(limit).all()
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str='', data=None):
        self.action_name = 'list'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_str, limit_str)

    def do(self):
        return self.query().all()


class OneAction(BaseAction):
    """ 取一条信息
        one()  
            结果不是一个会抛异常
            无结果: sqlalchemy.orm.exc.NoResultFound
            结果多于一个: sqlalchemy.orm.exc.MultipleResultsFound
        on_or_none()
            无结果:  返回None
            结果多于一个会抛异常
            结果多于一个: sqlalchemy.orm.exc.MultipleResultsFound
        first() == ...limit(1)
            不会抛异常
            无结果： 返回None
            因为实际使用limit(1) 查询的 所以最多返回一个结果

        dbsess.query(tableMap).fileds(fields).filter(where).order_by(order_by)\
            .group_by(group_by).offset(offset).limit(limit).one()
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str='', data=None):
        self.action_name = 'one'
        limit = '0,1'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_str, limit_str)

    def do(self):
        return self.query().all()
