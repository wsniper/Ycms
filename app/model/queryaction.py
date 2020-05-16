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
                      condition=None, fields=None, order_by_list=None, group_by_str='', limit_str='', data=None):
        """ 数据库操作（URD）基类

            组装sqlalchemy.Query

            updata/delete/list/one 等 只传自己需要的参数 
        """
        self.dbsession = dbsession
        self.dist_tables = dist_tables
        self.table_map_dict = table_map_dict
        self.fields = fields
        self.condition =condition
        self.order_by_list = order_by_list
        self.group_by_str = group_by_str
        self.limit_str = limit_str
        self.data = data 

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
            raise exc.YcmsTableNotExistsError('要操作的表不存在: ' + str(t_not_exists))
        return t_maps

    def get_fields_name(self, mapper):
        """ 取 数据表 字段名列表

            :param mapper: sqlalchemy table mapper
        """
        return [f_name for f_name in tuple(mapper.__dict__) if not f_name.startswith('_')]


    def field_in_mapper(self, f_name, mapper):
        """ 检查字段名 是否在mapper中
        """
        return f_name in self.get_fields_name(mapper)


    def keys_not_in_mapper(self, dict_data, mapper):
        """ 获取 所有不在mapper中的dict_data 的keys

            为空则说明都在（或者 dict_data 是空的）
        """
        return [k for k in dict_data.keys() if not self.field_in_mapper(k, mapper)]


    def query(self):
        """ 组装sqlalchemy 查询语句

        """
        query = self.dbsession.query(*self.get_dist_table_map())
        fields = ParseFields(self.fields, self.table_map_dict).parse()
        if fields:
            query = query.with_entities(*fields)
        where = ParseCondition(self.condition, self.table_map_dict)
        params = where.params
        where = where.parse()
        order_by = ParseOrderBy(self.order_by_list, self.table_map_dict).parse()
        group_by = ParseGroupBy(self.group_by_str).parse()
        offset, limit = ParseLimit(self.limit_str).parse()
        # !!! 不能直接使用if where/order_by/group_by  因为其未sqlalchemy.text且未实现预期的 __bool__
        if len(str(where)) > 0: 
            query = query.filter(where)
            if params:
                query = query.params(**params)
        elif self.action_name in ('update', 'delete'):
            raise exc.YcmsDangerActionError('数据库危险操作! <%s，必须限定条件(需要where子句)>' % self.action_name)
        if self.action_name in ('list', 'one') and order_by:
            query = query.order_by(*order_by)
        if self.action_name in ('list', 'one') and  str(group_by):
            query = query.group_by(group_by)
        if self.action_name in ('list', 'one') and  limit:
            query = query.offset(offset or 0).limit(limit)
        return query


class CreateAction(BaseAction):
    """ (批量)新增数据
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, data=None, bulk=True):
        """
            :param buld: 是否使用sqlalchem的 bulk_insert_mappings
            其他param参加 BaseAction
        """
        self.bulk = bulk
        self.action_name = 'create'
        super().__init__(dbsession, dist_tables, table_map_dict, None, None, None, None, None, data)



    def do(self):
        # logging.getLogger('debug').info(self.data)
        for table_name, values in self.data.items():
            mapper = self.table_map_dict.get(table_name)
            if not mapper:
                raise exc.YcmsTableNotExistsError(None, 'app.schema.' + table_name)
            if not values:
                raise exc.YcmsDBDataRequiredError('请传入需要添加的数据字典列表')
            for value in values:
                err_keys = self.keys_not_in_mapper(value, mapper)
                if err_keys:
                    raise exc.YcmsDBFieldNotExistsError('数据表%s中不存在字段<%s>' %
                                                        (mapper, err_keys))
            if self.bulk: 
                if values:
                    self.dbsession.bulk_insert_mappings(mapper, values)
            else:
                values = [mapper(**value) for value in values]
                if values:
                    self.dbsession.add_all(values)
        return self.dbsession


class UpdateAction(BaseAction):
    """ 根据条件更新
        !!! 没有 condition（filter）直接不能操作。防止误改全表
        dbsess.query(tableMap).filter(where).update(data)
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, condition, data=None, bulk=True):
        self.action_name = 'update'
        self.bulk = bulk
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, None, None, None, None, data)

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
        # todo  self.dbsession.bulk_update_mapping(mapper, data)
        return  self.query().update(self.data, synchronize_session=False)


class DeleteAction(BaseAction):
    """ 根据条件删除
        !!! 没有 condition（filter）直接不能操作。防止误删全表
        dbsess.query(tableMap).filter(where).update(data)
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, condition):
        self.action_name = 'delete'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, None, None, None, None, None)

    def do(self):
        return  self.query().delete(synchronize_session=False)


class ListAction(BaseAction):
    """ 取多条信息
        dbsess.query(tableMap).fileds(fields).filter(where).order_by(order_by)\
            .group_by(group_by).offset(offset).limit(limit).all()
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition=None, fields=None,  order_by=None, group_by_str='', limit=''):
        self.action_name = 'list'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, fields, order_by, group_by_str, limit)

    def do(self):
        return self.query().all()


class OneAction(ListAction):
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
    def __init__(self, dbsession, dist_tables, table_map_dict, condition=None, fields=None):
        self.action_name = 'one'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                      condition, fields, None, None, '0,1')

    def do(self):
        rs = super().do()
        return rs[0] if rs else None


class DBAction:
    """ 再次封装CURD 简化公共参数传递

        将必填参数 dbsession / dist_tables / table_map_dict 放到类初始化时传入
        这样，若一个逻辑单元（函数/方法等）使用相同的上述三个公共参数时可实例化
        （传入一次上述参数）后直接使用实例来简化输入，专注业务逻辑
    """
    def __init__(self, dbsession, dist_tables, table_map_dict):
        self.dbsession = dbsession 
        self.dist_tables = dist_tables
        self.table_map_dict = table_map_dict

    def create(self, table, data, bulk=False):
        """ 单条添加

            :param data: 需要添加的数据 dict() or [[dict]]
        """
        if all([table, isinstance(data, dict)]):
            data = {table: [data]}
        return CreateAction(
            self.dbsession,
            self.dist_tables,
            self.table_map_dict,
            data=data,
            bulk=bulk
        ).do()


    def create_all(self, data, bulk=False):
        """ 批量添加
            
            :return: self.dbsession
        """
        return CreateAction(
            self.dbsession,
            self.dist_tables,
            self.table_map_dict,
            data=data,
            bulk=bulk
        ).do()

    def update(self, condition, data, bulk=False):
        """ 根据条件更新
            
            :return: self.dbsession
        """
        return UpdateAction(
            self.dbsession,
            self.dist_tables,
            self.table_map_dict,
            condition=condition,
            data=data,
            bulk=bulk
        ).do()

    def delete(self, condidtion):
        return DeleteAction(
            self.dbsession,
            self.dist_tables,
            self.table_map_dict,
            condition=condition
        ).do()

    def list(self, fields=None, condition=None, order_by=None, group_by=None, limit=''):
        """ 取多条记录

            :return: list[mapper] or empty list[]
        """
        return ListAction(
            self.dbsession,
            self.dist_tables,
            self.table_map_dict,
            fields=fields,
            condition=condition,
            order_by=order_by,
            group_by=group_by,
            limit=limit
        ).do()

    def one(self, fields=None, condition=None, order_by=None, group_by=None, limit=''):
        """ 取一条记录

            :return: sqlalchemy table mapper  or None
        """
        return OneAction(
            self.dbsession,
            self.dist_tables,
            self.table_map_dict,
            fields=fields,
            condition=condition
        ).do()
