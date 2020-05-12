""" 封装sqlalchemy URD操作
"""

from .parse_condition import (
    ParseCondition,
    ParseFields,
    ParseOrderby,
    ParseGroupBy,
    ParseLimit
)


class BaseAction:
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str=''):
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
        for t_name in self.form_tables:
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
        """
        query = dbsess.query(*self.get_dist_table_map())
        where = ParseCondition(self.condition).parse()
        order_by = ParseOrderby(self.order_by_str).parse()
        group_by = Parsegroupby(self.group_by_str).parse()
        offset, limit = ParseLimit(self.limit_str).parse()
        if where:
            query = query.filter(where)
        elif self.action_name in ('update', 'delete'):
            raise exc.YcmsDangerActionError('数据库操作，更新/删除必须限定条件')
        if self.action_name in ('list', 'one') and order_by:
            query = query.order_by(order_by)
        if self.action_name in ('list', 'one') and  group_by:
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
        self.action_name = 'create'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_st, limit)

    def do(self):
        for table_name, values in data.items():
            t_schema = self.table_map_dict.get(table_name)
            if not t_schema:
                raise exc.YcmsTableNotExistsError(None, 'app.schema.' + table_name)
            if bulk_insert: 
                if values:
                    dbsess.bulk_insert_mappings(t_schema, values)
            else:
                values = [t_schema(**value) for value in values]
                if values:
                    dbsess.add_all(values)
        return dbsess


class UpdateAction(BaseAction):
    """ 根据条件更新
        !!! 没有 condition（filter）直接不能操作。防止误改全表
        dbsess.query(tableMap).filter(where).update(data)
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str='', data=None):
        self.action_name = 'update'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_st, limit)

    def do(self):
        return  self.query().update(data)


class DeleteAction(BaseAction):
    """ 根据条件删除
        !!! 没有 condition（filter）直接不能操作。防止误删全表
        dbsess.query(tableMap).filter(where).update(data)
    """
    def __init__(self, dbsession, dist_tables, table_map_dict, 
                      condition, order_by_str='', group_by_str='', limit_str='', data=None):
        self.action_name = 'delete'
        super().__init__(dbsession, dist_tables, table_map_dict, 
                         condition, order_by_str, group_by_st, limit)

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
                         condition, order_by_str, group_by_st, limit, data)

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
                         condition, order_by_str, group_by_st, limit, data)

    def do(self):
        return self.query().all()
