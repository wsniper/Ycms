""" 解析 where子句的dict定义


    :param where: where子句的定义 dict格式

    where f > 1 and f2<2 or f3 like '%..%' and f4 in (...) and f5 between 
    
    {
        where: [ #or
         [(), (), ()], # and
         [(), (), ()], # and
         [(), (), ()]  # and
        ],
        order_by: ['a desc', 'b asc'],
        offset: n,
        limit: n
    }
"""


class ParseCondition:

    def __init__(self, condition, table_map):
        """ 解析  
        """
        self.where = condition.get('where', [])
        self.order_by = condition.get('order_by', '')
        self.offset = condition.get('offset', '0')
        self.limit = condition.get('limit', '0')
        self.table_name = table_map.__tablename__

        self.params = {}
        self.params_count = 0

        self.op_white_list = ('lt', 'gt', 'le', 'ge', 'eq', 'neq', 'in', 'notin', 'exists',
                              'notexists', 'like', 'between', 'offset', 'limit', 'order_by')

    def parse(self):
        pass

    def parse_where(self):
        """
        dbsess.query(map).fields(...)
            .filter(or_(and_(map.a, map.b), and_(map.c, map.d, map.e)..))
        """
        or_rs = []
        for or_item in self.where:
            for and_group in or_item:
                and_rs = []
                for item in and_group:
                    # TODO 按操作符 解析
                    op = item[1]
                    if op not in self.op_white_list:
                        raise exc.YcmsSqlOperatorError('非法操作符<' + op + '>')
                    and_rs.append(self._parse_one(item))
                or_rs.append(and_(','.join(and_rs)))
        return or_(','.join(or_rs))

    def parse_other(self):
        pass


    def _parse_one(self, tuple_str):
        """ 解析一条的公共方法

            :paramtuple_string:  带解析数据 2元素的tuple
                eg: (.., ..)
        """
        left = self._get_map_attr_or_val(tuple_str[0], True)
        right = self._get_map_attr_or_val(tuple_str[2])

        op = tuple_str[1]
        if op in ('in', 'notin', 'exsits', 'notexists'):
            param_list = []
            for i in right[0].split(','):
                param_key = self._gen_param_key(left)
                self.params[param_key] = ''.join(['"', i, '"'])
                param_list.append(':' + param_key)
            right = ''.join(['(', ','.join(param_list), ')'])
        elif op == 'between':
            param_key_1 = self._gen_param_key(left)
            param_key_2 = self._gen_param_key(left)
            right = ' and '.join(param_key_1, param_key_2)
            vals = right[0].split(',')
            self.params[param_key_1] = ''.join(['"', vals[0], '"'])
            self.params[param_key_2] = ''.join(['"', vals[1], '"'])
        else:
            # 需要将值 用param代替 防止注入
            if not right[1]:
                param_key = self._gen_param_key(left)
                self.params[param_key] = right
                right = ':' + param_key
            else: 
                right = right[0]

        return left, right

    def _gen_param_key(self, field_name):
        # params计数自增 防止重名
        self.params_count += 1
        return '_'.jon([field_name.replace('.', ''), str(self.params_count)])

    def _get_map_attr_or_val(self, string, is_table=False):
        """ 解析
            _@$@_tablename.field_name_@$@_
        """
        string = str(string)
        rs = (''.join('"', string, '"'), False)
        t_map = None
        t_map_f = None
        if string.count('.') == 1 and string.startswith('_@$@_') and string.endswith('_@$@_'):
            t_name, f_name = string.replace('_@$@_', '').split('.')
            t_map = TABLES(t_name, None)
            t_map_f = getattr(t_map, f_name)
        if all([t_map, t_map_f]):
            rs = ('.'.join([t_name, f_name]), True)
        elif is_table:
            raise exc.YcmsSqlConditionParseError(string)
        return rs


    def parse_gt(self, tuple_arg):
        """ 解析sql操作符: >

            ('_@$@_t_table_name.f_field_@$@_', 'gt', '_@$@_table_name.field2_@$@_'/val)
        """
        return text('>'.join(self._parse_one(tuple_lt)))

    def parse_lt(self, tuple_arg):
        """ 解析sql操作符: <
            ('_@$@_t_table_name.f_field_@$@_', '_@$@_table_name.field2_@$@_'/val)
        """
        return text('<'.join(self._parse_one(tuple_lt)))

    def parse_eq(self, tuple_arg):
        """ 解析sql操作符: =
        """
        return text('='.join(self._parse_one(tuple_lt)))

    def parse_ge(self, tuple_arg):
        """ 解析sql操作符: >=
        """
        return text('>='.join(self._parse_one(tuple_lt)))

    def parse_le(self, tuple_arg):
        """ 解析sql操作符: <=
        """
        return text('<='.join(self._parse_one(tuple_lt)))

    def parse_neq(self, tuple_arg):
        """ 解析sql操作符: <>
        """
        return text('<>'.join(self._parse_one(tuple_lt)))

    def parse_like(self, tuple_arg):
        """ 解析sql操作符: like
        """
        return text('like'.join(self._parse_one(tuple_lt)))

    def parse_in(self, tuple_arg):
        """ 解析sql操作符: in
        """
        return text('in'.join(self._parse_one(tuple_lt)))

    def parse_not_in(self, tuple_arg):
        """ 解析sql操作符: in
        """
        return text('not in'.join(self._parse_one(tuple_lt)))

    def parse_between(self, tuple_arg):
        """ 解析sql操作符:  between
        """
        return text('between'.join(self._parse_one(tuple_lt)))

    def parse_exists(self, tuple_arg):
        """ 解析sql操作符: exists
        """
        return text('exists'.join(self._parse_one(tuple_lt)))

    def parse_not_exists(self, tuple_arg):
        """ 解析sql操作符: exists
        """
        return text('not exists'.join(self._parse_one(tuple_lt)))

    def parse_order_by(self, tuple_arg):
        """
        """

    def parse_offset(self, offset):
        """
        """

    def parse_limit(self, limit):
        """
        """

