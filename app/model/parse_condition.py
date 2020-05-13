""" 解析 where子句的dict定义

    :param where: where子句的定义 dict格式

    where f > 1 and f2<2 or f3 like '%..%' and f4 in (...) and f5 between 
    [ #or
        [(), (), ()], # and
        [(), (), ()], # and
        [(), (), ()]  # and
    ]
"""

from sqlalchemy import text, or_, and_, asc, desc

from .. import exc
from ..util import num

from ..logger import dlogger


class ParseSqlMixIn:
    def get_map_attr_or_val(self, val, is_field=False, return_field_obj=False, parse_field_with_no_flag_char=False):
        """ 将val 解析为数据库字段（返回值第二个布尔值标示）或普通字符串

            :param parse_field_with_no_flag_char: 不带也按照字段名解析
            tablename.field_name
        """
        self.splitor = getattr(self, 'splitor', '')
        val = str(val) #str(val) if isinstance(val, (int, float)) else ''.join(['"', str(val), '"'])
        rs = (val, False)
        t_map = None
        t_map_f = None
        if (val.count('.') == 1 and val.startswith(self.splitor) and 
                                    val.endswith(self.splitor) or parse_field_with_no_flag_char):
            t_name, f_name = val.replace(self.splitor, '').split('.')
            t_map = self.table_map_dict.get(t_name, None)
            t_map_f = getattr(t_map, f_name, None)
            if not t_map_f:
                raise exc.YcmsSqlConditionParseError('字段不存在 <' + t_name + '.' + f_name + '>')

        if all([t_map, t_map_f]):
            if return_field_obj:
                rs = t_map_f, True
            else:
                rs = ('.'.join([t_name, f_name]), True)
        elif is_field:
            raise exc.YcmsSqlConditionParseError(val)
        return rs


class ParseCondition(ParseSqlMixIn):

    def __init__(self, condition, table_map_dict=None):
        """ 解析  where子句及limit/offset/order_by

            仅实现了下面 op_white_list中的操作符/关键字
            
            # TODO 优化查询性能，将有索引的字段条件放到最前面

            专用分隔符：
                in/not in/exists/not exists/between中的逗号使用该分隔符代替
                表名字段 前后用该分隔符包裹
        """
        # 出现在“值”位置的‘字段’以及in/between等多值项的值使用self.splitor
        self.splitor = '_@$@_'
        self.where = condition or []
        self.params = {}
        self.params_count = 0
        self.op_white_list = ('lt', 'gt', 'le', 'ge', 'eq', 'neq', 'in', 'notin', 'exists',
                              'notexists', 'like', 'between')
        self.table_map_dict = table_map_dict

    def parse(self):
        """ 解析where子句

            dbsess.query(map).fields(...)
                .filter(or_(and_(map.a, map.b), and_(map.c, map.d, map.e)..))
        """
        or_rs = []
        i = 0
        for or_item in self.where:
            i+=1
            and_rs = []
            for and_item in or_item:
                op = and_item[1]
                if op not in self.op_white_list:
                    raise exc.YcmsSqlOperatorError('非法操作符<' + op + '>')
                m = getattr(self, 'parse_'+op)
                and_rs.append(m(and_item))
            or_rs.append(and_(*and_rs))
            # print(or_rs, i)
        return or_(*or_rs)

    def _parse_one(self, tuple_str):
        """ 解析一条的公共方法

            :paramtuple_string:  待解析数据 3元素的tuple 
            !!! 只有右边（值）是字段时采用 分隔符 
                eg: ('user.name', 'like', '%adf')
                    ('user.age', 'eq', 'user.id')
        """
        left = self.get_map_attr_or_val(tuple_str[0], True, False, True)
        right = self.get_map_attr_or_val(tuple_str[2])

        op = tuple_str[1]
        if op in ('in', 'notin', 'exsits', 'notexists'):
            param_list = []
            for i in right[0].split(self.splitor):
                param_key = self._gen_param_key(left[0])
                self.params[param_key] = i
                param_list.append(':' + param_key)
            right = ''.join(['(', ','.join(param_list), ')'])
        elif op == 'between':
            param_key_1 = self._gen_param_key(left[0])
            param_key_2 = self._gen_param_key(left[0])
            vals = right[0].split(self.splitor)
            if len(vals) != 2:
                raise exc.YcmsSqlConditionParseError('between 需要两个值隔开')
            right = ' and '.join([':' + param_key_1, ':' + param_key_2])
            self.params[param_key_1] = vals[0]
            self.params[param_key_2] = vals[1]
        elif op == 'like':
            param_key = self._gen_param_key(left[0])
            val = right[0].strip('%')
            val_ = val.replace('%', '%%')
            self.params[param_key] = right[0].replace(val, val_)
            right = ':' + param_key
        else:
            # “值”不是“字段”
            if not right[1]:
                param_key = self._gen_param_key(left[0])
                self.params[param_key] = right[0]
                right = ':' + param_key
            else: 
                right = right[0]
        left = left[0]
        return left, right

    def _gen_param_key(self, field_name):
        # params计数自增 防止重名
        self.params_count += 1
        return '_'.join([field_name.replace('.', ''), str(self.params_count)])

    def parse_gt(self, tuple_arg):
        """ 解析sql操作符: > gt

            ('t_table_name.f_field', 'gt', 'table_name.field2'/val)
        """
        return text(' > '.join(self._parse_one(tuple_arg)))

    def parse_lt(self, tuple_arg):
        """ 解析sql操作符: < lt
            ('t_table_name.f_field', 'table_name.field2'/val)
        """
        return text(' < '.join(self._parse_one(tuple_arg)))

    def parse_eq(self, tuple_arg):
        """ 解析sql操作符: = eq
        """
        return text(' = '.join(self._parse_one(tuple_arg)))

    def parse_ge(self, tuple_arg):
        """ 解析sql操作符: >= ge
        """
        return text(' >= '.join(self._parse_one(tuple_arg)))

    def parse_le(self, tuple_arg):
        """ 解析sql操作符: <= le
        """
        return text(' <= '.join(self._parse_one(tuple_arg)))

    def parse_neq(self, tuple_arg):
        """ 解析sql操作符: <> neq
        """
        return text(' <> '.join(self._parse_one(tuple_arg)))

    def parse_like(self, tuple_arg):
        """ 解析sql操作符: like like
        """
        rs = self._parse_one(tuple_arg)
        return text(' like '.join(rs)) 

    def parse_in(self, tuple_arg):
        """ 解析sql操作符: in in
        """
        return text(' in '.join(self._parse_one(tuple_arg)))

    def parse_not_in(self, tuple_arg):
        """ 解析sql操作符: not in notin
        """
        return text(' not in '.join(self._parse_one(tuple_arg)))

    def parse_between(self, tuple_arg):
        """ 解析sql操作符:  between between
        """
        return text(' between '.join(self._parse_one(tuple_arg)))

    def parse_exists(self, tuple_arg):
        """ 解析sql操作符: exists exists
        """
        return text(' exists '.join(self._parse_one(tuple_arg)))

    def parse_not_exists(self, tuple_arg):
        """ 解析sql操作符: not exists notexists
        """
        return text(' not exists '.join(self._parse_one(tuple_arg)))


class ParseOrderBy(ParseSqlMixIn):
    """ 解析orderby
    """
    def __init__(self, src, table_map_dict=None):
        self.src = src or [] 
        self.fn = {'asc': asc, 'desc': desc}
        self.table_map_dict = table_map_dict

    def parse(self):
        """ 执行解析
        """
        rs = []
        for item in self.src:
            fn = self.fn.get(item[1])
            field = self.get_map_attr_or_val(item[0], True)
            if not all([len(item) == 2, fn]):
                raise exc.YcmsSqlConditionParseError('order_by 需要明确指出升降序' + str(item))
            rs.append(fn(text(field[0])))
        return rs


class ParseFields(ParseSqlMixIn):
    """ 解析 需要操作的字段

        get_map_attr_or_val() 来确保 是真实字段
        :return: text('f, f, f, f')
    """
    def __init__(self, src, table_map_dict=None):
        self.src = src or []
        self.table_map_dict = table_map_dict

    def parse(self):
        rs = []
        for item in self.src:
            field = self.get_map_attr_or_val(item, True, True,  True)
            rs.append(field[0])
        return rs


class ParseLimit(ParseSqlMixIn):
    def __init__(self, src):
        """ 解析  offset, limit

            :param src: 逗号分隔的正整数
        """
        self.src = src or '0,9999999'

    def parse(self):
        """
        """
        offset = 0
        limit = 9999999
        if self.src:
            offset, limit = [num.parse_int_or_zero_unsigned(str(i).replace(' ', '')) 
                                 for i in str(self.src).split(',')]

        limit = limit if limit > 0  else 9999999
        return offset, limit


class ParseGroupBy(ParseSqlMixIn):
    def __init__(self, src, table_map_dict=None):
        """
            :param src: 带解析字符串
                eg: table.field$sum|table.field,avg|table.field
        """
        self.src = src or ''
        self.sql_fn_white_list = ('sum', 'count', 'avg', 'max', 'min')
        self.table_map_dict = table_map_dict

    def parse(self):
        """
        """
        if not self.src:
            return ''
        tmp = self.src.split('$')
        by_field = self.get_map_attr_or_val(tmp[0], True, False, True)[0]
        fn_strs = []
        if len(tmp) > 1:
            fn_fields = [item.split('|') for item in tmp[1].split(',')]
            for item in fn_fields:
                fn, field = item
                if fn not in self.sql_fn_white_list:
                    raise exc.YcmsSqlConditionParseError('group by 禁止使用该函数<' + fn + '>')
                field = self.get_map_attr_or_val(field, True, False, True)
                if not field[1]:
                    raise exc.YcmsSqlConditionParseError(
                        '字段不存在 <' + field[0] + '>'
                    )
                fn_strs.append('%s(%s) as %s' % (fn, field[0], 
                                                 '_'.join([fn, field[0].replace('.', '_')])))
        if fn_strs:
            fn_strs = ', '.join(fn_strs)
        return by_field,  fn_strs
