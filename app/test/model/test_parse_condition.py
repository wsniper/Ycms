from app.model.parse_condition import ParseCondition
from app.schema import TABLES

def test_parse_condition():
    """ 解析 where / limit /offset 等sql子句
    """

    d = {
        'where': [
            [('_@$@_user.name_@$@_', 'eq', 9), ('_@$@_user.id_@$@_', 'gt', '22')],
            [('_@$@_user.name_@$@_', 'like', '%ab%'), ('_@$@_user.id_@$@_', 'le', '23'), ('_@$@_user.id_@$@_', 'lt', '23')],
            [('_@$@_user.name_@$@_', 'like', '%ab%'), ('_@$@_user.id_@$@_', 'in', '2_@$@_5_@$@_3'), ('_@$@_user.id_@$@_', 'lt', '23')],
            [('_@$@_user.name_@$@_', 'between', 'aa_@$@_bb'), ('_@$@_user.id_@$@_', 'ge', '6'), ('_@$@_user.id_@$@_', 'neq', '7')],
        ]
    }

    pc = ParseCondition(d, TABLES['user'])
    stm = pc.parse()
    print(stm)
    print(pc.params)
