from ..db import get_dbsess
import sqlalchemy

from ..import exc  
from ..schema import TABLES

from .queryaction import (CreateAction, UpdateAction, DeleteAction, ListAction, OneAction)


def create(data, bulk=True, commit_it=True):
    """ 添加数据

        这里不做任何验证，直接使用数据入库。
        验证由调用者处理。

        :param data: 需要插入的数据
            eg: {
                table_a: [
                    {field_a: va, field_b: vb...}
                ],
                table_b: [
                    {field_a: va, field_b: vb...}
                ],
                ...
            }
        :param bulk: 默认True 是否使用 sqlalchemy::session::bulk_insert_mappings
            False: 逐条数据执行 insert
            True： 类似 insert ... values((...), (...)) 更有效率。
    """
    dbsess = get_dbsess()
    try:
        dbsess = CreateAction(dbsess, data.keys(), TABLES, data=data, bulk=bulk).do()
        if commit_it:
            dbsess.commit()
    except Exception as e:
        dbsess.rollback()
        raise e
    return dbsess



def update(data, where, orderby=None, offset=0, limit=10, bulk_update=True):
    """ 更新数据
        
        :param data: 需要插入的数据
            eg: {
                table_a: [
                    {field_a: va, field_b: vb...}
                ],
                table_b: [
                    {field_a: va, field_b: vb...}
                ],
                ...
            }
        :param bulk_update: 默认True 是否使用 sqlalchemy::session::bulk_update_mappings
            False: 逐条数据执行 update
            True： 类似 update ... values((...), (...)) 更有效率。
    """
    dbsess = get_dbsess()
    try:
        if bulk:
            pass
        else:
            pass
    except Exception as e:
        dbsess.rollback()
        raise e
    return dbsess

