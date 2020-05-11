from ..db import get_dbsess
import sqlalchemy

from ..import exc  
from ..schema import TABLES


def create(data, bulk_insert=True):
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
        :param bulk_insert: 默认True 是否使用 sqlalchemy::session::bulk_insert_mappings
            False: 逐条数据执行 insert
            True： 类似 insert ... values((...), (...)) 更有效率。
    """
    dbsess = get_dbsess()
    try:
        for table_name, values in data.items():
            t_schema = TABLES.get(table_name)
            if not t_schema:
                raise exc.YcmsTableNotExistsError(None, 'app.schema.' + table_name)
            if bulk_insert: 
                if values:
                    dbsess.bulk_insert_mappings(t_schema, values)
            else:
                values = [t_schema(**value) for value in values]
                if values:
                    dbsess.add_all(values)
        dbsess.commit()
    except (sqlalchemy.exc.SQLAlchemyError, KeyError) as e:
        dbsess.rollback()
        raise e
    return dbsess, t_schema



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

