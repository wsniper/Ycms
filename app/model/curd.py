from ..db import get_dbsess
import sqlalchemy
from sqlalchemy import text

from ..exc import YcmsTableNotExistsError
from ..schema import TABLES

def create(data):
    """ 添加数据

        这里不做任何验证，直接使用数据入库。
        验证由调用者处理。
        data: {
            table_a: [
                {field_a: va, field_b: vb...}
            ],
            table_b: [
                {field_a: va, field_b: vb...}
            ],
            ...
        }
    """
    dbsess = get_dbsess()
    try:
        for table_name, values in data.items():
            t_schema = TABLES.get(table_name)
            if not t_schema:
                raise YcmsTableNotExistsError(None, 'app.schema.' + table_name)
            dbsess.add_all([t_schema(**value) for value in values])
        dbsess.commit()
    except (sqlalchemy.exc.SQLAlchemyError, KeyError) as e:
        dbsess.rollback()
        raise e
    return dbsess


