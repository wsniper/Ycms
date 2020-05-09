from .base import get_dbsess
from ..schema import BaseModel


def init_db():
    """ 初始化系统数据库

        建立所有表，在app/schema/tables中定义的
    """
    dbsess = get_dbsess()
    engine = dbsess.bind
    return BaseModel.metadata.create_all(engine)

    


