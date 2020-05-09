from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..conf import DB
from flask import g


def get_dbsess():
    """ 获取sqlalchemy session  

        保证在单个请求中是单例
    """
    dbsess =  g.get('dbsession')
    if not dbsess:
        engine = create_engine(DB.driver, echo=DB.is_debug)
        Sess = sessionmaker(bind=engine)
        dbsess = Sess()
        g.dbsession = dbsess
    return dbsess


def execute(sql_str, many=False):
    """ 执行原始sql 返回游标
    """
    dbsess = get_dbsess()
    conn = dbsess.bind.connect()
    cursor = None
    if many:
        cursor = conn.executemany(conn)
    else:
        cursor = conn.execute(conn)
    return cursor
