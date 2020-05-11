import time

from sqlalchemy import Column, Integer, String, Text, Index
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()

class BaseSchemaMixin:
    id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)  
    # status: 0->已删除 1->禁用 99->正常
    status = Column('status', Integer, nullable=False, server_default='99')  
    add_time = Column('add_time', Integer, nullable=False, 
                              default=lambda x: str(int(time.time())),
                              onupdate=lambda x: str(int(time.time())), server_default='0')  
    last_update_time = Column('last_update_time', Integer, nullable=False, 
                              default=lambda x: str(int(time.time())),
                              onupdate=lambda x: str(int(time.time())), server_default='0')  

    def __repr__(self):
        return '<{table_name}{fields}>'\
                .format(table_name=self.__class__.__name__, 
                    fields=str( 
                        {k:v for k, v in self.__dict__.items() if not k.startswith('_')}
                    ).replace('{', '(').replace('}', ')')
                )


