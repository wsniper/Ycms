from ..schema_base import (BaseModel, BaseSchemaMixin)
from sqlalchemy import Column, Integer, String, Text, Index

class Roll(BaseModel, BaseSchemaMixin):
    __tablename__ = 'roll'
    name = Column('name', String(50), nullable=False, server_default='', comment='程序调用名')
    title = Column('title', String(50), nullable=False, server_default='', comment='汉字简称')
    description = Column('description', String(50), nullable=False, server_default='',
                         comment='汉字描述')

