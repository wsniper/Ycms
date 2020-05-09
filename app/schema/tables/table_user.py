from ..schema_base import (BaseModel, BaseSchemaMixin)
from sqlalchemy import Column, Integer, String, Text, Index

class User(BaseModel, BaseSchemaMixin):
    __tablename__ = 'user'
    name = Column('name', String(50), nullable=False, server_default='', comment='用户名')
    password = Column('password', String(256), nullable=False, server_default='', comment='密码',
                      doc='salt$sha256$encrypedpassword')




