from sqlalchemy import Column, Integer, String

from app.schema import (BaseModel, BaseSchemaMixin )

def test_base_schema_mixin():
    """ 测试调用 BaseSchemaMixin 创建“自动添加公共字段的”sqlalchemy 表map

        BaseSchemaMixin 已包含字段：id / status / last_update_time / add_time
    """

    class Test(BaseModel, BaseSchemaMixin):
        __tablename__ = 'test'
        name = Column('name', String(30), nullable=False, server_default='', comment='test name')


    t = Test(id=9, status=7, add_time='123123', last_update_time='122122', name='abc')

    assert t.id == 9
    assert t.status == 7
    assert t.name == 'abc'
    assert t.add_time == '123123'
    assert t.last_update_time == '122122'
    assert t.__tablename__ == 'test'
    print('\n', t)
    assert str(t) == "<Test('id': 9, 'status': 7, 'add_time': '123123',"\
                     " 'last_update_time': '122122', 'name': 'abc')>"





