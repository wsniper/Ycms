from .schema_base import (BaseModel, BaseSchemaMixin)
from .tables.table_user import User
from .tables.table_roll import Roll


# 用于遍历使用
TABLES = {'user': User, 'roll': Roll}
