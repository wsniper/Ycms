from .schema_base import (BaseModel, BaseSchemaMixin)
from .tables.table_user import User


# 用于遍历使用
TABLES = {'user': User}
