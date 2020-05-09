from ..db import get_dbsess
from ..schema import User

def create(data):
    """ 添加数据

        这里不做任何验证，直接使用数据入库。
        验证在调用者处处理。
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

