""" Ycms 异常
"""

from ..conf import EXC


class YcmsError(Exception):
    def __init__(self, message, data, logit=False):
        self.message = message
        self.data = data

    def __repr__(self):
        return '{class_name}<message: {message}>\n{data}'.format(
                    class_name=self.__class__.__name__,
                    message=self.message,
                    data=str(self.data)
                )


class YcmsTableNotExistsError(YcmsError):
    """ 待操作的数据表不存在
    """
    def __init__(self, message, data, logit=False):
        message = message or '待操作的数据表不存在: ' + str(data)
        super().__init__(message, data, logit)

