""" Ycms 异常
"""
from sqlalchemy.exc import SQLAlchemyError
import pydantic
from pydantic import (PydanticTypeError, PydanticValueError)


class YcmsError(Exception):
    def __init__(self, message, data='', logit=False):
        self.message = message
        self.ycms_message = message

        self.data = data

    def __repr__(self):
        return '{class_name}<message: {message}>\n{data}'.format(
                    class_name=self.__class__.__name__,
                    message=self.message,
                    data=str(self.data)
                )



class YcmsFileNotFound(YcmsError, FileNotFoundError):
    def __init__(self, message='找不到文件'):
        self.message = message
        self.ycms_message = message


class YcmsKeyError(YcmsError, KeyError):
    def __init__(self, message='键错误'):
        self.message = message
        self.ycms_message = message



class YcmsValueError(YcmsError, ValueError):
    def __init__(self, message='值错误'):
        self.message = message
        self.ycms_message = message



class YcmsPydanticValidateError(PydanticTypeError, PydanticValueError):
    def __init__(self, message='数据验证未通过'):
        self.message = message
        self.ycms_message = message

        self.engine = 'pydantic'


class YcmsDBError(YcmsError):
    """ 数据库操作异常

        包括:
            - 解析条件并生成sqlalchemy操作语句
            - 实际操作数据库数据
    """
    def __init__(self, message, data='', logit=False):
        message = message or '数据库操作失败: ' + str(data)
        super().__init__(message, data, logit)


class YcmsSqlalchemyError(YcmsDBError, SQLAlchemyError):
    def __init__(self, message='数据库操作失败'):
        self.message = message
        self.ycms_message = message



class YcmsDBFieldNotExistsError(YcmsDBError):
    """ 的表中不存在该传入的字段
    """
    def __init__(self, message, data='', logit=False):
        message = message or '表中不存在该字段: ' + str(data)
        super().__init__(message, data, logit)


class YcmsDBDataRequiredError(YcmsDBError):
    """ update/add 需要传入数据
    """
    def __init__(self, message, data='', logit=False):
        message = message or '需要传入新增/修改数据库的数据: ' + str(data)
        super().__init__(message, data, logit)


class YcmsTableNotExistsError(YcmsDBError):
    """ 待操作的数据表不存在
    """
    def __init__(self, message, data='', logit=False):
        message = message or '待操作的数据表不存在: ' + str(data)
        super().__init__(message, data, logit)


class YcmsSqlParseError(YcmsDBError):
    """ 解析sql失败
    """
    def __init__(self, message, data='', logit=False):
        message = message or '解析sql失败 ' + str(data)
        super().__init__(message, data, logit)


class YcmsSqlOperatorError(YcmsSqlParseError):
    def __init__(self, message, data='', logit=False):
        message = message or '非法sql操作符 ' + str(data)
        super().__init__(message, data, logit)


class YcmsSqlConditionParseError(YcmsSqlParseError):
    def __init__(self, message, data='', logit=False):
        message = message or '非法sql条件 ' + str(data)
        super().__init__(message, data, logit)


class YcmsDangerActionError(YcmsError):
    def __init__(self, message, data='', logit=False):
        message = message or '危险操作被禁止 ' + str(data)
        super().__init__(message, data, logit)
