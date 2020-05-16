import logging
import functools
from flask import redirect
""" 通用的请求handler的装饰器

    直接将返回前端的数据格式封装。无论请求成功或失败都可返回下面数据格式
        {
            'status': 'SUCCESS/ERROR',
            'data': [],
            'message': ''
        }

    !!! 注意：
        若使用装饰器时不显式传入logit=False,则需要在使用装饰器前设置一个名为 'error'的logger
        否则，logger会将日志信息打印到console
"""


def get_exc_info_customized(e, exc_list):
    info = ['未知错误', 500, None]
    for exc_item in exc_list:
        if type(e) is exc_item[0]:
            info = list(exc_item[1:4])
            info = ['', 500, None] if not info else info
            info.extend([500, None]) if len(info) == 1 else None
            info.append(None) if len(info) == 2 else None
            # ycms_message  
            info[0] = info[0] if info[0] else getattr(e, 'ycms_message', None)
    return info


def wrapper(exc_list=None, response_status_code=200, catch_all_exc=True, logit=True):
    """ 通用的请求handler的装饰器
        
        传入预期需要捕获的异常列表。
        当列表内异常发生时，首先检查第二项若存在第二项（错误信息）使用该错误信息作为返回给前端数据
        的错误信息。若没有提供第二项或第二项为非真值则尝试使用异常对象自己的message，若异常对象没有
        message属性，则兜底使用'未知错误'作为异常信息。

        :param exc_list<type list>: 预期捕获的异常列表
            格式：[ (exc_obj, message, status_code, redirect_url), ...  ]
            exc_ob: 异常对象
            message: 返回给前端的异常信息
            redirect_url: 重定向到的目的url
        :param response_status: 返回给前端的http status code 默认： 200
            注：pydantic 异常一般应保 402
        :param logit: 是否记录异常信息到错误日志 默认： True

    """
    exc_list = exc_list if exc_list else []
    exc_obj_list = [exc_item[0] for exc_item in exc_list]
    # 若捕获所有异常 则在异常列表后append Exception
    exc_obj_list.append(Exception) if catch_all_exc else None
    exc_obj_list = tuple(exc_obj_list)
    def wrapper(fn):
        @functools.wraps(fn)
        def inner(*arg, **kwarg):
            resp = {
                'status': 'SUCCESS',
                'data': None,
                'errors': None,
            }
            response_status_code = 200
            try:
                rs= fn(*arg, **kwarg)
                if isinstance(rs, (tuple, list)):
                    rs = list(rs)[:3]
                    rs = [None, '', 200] if not rs else rs
                    ['', 200].extend(rs) if len(rs) == 1 else None
                    rs.appen(200) if len(rs) == 2 else None
                    resp['data'] = rs[0]
                    resp['message'] = rs[1] if rs[1] else '操作成功'
                    response_status_code = rs[2]
                else:
                    resp['data'] = rs 
                    resp['message'] = '操作成功'
            except exc_obj_list as e:
                """ 处理 返回错误信息 可返回给前端的
                """
                exc_message, response_status_code, redirect_url = get_exc_info_customized(e, exc_list)
                exc_message = exc_message if exc_message else '未知错误'
                resp['status'] = 'ERROR'
                resp['message'] = exc_message 

                if redirect_url:
                    if response_status_code not in (301, 302, 303, 305, 307):
                        response_status_code = 302
                    return redirect(redirect_url, response_status_code)
                ## 记录日志
                if logit:
                    logger = logging.getLogger('error')
                    logger.error(exc_message, exc_info=True)
            return resp, response_status_code
        return inner
    return wrapper
