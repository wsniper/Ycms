""" funcs 下的所有模块必须在此import一下 
    
    目的是利用import的副作用--顶层代码在import时执行
    来执行类似@bpuser.route(....),进而将模块的逻辑代码绑定到对应的blueprint
"""
from . import index
