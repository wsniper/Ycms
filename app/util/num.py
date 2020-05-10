""" 数字相关操作
"""
def parse_int_or_zero(arg):
    try:
        return int(str(arg))
    except ValueError as e:
        return 0

def parse_int_or_false(arg):
    try:
        return int(str(arg))
    except ValueError as e:
        return False

def parse_int_or_none(arg):
    try:
        return int(str(arg))
    except ValueError as e:
        return None
