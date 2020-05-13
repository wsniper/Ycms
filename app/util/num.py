""" 数字相关操作
"""
### 整形
def parse_int_or_zero(arg):
    try:
        return int(str(arg))
    except ValueError as e:
        return 0


def parse_int_or_zero_unsigned(arg):
    try:
        return int(str(arg)) if int(str(arg)) > 0 else 0
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

### float

def parse_float_or_zero(arg):
    try:
        return float(str(arg))
    except ValueError as e:
        return 0


def parse_float_or_zero_unsigned(arg):
    try:
        return float(str(arg)) > 0 or 0
    except ValueError as e:
        return 0


def parse_float_or_false(arg):
    try:
        return float(str(arg))
    except ValueError as e:
        return False


def parse_float_or_none(arg):
    try:
        return float(str(arg))
    except ValueError as e:
        return None
