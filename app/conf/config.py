import os
from os import path

class APP:
    root = path.dirname(path.dirname(path.abspath(__file__)))
    secret_key = '$#@$@#dsfjaj08234jlk234098sjdjiouJLUj@#^^&*sdfjERJljFDlk$'


class DB:
    db_type = 'sqlite'
    driver = 'sqlite:///:memory:'
    is_debug = True


class URL:
    admin = '/management'
    user = '/user'

class EXC:
    log_path = path.join(APP.root, 'log', 'error.log')


class LOGGING:
    sigle_file_max_bytes = 4096
    max_file_count = 4096
    file_name = path.join(APP.root, 'log', 'ycms.log')
