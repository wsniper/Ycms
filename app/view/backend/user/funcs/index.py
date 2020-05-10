from flask import current_app, session
from ....bp_base import bpuser
import flask

@bpuser.route('')
def index():
    session['uid'] = 'uid_9'
    current_app.logger.warning('xxxxxxxxxxxxxxx this is a warning messge')
    print('user........................index')
    return 'view.user.index.index'
