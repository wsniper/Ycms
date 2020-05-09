""" 这个包 是前台浏览？
"""
from .bp_base import bpadmin
from .bp_base import bpuser
from .bp_base import bpfrontend

from .backend import *
from .frontend import *

def register_bp(app):
    app.register_blueprint(bpadmin)
    app.register_blueprint(bpuser)
    app.register_blueprint(bpfrontend)
        
