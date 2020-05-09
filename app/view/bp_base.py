from flask import Blueprint
from ..conf import URL
bpadmin = Blueprint('bpadmin', __name__, url_prefix=URL.admin or '/admin')
bpuser = Blueprint('bpuser', __name__, url_prefix=URL.user or '/user')
bpfrontend = Blueprint('bpfrontend', __name__, url_prefix='/')

