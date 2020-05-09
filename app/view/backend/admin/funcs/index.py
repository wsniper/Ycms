from ....bp_base import bpadmin

@bpadmin.route('')
def index():
    return 'view.admin.index.index'
