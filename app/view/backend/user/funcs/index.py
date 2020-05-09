from ....bp_base import bpuser

@bpuser.route('')
def index():
    return 'view.user.index.index'
