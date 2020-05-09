from ...bp_base import bpfrontend

@bpfrontend.route('')
def index():
    return 'view.frontend.index.index'
