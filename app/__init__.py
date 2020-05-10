from flask import Flask
from . import logger
from . import conf


from .view import register_bp

def create_app(test_config=None):
    app_ins = Flask(__name__)
    app_ins.config.from_mapping({
        'SECRET_KEY': conf.APP.secret_key
    })

    logger.init_logger(app_ins)
    register_bp(app_ins)

    return app_ins

