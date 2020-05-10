import logging
from logging import handlers
from flask import has_request_context, request, session

from .. import conf


class FormatterWithRequestInfo(logging.Formatter):
    def format(self, record):
        if has_request_context():
            record.url = request.url
            record.remote_addr = request.remote_addr
            record.uid = session.get('uid', 0)
        else:
            record.url = None
            record.remote_addr = None
            record.uid = 0
        return super().format(record)



def init_logger(app):
    from flask.logging import default_handler

    # app.logger.removeHandler(default_handler)
    


    formatter = FormatterWithRequestInfo('[%(asctime)s] %(remote_addr)s requested %(url)s uid<%(uid)s>\n'
                                             '%(levelname)s in %(module)s: %(message)s')
    handler = handlers.RotatingFileHandler(filename=conf.LOGGING.file_name,
                                                   maxBytes=conf.LOGGING.sigle_file_max_bytes or 4096,
                                                   backupCount=conf.LOGGING.max_file_count or 4096)

    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    # app.logger = logger

