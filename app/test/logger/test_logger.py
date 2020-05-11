from app.test import client


def test_log_warning(client):
    client.get('/user')


def test_debug_logger():
    import logging
    dlogger = logging.getLogger('debug')
    dlogger.info('test debug logger in file: '+__file__)


