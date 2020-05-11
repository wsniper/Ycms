from os import path

conf = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }
    },
    'handlers': {
        'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        },
        'debug_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': path.join(path.dirname(path.dirname(path.abspath(__file__))), 'log',
                                  'debug.log'),
            'formatter': 'default',
            'mode': 'a',
            'maxBytes': 99999,
            'backupCount': 999999999
        }
    },
    'loggers': {
        'debug': {
            'level': 'DEBUG',
            'handlers': ['debug_handler']
        }
    },
    'default': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
}
